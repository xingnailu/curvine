// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use crate::worker::java::{JavaError, JavaErrorHandler, JavaErrorType};
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::err_box;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child as TokioChild, Command as TokioCommand};
use tokio::sync::mpsc;

/// JVM进程配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JvmConfig {
    pub java_home: String,
    pub classpath: String,
    pub main_class: String,
    pub heap_size: String,
    pub gc_options: Vec<String>,
    pub jvm_args: Vec<String>,
    pub max_retries: u32,
    pub process_timeout_ms: u64,
    pub health_check_interval_ms: u64,
}

impl Default for JvmConfig {
    fn default() -> Self {
        Self {
            java_home: std::env::var("JAVA_HOME").unwrap_or_else(|_| "/usr/lib/jvm/java-8-openjdk".to_string()),
            classpath: String::new(),
            main_class: "io.curvine.worker.OssDataSyncRunner".to_string(),
            heap_size: "2g".to_string(),
            gc_options: vec![
                "-XX:+UseG1GC".to_string(),
                "-XX:MaxGCPauseMillis=200".to_string(),
                "-XX:+UnlockExperimentalVMOptions".to_string(),
            ],
            jvm_args: vec![
                "-server".to_string(),
                "-Dfile.encoding=UTF-8".to_string(),
            ],
            max_retries: 3,
            process_timeout_ms: 60 * 60 * 1000, // 1小时
            health_check_interval_ms: 30 * 1000, // 30秒
        }
    }
}

/// JVM进程状态
#[derive(Debug, Clone, PartialEq)]
pub enum JvmProcessState {
    NotStarted,
    Starting,
    Running,
    Failed(JavaError),
    Stopped,
    Terminated,
}

/// JVM进程信息
#[derive(Debug)]
pub struct JvmProcessInfo {
    pub process_id: u32,
    pub state: JvmProcessState,
    pub start_time: Instant,
    pub last_heartbeat: Option<Instant>,
    pub retry_count: u32,
    pub task_id: String,
}

/// OSS同步任务参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OssTaskParams {
    pub task_id: String,
    pub job_id: String,
    pub source_path: String,
    pub target_path: String,
    pub oss_config: HashMap<String, String>,
    pub curvine_config: HashMap<String, String>,
}

/// JVM进程管理器
pub struct JvmProcessManager {
    config: JvmConfig,
    processes: Arc<Mutex<HashMap<String, JvmProcessInfo>>>,
    error_handler: JavaErrorHandler,
    shutdown_tx: Option<mpsc::Sender<String>>,
}

impl JvmProcessManager {
    pub fn new(config: JvmConfig) -> Self {
        Self {
            config,
            processes: Arc::new(Mutex::new(HashMap::new())),
            error_handler: JavaErrorHandler::new(),
            shutdown_tx: None,
        }
    }

    /// 启动OSS同步任务
    pub async fn start_oss_task(&mut self, params: OssTaskParams) -> FsResult<()> {
        let task_id = params.task_id.clone();
        
        info!("Starting OSS sync task: {}", task_id);
        
        // 检查是否已存在运行中的任务
        if self.is_task_running(&task_id) {
            return err_box!("Task {} is already running", task_id);
        }

        // 启动JVM进程
        let mut retry_count = 0;
        loop {
            match self.spawn_jvm_process(&params).await {
                Ok(process_info) => {
                    self.processes.lock().unwrap().insert(task_id.clone(), process_info);
                    info!("Successfully started JVM process for task: {}", task_id);
                    break;
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= self.config.max_retries {
                        error!("Failed to start JVM process after {} retries: {}", retry_count, e);
                        return Err(e);
                    }
                    
                    warn!("Failed to start JVM process (attempt {}): {}, retrying...", retry_count, e);
                    tokio::time::sleep(Duration::from_secs(retry_count as u64 * 2)).await;
                }
            }
        }

        Ok(())
    }

    /// 生成JVM进程
    async fn spawn_jvm_process(&self, params: &OssTaskParams) -> FsResult<JvmProcessInfo> {
        let java_executable = format!("{}/bin/java", self.config.java_home);
        let jar_path = self.build_jar_path();
        
        let mut cmd = TokioCommand::new(java_executable);
        cmd.arg(format!("-Xmx{}", self.config.heap_size))
           .arg(format!("-Xms{}", self.config.heap_size))
           .args(&self.config.gc_options)
           .args(&self.config.jvm_args)
           .arg("-cp")
           .arg(&self.config.classpath)
           .arg(&self.config.main_class)
           .arg("--task-id")
           .arg(&params.task_id)
           .arg("--source-path")
           .arg(&params.source_path)
           .arg("--target-path")
           .arg(&params.target_path)
           .arg("--oss-config")
           .arg(serde_json::to_string(&params.oss_config).unwrap())
           .arg("--curvine-config")
           .arg(serde_json::to_string(&params.curvine_config).unwrap())
           .stdin(Stdio::piped())
           .stdout(Stdio::piped())
           .stderr(Stdio::piped());

        let child = cmd.spawn()
            .map_err(|e| curvine_common::error::FsError::from(format!("Failed to spawn JVM process: {}", e)))?;

        let process_id = child.id().unwrap_or(0);
        info!("Spawned JVM process with PID: {}", process_id);

        // 启动进程监控
        self.start_process_monitoring(child, params.task_id.clone()).await?;

        Ok(JvmProcessInfo {
            process_id,
            state: JvmProcessState::Starting,
            start_time: Instant::now(),
            last_heartbeat: Some(Instant::now()),
            retry_count: 0,
            task_id: params.task_id.clone(),
        })
    }

    /// 启动进程监控
    async fn start_process_monitoring(&self, mut child: TokioChild, task_id: String) -> FsResult<()> {
        let processes = self.processes.clone();
        let error_handler = self.error_handler.clone();
        
        tokio::spawn(async move {
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();
            
            let mut stdout_reader = BufReader::new(stdout);
            let mut stderr_reader = BufReader::new(stderr);
            
            let mut stdout_line = String::new();
            let mut stderr_line = String::new();
            
            loop {
                tokio::select! {
                    result = stdout_reader.read_line(&mut stdout_line) => {
                        match result {
                            Ok(0) => break, // EOF
                            Ok(_) => {
                                info!("[JVM-{}] {}", task_id, stdout_line.trim());
                                Self::process_stdout_message(&task_id, &stdout_line, &processes);
                                stdout_line.clear();
                            }
                            Err(e) => {
                                error!("Error reading stdout: {}", e);
                                break;
                            }
                        }
                    }
                    result = stderr_reader.read_line(&mut stderr_line) => {
                        match result {
                            Ok(0) => break, // EOF
                            Ok(_) => {
                                warn!("[JVM-{}] {}", task_id, stderr_line.trim());
                                Self::process_stderr_message(&task_id, &stderr_line, &processes, &error_handler);
                                stderr_line.clear();
                            }
                            Err(e) => {
                                error!("Error reading stderr: {}", e);
                                break;
                            }
                        }
                    }
                    result = child.wait() => {
                        match result {
                            Ok(status) => {
                                if status.success() {
                                    info!("JVM process {} completed successfully", task_id);
                                    Self::update_process_state(&task_id, JvmProcessState::Stopped, &processes);
                                } else {
                                    error!("JVM process {} failed with exit code: {:?}", task_id, status.code());
                                    let java_error = JavaError::new(
                                        JavaErrorType::ProcessExit,
                                        format!("Process exited with code: {:?}", status.code())
                                    );
                                    Self::update_process_state(&task_id, JvmProcessState::Failed(java_error), &processes);
                                }
                            }
                            Err(e) => {
                                error!("Error waiting for JVM process {}: {}", task_id, e);
                                let java_error = JavaError::new(
                                    JavaErrorType::ProcessError,
                                    format!("Process wait error: {}", e)
                                );
                                Self::update_process_state(&task_id, JvmProcessState::Failed(java_error), &processes);
                            }
                        }
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// 处理stdout消息
    fn process_stdout_message(
        task_id: &str,
        line: &str,
        processes: &Arc<Mutex<HashMap<String, JvmProcessInfo>>>,
    ) {
        if line.contains("TASK_STARTED") {
            Self::update_process_state(task_id, JvmProcessState::Running, processes);
        } else if line.contains("HEARTBEAT") {
            Self::update_heartbeat(task_id, processes);
        } else if line.contains("TASK_COMPLETED") {
            Self::update_process_state(task_id, JvmProcessState::Stopped, processes);
        }
    }

    /// 处理stderr消息
    fn process_stderr_message(
        task_id: &str,
        line: &str,
        processes: &Arc<Mutex<HashMap<String, JvmProcessInfo>>>,
        error_handler: &JavaErrorHandler,
    ) {
        if let Some(java_error) = error_handler.parse_error(line) {
            Self::update_process_state(task_id, JvmProcessState::Failed(java_error), processes);
        }
    }

    /// 更新进程状态
    fn update_process_state(
        task_id: &str,
        state: JvmProcessState,
        processes: &Arc<Mutex<HashMap<String, JvmProcessInfo>>>,
    ) {
        if let Ok(mut processes) = processes.lock() {
            if let Some(process_info) = processes.get_mut(task_id) {
                process_info.state = state;
            }
        }
    }

    /// 更新心跳时间
    fn update_heartbeat(
        task_id: &str,
        processes: &Arc<Mutex<HashMap<String, JvmProcessInfo>>>,
    ) {
        if let Ok(mut processes) = processes.lock() {
            if let Some(process_info) = processes.get_mut(task_id) {
                process_info.last_heartbeat = Some(Instant::now());
            }
        }
    }

    /// 检查任务是否正在运行
    pub fn is_task_running(&self, task_id: &str) -> bool {
        if let Ok(processes) = self.processes.lock() {
            if let Some(process_info) = processes.get(task_id) {
                matches!(process_info.state, JvmProcessState::Starting | JvmProcessState::Running)
            } else {
                false
            }
        } else {
            false
        }
    }

    /// 获取任务状态
    pub fn get_task_state(&self, task_id: &str) -> Option<JvmProcessState> {
        if let Ok(processes) = self.processes.lock() {
            processes.get(task_id).map(|info| info.state.clone())
        } else {
            None
        }
    }

    /// 停止任务
    pub async fn stop_task(&self, task_id: &str) -> FsResult<()> {
        info!("Stopping OSS sync task: {}", task_id);
        
        let process_id = {
            if let Ok(mut processes) = self.processes.lock() {
                if let Some(process_info) = processes.get_mut(task_id) {
                    process_info.state = JvmProcessState::Terminated;
                    Some(process_info.process_id)
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(pid) = process_id {
            // 发送终止信号
            let result = tokio::process::Command::new("kill")
                .arg("-15") // SIGTERM
                .arg(pid.to_string())
                .output()
                .await;

            match result {
                Ok(output) => {
                    if output.status.success() {
                        info!("Successfully sent SIGTERM to process {}", pid);
                    } else {
                        warn!("Failed to send SIGTERM to process {}: {}", 
                              pid, String::from_utf8_lossy(&output.stderr));
                    }
                }
                Err(e) => {
                    warn!("Error sending SIGTERM to process {}: {}", pid, e);
                }
            }

            // 等待进程结束，如果超时则强制终止
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            let _ = tokio::process::Command::new("kill")
                .arg("-9") // SIGKILL
                .arg(pid.to_string())
                .output()
                .await;
        }

        // 从进程列表中移除
        if let Ok(mut processes) = self.processes.lock() {
            processes.remove(task_id);
        }

        Ok(())
    }

    /// 构建JAR包路径
    fn build_jar_path(&self) -> String {
        // 这里应该返回实际的JAR包路径
        format!("{}/curvine-oss-sync.jar", self.config.classpath)
    }

    /// 清理已完成的任务
    pub fn cleanup_completed_tasks(&self) {
        if let Ok(mut processes) = self.processes.lock() {
            let completed_tasks: Vec<String> = processes
                .iter()
                .filter(|(_, info)| {
                    matches!(info.state, JvmProcessState::Stopped | JvmProcessState::Terminated)
                })
                .map(|(task_id, _)| task_id.clone())
                .collect();

            for task_id in completed_tasks {
                info!("Cleaning up completed task: {}", task_id);
                processes.remove(&task_id);
            }
        }
    }

    /// 获取运行中的任务数量
    pub fn running_tasks_count(&self) -> usize {
        if let Ok(processes) = self.processes.lock() {
            processes
                .values()
                .filter(|info| matches!(info.state, JvmProcessState::Starting | JvmProcessState::Running))
                .count()
        } else {
            0
        }
    }
}

impl Drop for JvmProcessManager {
    fn drop(&mut self) {
        // 清理所有运行中的进程
        if let Ok(processes) = self.processes.lock() {
            for (task_id, process_info) in processes.iter() {
                if matches!(process_info.state, JvmProcessState::Starting | JvmProcessState::Running) {
                    warn!("Terminating JVM process {} on shutdown", task_id);
                    let _ = std::process::Command::new("kill")
                        .arg("-9")
                        .arg(process_info.process_id.to_string())
                        .output();
                }
            }
        }
    }
}
