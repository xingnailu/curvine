// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use crate::worker::java::{JavaError, JvmProcessManager, JvmConfig, OssTaskParams, JvmProcessState};
use curvine_common::FsResult;
use curvine_common::state::LoadTaskInfo;
use log::{error, info, warn};
use orpc::err_box;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;

/// OSS任务执行结果
#[derive(Debug, Clone)]
pub enum OssTaskResult {
    Success {
        task_id: String,
        bytes_transferred: i64,
        duration_ms: u64,
    },
    Failed {
        task_id: String,
        error: JavaError,
        retries_exhausted: bool,
    },
    Timeout {
        task_id: String,
        timeout_ms: u64,
    },
    Cancelled {
        task_id: String,
    },
}

/// OSS任务执行器
pub struct OssTaskExecutor {
    jvm_manager: Arc<Mutex<JvmProcessManager>>,
    config: JvmConfig,
    active_tasks: Arc<Mutex<HashMap<String, TaskExecutionContext>>>,
}

/// 任务执行上下文
#[derive(Debug)]
struct TaskExecutionContext {
    task_info: LoadTaskInfo,
    start_time: std::time::Instant,
    retry_count: u32,
    last_error: Option<JavaError>,
}

impl OssTaskExecutor {
    /// 创建新的OSS任务执行器
    pub fn new(config: JvmConfig) -> Self {
        let jvm_manager = JvmProcessManager::new(config.clone());
        
        Self {
            jvm_manager: Arc::new(Mutex::new(jvm_manager)),
            config,
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 执行OSS同步任务
    pub async fn execute_oss_task(&self, task_info: LoadTaskInfo) -> FsResult<OssTaskResult> {
        let task_id = task_info.task_id.clone();
        info!("Starting OSS task execution: {}", task_id);

        // 检查是否为OSS schema
        if !self.is_oss_schema(&task_info.source_path) {
            return err_box!("Task {} is not an OSS schema task", task_id);
        }

        // 创建任务执行上下文
        let context = TaskExecutionContext {
            task_info: task_info.clone(),
            start_time: std::time::Instant::now(),
            retry_count: 0,
            last_error: None,
        };

        self.active_tasks.lock().await.insert(task_id.clone(), context);

        // 执行任务（包含重试逻辑）
        let result = self.execute_with_retry(task_info).await;

        // 清理任务上下文
        self.active_tasks.lock().await.remove(&task_id);

        result
    }

    /// 带重试逻辑的任务执行
    async fn execute_with_retry(&self, task_info: LoadTaskInfo) -> FsResult<OssTaskResult> {
        let task_id = task_info.task_id.clone();
        let max_retries = self.config.max_retries;

        for retry_count in 0..=max_retries {
            info!("Executing OSS task {} (attempt {}/{})", task_id, retry_count + 1, max_retries + 1);

            // 更新重试计数
            if let Some(context) = self.active_tasks.lock().await.get_mut(&task_id) {
                context.retry_count = retry_count;
            }

            match self.execute_single_attempt(&task_info).await {
                Ok(result) => {
                    info!("OSS task {} completed successfully", task_id);
                    return Ok(result);
                }
                Err(e) => {
                    warn!("OSS task {} attempt {} failed: {}", task_id, retry_count + 1, e);

                    // 尝试解析为Java异常
                    let java_error = self.parse_rust_error_to_java_error(e);
                    
                    // 更新最后的异常信息
                    if let Some(context) = self.active_tasks.lock().await.get_mut(&task_id) {
                        context.last_error = Some(java_error.clone());
                    }

                    // 检查是否应该重试
                    if retry_count >= max_retries || !java_error.is_retryable() {
                        error!("OSS task {} failed after {} attempts, giving up", task_id, retry_count + 1);
                        return Ok(OssTaskResult::Failed {
                            task_id,
                            error: java_error,
                            retries_exhausted: retry_count >= max_retries,
                        });
                    }

                    // 等待重试延迟
                    let delay_seconds = java_error.retry_delay_seconds();
                    if delay_seconds > 0 {
                        info!("Waiting {} seconds before retry for task {}", delay_seconds, task_id);
                        tokio::time::sleep(Duration::from_secs(delay_seconds)).await;
                    }
                }
            }
        }

        // 理论上不会到达这里
        unreachable!()
    }

    /// 执行单次尝试
    async fn execute_single_attempt(&self, task_info: &LoadTaskInfo) -> FsResult<OssTaskResult> {
        let task_id = task_info.task_id.clone();
        
        // 构建任务参数
        let params = self.build_task_params(task_info)?;
        
        // 启动JVM进程
        {
            let mut jvm_manager = self.jvm_manager.lock().await;
            jvm_manager.start_oss_task(params).await?;
        }

        // 监控任务执行
        let result = self.monitor_task_execution(&task_id).await?;

        // 清理JVM进程
        {
            let jvm_manager = self.jvm_manager.lock().await;
            let _ = jvm_manager.stop_task(&task_id).await;
        }

        Ok(result)
    }

    /// 监控任务执行
    async fn monitor_task_execution(&self, task_id: &str) -> FsResult<OssTaskResult> {
        let timeout_duration = Duration::from_millis(self.config.process_timeout_ms);
        let check_interval = Duration::from_millis(5000); // 5秒检查一次
        
        let start_time = std::time::Instant::now();

        loop {
            // 检查超时
            if start_time.elapsed() > timeout_duration {
                return Ok(OssTaskResult::Timeout {
                    task_id: task_id.to_string(),
                    timeout_ms: self.config.process_timeout_ms,
                });
            }

            // 检查任务状态
            let state = {
                let jvm_manager = self.jvm_manager.lock().await;
                jvm_manager.get_task_state(task_id)
            };

            match state {
                Some(JvmProcessState::Running) => {
                    // 任务仍在运行，继续监控
                    tokio::time::sleep(check_interval).await;
                    continue;
                }
                Some(JvmProcessState::Stopped) => {
                    // 任务成功完成
                    return Ok(OssTaskResult::Success {
                        task_id: task_id.to_string(),
                        bytes_transferred: 0, // TODO: 从Java进程获取实际传输的字节数
                        duration_ms: start_time.elapsed().as_millis() as u64,
                    });
                }
                Some(JvmProcessState::Failed(java_error)) => {
                    // 任务失败
                    return Ok(OssTaskResult::Failed {
                        task_id: task_id.to_string(),
                        error: java_error,
                        retries_exhausted: false,
                    });
                }
                Some(JvmProcessState::Terminated) => {
                    // 任务被取消
                    return Ok(OssTaskResult::Cancelled {
                        task_id: task_id.to_string(),
                    });
                }
                Some(JvmProcessState::Starting) | Some(JvmProcessState::NotStarted) => {
                    // 继续等待
                    tokio::time::sleep(check_interval).await;
                    continue;
                }
                None => {
                    // 进程不存在，可能已经退出
                    return err_box!("JVM process for task {} not found", task_id);
                }
            }
        }
    }

    /// 构建任务参数
    fn build_task_params(&self, task_info: &LoadTaskInfo) -> FsResult<OssTaskParams> {
        let oss_config = task_info.job.ufs_conf.clone();
        
        // 从配置中提取必要的参数并验证
        self.validate_oss_config(&oss_config)?;

        // 构建Curvine配置
        let curvine_config = self.build_curvine_config(task_info)?;

        Ok(OssTaskParams {
            task_id: task_info.task_id.clone(),
            job_id: task_info.job.job_id.clone(),
            source_path: task_info.source_path.clone(),
            target_path: task_info.target_path.clone(),
            oss_config,
            curvine_config,
        })
    }

    /// 验证OSS配置
    fn validate_oss_config(&self, config: &HashMap<String, String>) -> FsResult<()> {
        // 检查必需的配置项
        let required_keys = vec![
            "fs.oss.endpoint",
            "fs.oss.accessKeyId", 
            "fs.oss.accessKeySecret",
            "fs.oss.bucket",
        ];

        for key in required_keys {
            if !config.contains_key(key) || config[key].is_empty() {
                return err_box!("Missing required OSS config: {}", key);
            }
        }

        Ok(())
    }

    /// 构建Curvine配置
    fn build_curvine_config(&self, task_info: &LoadTaskInfo) -> FsResult<HashMap<String, String>> {
        let mut config = HashMap::new();

        // 添加基本配置
        config.insert("curvine.target.path".to_string(), task_info.target_path.clone());
        config.insert("curvine.replicas".to_string(), task_info.job.replicas.to_string());
        config.insert("curvine.block.size".to_string(), task_info.job.block_size.to_string());
        config.insert("curvine.storage.type".to_string(), format!("{:?}", task_info.job.storage_type));
        config.insert("curvine.ttl.ms".to_string(), task_info.job.ttl_ms.to_string());
        config.insert("curvine.ttl.action".to_string(), format!("{:?}", task_info.job.ttl_action));

        // TODO: 添加Curvine集群连接配置
        // config.insert("curvine.master.address".to_string(), master_address);
        // config.insert("curvine.worker.address".to_string(), worker_address);

        Ok(config)
    }

    /// 检查是否为OSS schema
    fn is_oss_schema(&self, path: &str) -> bool {
        path.starts_with("oss://")
    }

    /// 将Rust错误转换为Java错误
    fn parse_rust_error_to_java_error(&self, error: curvine_common::error::FsError) -> JavaError {
        let message = error.to_string();
        
        // 简单的错误分类
        let error_type = if message.contains("timeout") {
            crate::worker::java::JavaErrorType::NetworkError
        } else if message.contains("connection") {
            crate::worker::java::JavaErrorType::StorageConnectionError
        } else if message.contains("authentication") || message.contains("access denied") {
            crate::worker::java::JavaErrorType::AuthenticationError
        } else if message.contains("configuration") {
            crate::worker::java::JavaErrorType::ConfigurationError
        } else {
            crate::worker::java::JavaErrorType::Unknown
        };

        JavaError::new(error_type, message)
    }

    /// 取消任务
    pub async fn cancel_task(&self, task_id: &str) -> FsResult<()> {
        info!("Cancelling OSS task: {}", task_id);

        // 停止JVM进程
        {
            let jvm_manager = self.jvm_manager.lock().await;
            jvm_manager.stop_task(task_id).await?;
        }

        // 清理任务上下文
        self.active_tasks.lock().await.remove(task_id);

        Ok(())
    }

    /// 获取活动任务列表
    pub async fn get_active_tasks(&self) -> Vec<String> {
        self.active_tasks
            .lock()
            .await
            .keys()
            .cloned()
            .collect()
    }

    /// 获取任务执行统计信息
    pub async fn get_task_stats(&self) -> TaskStats {
        let active_tasks = self.active_tasks.lock().await;
        let jvm_manager = self.jvm_manager.lock().await;

        TaskStats {
            active_tasks_count: active_tasks.len(),
            running_processes_count: jvm_manager.running_tasks_count(),
            total_retries: active_tasks.values().map(|ctx| ctx.retry_count).sum(),
        }
    }

    /// 清理资源
    pub async fn cleanup(&self) {
        info!("Cleaning up OSS task executor");

        // 取消所有活动任务
        let active_task_ids: Vec<String> = self.active_tasks.lock().await.keys().cloned().collect();
        for task_id in active_task_ids {
            if let Err(e) = self.cancel_task(&task_id).await {
                warn!("Failed to cancel task {} during cleanup: {}", task_id, e);
            }
        }

        // 清理JVM管理器
        let jvm_manager = self.jvm_manager.lock().await;
        jvm_manager.cleanup_completed_tasks();
    }
}

/// 任务执行统计信息
#[derive(Debug, Clone)]
pub struct TaskStats {
    pub active_tasks_count: usize,
    pub running_processes_count: usize,
    pub total_retries: u32,
}

impl Drop for OssTaskExecutor {
    fn drop(&mut self) {
        // 注意：在异步上下文中不能直接调用async方法
        // 这里只是记录日志，实际清理应该在调用cleanup方法时完成
        warn!("OssTaskExecutor is being dropped, make sure cleanup() was called");
    }
}
