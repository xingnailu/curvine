// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use crate::worker::java::{OssTaskExecutor, OssTaskResult, JvmConfig, TaskStats};
use crate::worker::task::TaskContext;
use curvine_common::state::JobTaskState;
use curvine_common::FsResult;
use log::{error, info, warn};
// use orpc::common::{LocalTime, TimeSpent}; // Unused imports
use orpc::err_box;
use std::sync::Arc;

/// OSS加载任务运行器
/// 
/// 专门处理OSS/OSS-HDFS数据源的加载任务
/// 通过独立的JVM进程执行数据同步
pub struct OssLoadTaskRunner {
    task: Arc<TaskContext>,
    executor: OssTaskExecutor,
    task_timeout_ms: u64,
}

impl OssLoadTaskRunner {
    /// 创建新的OSS任务运行器
    pub fn new(
        task: Arc<TaskContext>, 
        jvm_config: JvmConfig,
        task_timeout_ms: u64,
    ) -> Self {
        let executor = OssTaskExecutor::new(jvm_config);
        
        Self {
            task,
            executor,
            task_timeout_ms,
        }
    }

    /// 执行OSS同步任务
    pub async fn run(&self) -> FsResult<()> {
        if let Err(e) = self.run0().await {
            // 任务失败，设置状态并返回错误
            error!("OSS task {} execute failed: {}", self.task.info.task_id, e);
            let _progress = self.task.set_failed(e.to_string());
            
            // 注意：这里不直接报告给Master，由调用者决定如何处理
            return Err(e);
        }
        
        Ok(())
    }

    async fn run0(&self) -> FsResult<()> {
        let task_id = &self.task.info.task_id;
        info!("Starting OSS sync task: {}", task_id);

        // 验证是否为OSS任务
        if !self.is_oss_task() {
            return err_box!("Task {} is not an OSS task", task_id);
        }

        // 更新任务状态为加载中
        self.task.update_state(JobTaskState::Loading, "Starting OSS sync via JVM");

        // 记录开始时间
        let start_time = std::time::Instant::now();

        // 执行OSS同步任务
        let result = self.executor.execute_oss_task(self.task.info.clone()).await?;

        // 处理执行结果
        self.handle_execution_result(result, start_time).await
    }

    /// 处理执行结果
    async fn handle_execution_result(
        &self, 
        result: OssTaskResult, 
        start_time: std::time::Instant
    ) -> FsResult<()> {
        let task_id = &self.task.info.task_id;
        let duration_ms = start_time.elapsed().as_millis() as u64;

        match result {
            OssTaskResult::Success { bytes_transferred, .. } => {
                info!(
                    "OSS task {} completed successfully: {} bytes in {} ms",
                    task_id, bytes_transferred, duration_ms
                );
                
                // 更新进度为100%
                self.task.update_progress(bytes_transferred, bytes_transferred);
                
                // 设置任务完成状态
                self.task.update_state(JobTaskState::Completed, "OSS sync completed via JVM");
                
                Ok(())
            }
            
            OssTaskResult::Failed { error, retries_exhausted, .. } => {
                let error_msg = format!(
                    "OSS sync failed: {} (retries exhausted: {})", 
                    error.message, retries_exhausted
                );
                
                error!("OSS task {} failed: {}", task_id, error_msg);
                
                if error.is_retryable() && !retries_exhausted {
                    // 可重试的错误，返回错误让上层决定重试
                    return err_box!("OSS sync failed (retryable): {}", error.message);
                } else {
                    // 不可重试或重试已耗尽，设置失败状态
                    let _progress = self.task.set_failed(error_msg.clone());
                    return err_box!("OSS sync failed (non-retryable): {}", error_msg);
                }
            }
            
            OssTaskResult::Timeout { timeout_ms, .. } => {
                let error_msg = format!("OSS sync timeout after {} ms", timeout_ms);
                error!("OSS task {} timed out", task_id);
                
                let _progress = self.task.set_failed(error_msg.clone());
                return err_box!("OSS sync timeout: {}", error_msg);
            }
            
            OssTaskResult::Cancelled { .. } => {
                warn!("OSS task {} was cancelled", task_id);
                self.task.update_state(JobTaskState::Canceled, "OSS sync cancelled");
                return err_box!("OSS sync was cancelled");
            }
        }
    }

    /// 检查是否为OSS任务
    fn is_oss_task(&self) -> bool {
        self.task.info.source_path.starts_with("oss://")
    }

    /// 取消任务
    pub async fn cancel(&self) -> FsResult<()> {
        let task_id = &self.task.info.task_id;
        info!("Cancelling OSS task: {}", task_id);
        
        // 取消Java进程中的任务
        self.executor.cancel_task(task_id).await?;
        
        // 更新任务状态
        self.task.update_state(JobTaskState::Canceled, "OSS sync cancelled by user");
        
        Ok(())
    }

    /// 获取任务统计信息
    pub async fn get_stats(&self) -> TaskStats {
        self.executor.get_task_stats().await
    }

    /// 清理资源
    pub async fn cleanup(&self) {
        self.executor.cleanup().await;
    }
}

impl Drop for OssLoadTaskRunner {
    fn drop(&mut self) {
        warn!("OssLoadTaskRunner is being dropped for task: {}, make sure cleanup() was called", 
              self.task.info.task_id);
    }
}
