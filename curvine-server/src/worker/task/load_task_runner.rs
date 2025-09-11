// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::worker::task::TaskContext;
use crate::worker::task::oss_load_task_runner::OssLoadTaskRunner;
use crate::worker::java::JvmConfig;
use curvine_client::file::{CurvineFileSystem, FsWriter};
use curvine_client::rpc::JobMasterClient;
use curvine_client::unified::{UfsFileSystem, UnifiedReader};
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{CreateFileOptsBuilder, JobTaskState};
use curvine_common::conf::WorkerConf;
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::common::{LocalTime, TimeSpent};
use orpc::err_box;
use std::sync::Arc;

pub struct LoadTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    master_client: JobMasterClient,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
    jvm_config: Option<JvmConfig>,
}

impl LoadTaskRunner {
    pub fn new(
        task: Arc<TaskContext>,
        fs: CurvineFileSystem,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
        worker_conf: &WorkerConf,
    ) -> Self {
        let master_client = JobMasterClient::new(fs.fs_client());
        
        // 从WorkerConf创建JvmConfig
        let jvm_config = Self::create_jvm_config(worker_conf);
        
        Self {
            task,
            fs,
            master_client,
            progress_interval_ms,
            task_timeout_ms,
            jvm_config: Some(jvm_config),
        }
    }

    /// 从WorkerConf创建JvmConfig
    fn create_jvm_config(worker_conf: &WorkerConf) -> JvmConfig {
        // OSS同步组件版本号，应与curvine-server/java/pom.xml中的version保持一致
        const OSS_SYNC_VERSION: &str = "1.0.0";
        
        // 构建classpath，默认从当前项目的lib目录下读取
        let jar_path = std::env::var("CURVINE_HOME")
            .map(|home| format!("{}/lib/curvine-oss-sync-{}.jar", home, OSS_SYNC_VERSION))
            .unwrap_or_else(|_| format!("./lib/curvine-oss-sync-{}.jar", OSS_SYNC_VERSION));
        
        JvmConfig {
            java_home: worker_conf.java_home.clone(),
            classpath: jar_path,
            main_class: "io.curvine.worker.OssDataSyncRunner".to_string(),
            heap_size: worker_conf.jvm_heap_size.clone(),
            gc_options: vec![
                "-XX:+UseG1GC".to_string(),
                "-XX:MaxGCPauseMillis=200".to_string(),
                "-XX:+UnlockExperimentalVMOptions".to_string(),
            ],
            jvm_args: vec![
                "-server".to_string(),
                "-Dfile.encoding=UTF-8".to_string(),
            ],
            max_retries: worker_conf.jvm_max_retries,
            process_timeout_ms: worker_conf.jvm_process_timeout_ms,
            health_check_interval_ms: 30 * 1000, // 30秒
        }
    }

    pub async fn run(&self) {
        if let Err(e) = self.run0().await {
            // The data replication process fails, set the status and report to the master
            error!("task {} execute failed: {}", self.task.info.task_id, e);
            let progress = self.task.set_failed(e.to_string());
            let res = self
                .master_client
                .report_task(
                    self.task.info.job.job_id.clone(),
                    self.task.info.task_id.clone(),
                    progress,
                )
                .await;

            if let Err(e) = res {
                warn!("report task {}", e)
            }
        }
    }

    async fn run0(&self) -> FsResult<()> {
        // 检查是否为OSS schema，如果是则委托给OSS专用运行器
        if self.is_oss_task() {
            return self.run_oss_task().await;
        }

        self.task
            .update_state(JobTaskState::Loading, "Task started");

        let (mut reader, mut writer) = self.create_stream().await?;
        let mut last_progress_time = LocalTime::mills();
        let mut read_cost_ms = 0;
        let mut total_cost_ms = 0;

        loop {
            if self.task.is_cancel() {
                break;
            }

            let spend = TimeSpent::new();
            let chunk = reader.async_read(None).await?;
            read_cost_ms += spend.used_ms();

            if chunk.is_empty() {
                break;
            }

            writer.write_chunk(chunk).await?;
            total_cost_ms += spend.used_ms();

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(writer.pos(), reader.len()).await;
            }

            if total_cost_ms > self.task_timeout_ms {
                return err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
                );
            }
        }

        writer.complete().await?;
        reader.complete().await?;
        self.update_progress(writer.pos(), reader.len()).await;

        info!(
            "task {} completed, source_path {}, target_path {}, copy bytes {}, read cost {} ms, task cost {} ms",
            self.task.info.task_id,
            self.task.info.source_path,
            self.task.info.target_path,
            writer.pos(),
            read_cost_ms,
            total_cost_ms,
        );

        Ok(())
    }

    async fn create_stream(&self) -> FsResult<(UnifiedReader, FsWriter)> {
        let info = &self.task.info;

        // create ufs reader
        let source_path = Path::from_str(&info.source_path)?;
        let ufs = UfsFileSystem::new(&source_path, info.job.ufs_conf.clone())?;
        let reader = ufs.open(&source_path).await?;
        let source_status = ufs.get_status(&source_path).await?;

        // create cv writer
        let target_path = Path::from_str(&info.target_path)?;
        let opts = CreateFileOptsBuilder::new()
            .overwrite(true)
            .create_parent(true)
            .replicas(info.job.replicas)
            .block_size(info.job.block_size)
            .storage_type(info.job.storage_type)
            .ttl_ms(info.job.ttl_ms)
            .ttl_action(info.job.ttl_action)
            .ufs_mtime(source_status.mtime)
            .build();
        let writer = self.fs.create_with_opts(&target_path, opts).await?;

        Ok((reader, writer))
    }

    pub async fn update_progress(&self, loaded_size: i64, total_size: i64) {
        if let Err(e) = self.update_progress0(loaded_size, total_size).await {
            warn!("update progress failed, err: {:?}", e);
        }
    }

    pub async fn update_progress0(&self, loaded_size: i64, total_size: i64) -> FsResult<()> {
        let progress = self.task.update_progress(loaded_size, total_size);
        let task = &self.task;

        self.master_client
            .report_task(&task.info.job.job_id, &task.info.task_id, progress)
            .await
    }

    /// 检查是否为OSS任务
    fn is_oss_task(&self) -> bool {
        self.task.info.source_path.starts_with("oss://")
    }

    /// 执行OSS任务
    async fn run_oss_task(&self) -> FsResult<()> {
        let jvm_config = self.jvm_config.as_ref()
            .ok_or_else(|| curvine_common::error::FsError::from("JVM config is required for OSS tasks".to_string()))?;

        info!("Delegating OSS task {} to Java process", self.task.info.task_id);

        // 创建OSS任务运行器
        let oss_runner = OssLoadTaskRunner::new(
            self.task.clone(),
            jvm_config.clone(),
            self.task_timeout_ms,
        );

        // 执行OSS任务
        match oss_runner.run().await {
            Ok(()) => {
                info!("OSS task {} completed successfully via Java process", self.task.info.task_id);
                
                // 报告任务完成给Master
                let progress = self.task.get_progress();
                let res = self
                    .master_client
                    .report_task(
                        self.task.info.job.job_id.clone(),
                        self.task.info.task_id.clone(),
                        progress,
                    )
                    .await;

                if let Err(e) = res {
                    warn!("Failed to report OSS task completion: {}", e);
                }

                Ok(())
            }
            Err(e) => {
                error!("OSS task {} failed: {}", self.task.info.task_id, e);
                
                // 报告任务失败给Master
                let progress = self.task.set_failed(e.to_string());
                let res = self
                    .master_client
                    .report_task(
                        self.task.info.job.job_id.clone(),
                        self.task.info.task_id.clone(),
                        progress,
                    )
                    .await;

                if let Err(e) = res {
                    warn!("Failed to report OSS task failure: {}", e);
                }

                Err(e)
            }
        }
    }
}
