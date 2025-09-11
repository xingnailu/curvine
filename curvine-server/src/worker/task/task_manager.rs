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

use crate::worker::task::load_task_runner::LoadTaskRunner;
use crate::worker::task::{TaskContext, TaskStore};
use curvine_client::file::{CurvineFileSystem, FsContext};
use curvine_common::conf::{ClusterConf, WorkerConf};
use curvine_common::state::LoadTaskInfo;
use curvine_common::FsResult;
use log::info;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncReceiver, AsyncSender};
use std::sync::Arc;

pub struct TaskManager {
    rt: Arc<Runtime>,
    fs: CurvineFileSystem,
    tasks: TaskStore,
    sender: AsyncSender<Arc<TaskContext>>,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
    worker_conf: WorkerConf,
}

impl TaskManager {
    /// Creates a new TaskManager with an existing runtime.
    ///
    /// This method initializes a task manager that handles load tasks execution
    /// with an external async runtime, providing better resource control and
    /// allowing runtime sharing across components.
    ///
    /// # Arguments
    ///
    /// * `rt` - An existing Arc-wrapped Runtime for async task execution
    /// * `conf` - The cluster configuration containing job and client settings
    /// * `sender` - Async channel sender for dispatching task contexts to workers
    ///
    /// # Returns
    ///
    /// Returns `FsResult<Self>` containing the initialized TaskManager or an error
    /// if filesystem initialization fails or configuration is invalid.
    ///
    /// # Behavior
    ///
    /// - Modifies client hostname to "localhost" to prevent local write priority
    /// - This ensures data distribution across all workers instead of local bias
    /// - Initializes filesystem client with the modified configuration
    /// - Sets up task store and timing configurations from job settings
    pub fn with_rt(
        rt: Arc<Runtime>,
        conf: &ClusterConf,
        sender: AsyncSender<Arc<TaskContext>>,
    ) -> FsResult<Self> {
        let mut new_conf = conf.clone();
        new_conf.client.hostname = "localhost".to_string();

        let fs = CurvineFileSystem::with_rt(new_conf, rt.clone())?;

        let mgr = Self {
            rt,
            fs,
            tasks: TaskStore::new(),
            sender,
            progress_interval_ms: conf.job.task_report_interval.as_millis() as u64,
            task_timeout_ms: conf.job.task_timeout.as_millis() as u64,
            worker_conf: conf.worker.clone(),
        };

        Ok(mgr)
    }

    pub fn start(&self, mut receiver: AsyncReceiver<Arc<TaskContext>>) {
        let task_store = self.tasks.clone();
        let rt = self.rt.clone();
        let fs = self.fs.clone();
        let progress_interval_ms = self.progress_interval_ms;
        let task_timeout_ms = self.task_timeout_ms;
        let worker_conf = self.worker_conf.clone();

        self.rt.spawn(async move {
            while let Some(task) = receiver.recv().await {
                let task_id = task.info.task_id.clone();
                let store = task_store.clone();
                let runner =
                    LoadTaskRunner::new(task, fs.clone(), progress_interval_ms, task_timeout_ms, &worker_conf);

                rt.spawn(async move {
                    runner.run().await;
                    let _ = store.remove(&task_id);
                });
            }
        });
    }

    pub fn submit_task(&self, task: LoadTaskInfo) -> FsResult<()> {
        let task_id = task.task_id.clone();
        if self.tasks.contains(&task_id) {
            return Ok(());
        }

        let context = self.tasks.insert(task);
        if let Err(e) = self.rt.block_on(self.sender.send(context.clone())) {
            let _ = self.tasks.remove(&task_id);
            return Err(e.into());
        }
        info!("submit task {}", task_id);

        Ok(())
    }

    pub fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let all_task = self.tasks.cancel(job_id);

        info!(
            "Successfully canceled {} tasks for job {}",
            all_task.len(),
            job_id
        );
        Ok(())
    }

    pub fn get_fs_context(&self) -> Arc<FsContext> {
        self.fs.fs_context()
    }
}
