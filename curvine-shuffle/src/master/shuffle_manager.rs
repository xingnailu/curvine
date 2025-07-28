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

use crate::client::ShuffleContext;
use crate::protocol::{SpiltKey, SplitInfo, StageKey, StageStatus};
use bitvec::prelude::Lsb0;
use bitvec::vec::BitVec;
use curvine_common::FsResult;
use curvine_server::master::fs::policy::ChooseContext;
use curvine_server::master::{MasterMonitor, SyncWorkerManager};
use log::{error, info, warn};
use orpc::common::{FastHashMap, TimeSpent};
use orpc::io::net::InetAddr;
use orpc::runtime::Runtime;
use orpc::sync::{FastDashMap, StateCtl};
use orpc::{err_box, try_option};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinSet;

struct StageSpace {
    key: StageKey,
    splits: FastDashMap<i32, Vec<SplitInfo>>,
    state: StateCtl,
    committed_tasks: Mutex<BitVec<usize, Lsb0>>,
}

impl StageSpace {
    fn new(key: StageKey) -> Self {
        Self {
            key,
            splits: FastDashMap::default(),
            state: StateCtl::new(StageStatus::Running.into()),
            committed_tasks: Mutex::new(BitVec::with_capacity(1000)),
        }
    }

    pub fn is_committed(&self) -> bool {
        self.state.value() >= StageStatus::Committed.into()
    }

    pub fn set_committed(&self) {
        self.state.set_state(StageStatus::Committed);
    }

    pub fn group_splits_by_worker(&self) -> FastHashMap<InetAddr, Vec<SplitInfo>> {
        let mut grouped = FastHashMap::default();

        for part in self.splits.iter() {
            for split in part.value() {
                grouped
                    .entry(split.worker_addr.clone())
                    .or_insert(vec![])
                    .push(split.clone());
            }
        }
        grouped
    }
}

pub struct AppState {
    pub app_id: String,
    pub last_heartbeat: i64,
    pub state: StateCtl,
}

#[derive(Clone)]
pub struct ShuffleManager {
    rt: Arc<Runtime>,
    shuffles: Arc<FastDashMap<StageKey, Arc<StageSpace>>>,
    pub apps: Arc<FastDashMap<String, AppState>>,
    worker_manager: SyncWorkerManager,
    master_monitor: MasterMonitor,
    context: Arc<ShuffleContext>,
}

impl ShuffleManager {
    pub fn new(
        rt: Arc<Runtime>,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
        context: Arc<ShuffleContext>,
    ) -> Self {
        Self {
            rt,
            shuffles: Arc::new(FastDashMap::default()),
            apps: Arc::new(FastDashMap::default()),
            worker_manager,
            master_monitor,
            context,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.master_monitor.is_active()
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    // Assign a worker to the file, which is responsible for writing data
    fn create_split(
        &self,
        part_id: i32,
        split_id: i32,
        split_size: i64,
        exclude_workers: Vec<u32>,
    ) -> FsResult<SplitInfo> {
        let wm = self.worker_manager.read();
        let ctx = ChooseContext::with_num(1, split_size, exclude_workers);
        let worker = wm.choose_worker(ctx)?;
        let worker = try_option!(worker.first());
        let worker_addr = InetAddr::new(&worker.hostname, self.context.conf().worker_port);
        let split = SplitInfo {
            part_id,
            split_id,
            worker_addr,
            write_len: 0,
        };

        Ok(split)
    }

    fn get_stage(&self, key: &StageKey) -> Arc<StageSpace> {
        if let Some(existing) = self.shuffles.get(key) {
            return existing.value().clone();
        }

        self.shuffles
            .entry(key.clone())
            .or_insert(Arc::new(StageSpace::new(key.clone())))
            .value()
            .clone()
    }

    // When the map task starts, request the master to register and return the target worker to which the data is written.
    // There are three cases:
    // 1. If the current part is not registered, assign a new worker and register the part
    // 2. If the part has been registered and it is not a re-request triggered by a split, return the last split
    // 3. If the part has been registered and it is a re-request triggered by a split:
    //  a. If split_id is smaller than the last split_id, return the last split.
    //  b. If split_id is larger than the last split_id, return an error
    //  c. If split_id is equal to the last split_id, assign a split, register and return the split.
    pub fn alloc_writer(
        &self,
        key: &StageKey,
        part_id: i32,
        split_size: i64,
        full_split: Option<SplitInfo>,
        exclude_workers: Vec<u32>,
    ) -> FsResult<SplitInfo> {
        // get stage
        let stage = self.get_stage(key);

        // get stage all splits
        let mut entry = stage.splits.entry(part_id).or_default();
        let part_splits = entry.value_mut();

        match (full_split, part_splits.last_mut()) {
            (None, None) => {
                let new_split = self.create_split(part_id, 0, split_size, exclude_workers)?;
                part_splits.push(new_split.clone());
                Ok(new_split)
            }

            (None, Some(last)) => Ok(last.clone()),

            (Some(full), Some(last)) => match last.split_id.cmp(&full.split_id) {
                Ordering::Greater => {
                    let last = last.clone();
                    part_splits[full.split_id as usize].write_len = full.write_len;
                    Ok(last)
                }

                Ordering::Equal => {
                    last.write_len = full.write_len;
                    let new_split = self.create_split(
                        part_id,
                        part_splits.len() as i32,
                        split_size,
                        exclude_workers,
                    )?;
                    part_splits.push(new_split.clone());
                    Ok(new_split)
                }

                _ => err_box!("Abnormal status"),
            },

            (Some(_), None) => {
                err_box!("Abnormal status")
            }
        }
    }

    fn commit_task(lock: &mut MutexGuard<BitVec<usize, Lsb0>>, task_id: usize) -> usize {
        if lock.len() <= task_id {
            lock.resize(task_id + 1, false);
        }
        lock.set(task_id, true);
        lock.count_ones()
    }

    pub async fn task_commit(
        &self,
        stage_key: &StageKey,
        task_id: i32,
        num_tasks: i32,
    ) -> FsResult<bool> {
        let stage = self.get_stage(stage_key);

        let mut commit_lock = stage.committed_tasks.lock().await;
        let committed_count = Self::commit_task(&mut commit_lock, task_id as usize);
        if committed_count < num_tasks as usize {
            return Ok(true);
        }

        if stage.is_committed() {
            warn!(
                "stage {} is committed, repeated map id {}",
                stage.key, task_id
            );
        }

        // In any case, we will perform a submission.
        // In special cases, Spark speculates or other reasons to start a copy of the map task,
        // The previous task was not killed normally, triggering the submission, and the new task will also trigger the submission.
        // We need to submit repeatedly to ensure the data is correct.
        let spend = TimeSpent::new();
        let grouped_splits = stage.group_splits_by_worker();
        let mut set: JoinSet<FsResult<Vec<SpiltKey>>> = JoinSet::new();
        for addr in grouped_splits.keys() {
            let addr = addr.clone();
            let context = self.context.clone();
            let key = stage_key.clone();
            set.spawn(async move {
                let client = context.worker_client(&addr).await?;
                let res = client.stage_commit(key).await?;
                Ok(res)
            });
        }

        let mut split_res = vec![];
        while let Some(res) = set.join_next().await {
            match res {
                Ok(res1) => match res1 {
                    Ok(v) => split_res.extend_from_slice(&v),
                    Err(e) => {
                        error!("Stage {} commit error: {:?}", stage_key, e);
                        return Err(e);
                    }
                },
                Err(e) => {
                    error!("Stage {} commit error: {:?}", stage_key, e);
                    return err_box!("Stage {} commit error: {:?}", stage_key, e);
                }
            }
        }

        stage.set_committed();
        info!(
            "Stage {} commit success, cost {} ms",
            stage.key,
            spend.used_ms()
        );
        Ok(true)
    }
}
