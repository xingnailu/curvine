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
use crate::common::ShuffleUtils;
use crate::protocol::{SpiltKey, SplitInfo, StageKey, StageReport};
use crate::worker::SplitWriter;
use curvine_client::file::CurvineFileSystem;
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::common::TimeSpent;
use orpc::err_box;
use orpc::sync::FastDashMap;
use orpc::sys::DataSlice;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

struct StageWriters {
    stage_key: StageKey,
    writers: FastDashMap<SpiltKey, Arc<Mutex<SplitWriter>>>,
}

impl StageWriters {
    fn new(stage: StageKey) -> Self {
        Self {
            stage_key: stage,
            writers: FastDashMap::default(),
        }
    }

    pub fn get_all_writer(&self) -> Vec<Arc<Mutex<SplitWriter>>> {
        let mut list = vec![];
        for item in self.writers.iter() {
            list.push(item.value().clone())
        }
        list
    }
}

#[derive(Clone)]
pub struct StageManager {
    stages: Arc<FastDashMap<StageKey, Arc<StageWriters>>>,
    fs: CurvineFileSystem,
    context: Arc<ShuffleContext>,
}

impl StageManager {
    pub fn new(fs: CurvineFileSystem, context: Arc<ShuffleContext>) -> Self {
        Self {
            stages: Arc::new(FastDashMap::with_capacity(4096)),
            fs,
            context,
        }
    }

    fn get_stage(&self, key: &StageKey) -> Arc<StageWriters> {
        if let Some(existing) = self.stages.get(key) {
            return existing.value().clone();
        }

        let entry = self
            .stages
            .entry(key.clone())
            .or_insert(Arc::new(StageWriters::new(key.clone())));
        entry.value().clone()
    }

    async fn get_writer(
        &self,
        stage_key: &StageKey,
        split_key: &SpiltKey,
        split_size: i64,
    ) -> FsResult<Arc<Mutex<SplitWriter>>> {
        let stage = self.get_stage(stage_key);

        if let Some(entry) = stage.writers.get(split_key) {
            // If the file has been submitted, you need to reopen it in append mode.
            let lock = entry.value().lock().await;

            if !lock.is_init() {
                return err_box!("writer {} not init", lock.path());
            }

            if lock.is_complete() {
                warn!(
                    "Stage {} already complete, {} reopen",
                    stage_key,
                    lock.path()
                );
            } else {
                return Ok(entry.value().clone());
            }
        }

        let spent = TimeSpent::new();
        let lock_writer = stage
            .writers
            .entry(split_key.clone())
            .or_insert(Arc::new(Mutex::new(SplitWriter::new(
                split_key.clone(),
                split_size,
            ))))
            .value()
            .clone();

        let mut lock = lock_writer.lock().await;
        if !lock.is_init() || lock.is_complete() {
            let path =
                ShuffleUtils::get_split_path(self.context.root_dir(), &stage.stage_key, split_key)?;
            let writer = self.fs.append_direct(&path, true).await?;

            lock.set_writer(writer);
            drop(lock);
            info!("Create writer {} cost {} ms", path, spent.used_ms());
        } else {
            drop(lock);
        }

        Ok(lock_writer)
    }

    pub async fn write_data(
        &self,
        stage_key: &StageKey,
        split_key: &SpiltKey,
        split_size: i64,
        data: DataSlice,
    ) -> FsResult<Option<SplitInfo>> {
        let writer = self.get_writer(stage_key, split_key, split_size).await?;

        let mut lock = writer.lock().await;
        let split_info = if lock.write(data).await? {
            Some(lock.to_split_info())
        } else {
            None
        };

        Ok(split_info)
    }

    pub async fn stage_commit(&self, stage_key: &StageKey) -> FsResult<Vec<SpiltKey>> {
        let stage = self.get_stage(stage_key);

        let spend = TimeSpent::new();
        let writers = stage.get_all_writer();

        let mut set: JoinSet<FsResult<SpiltKey>> = JoinSet::new();
        for writer in writers {
            set.spawn(async move {
                let spend = TimeSpent::new();

                let mut writer = writer.lock().await;
                writer.complete().await?;

                info!(
                    "writer {} complete, cost {} ms",
                    writer.path(),
                    spend.used_ms()
                );
                Ok(writer.split_key.clone())
            });
        }

        let mut split_res = vec![];
        while let Some(res) = set.join_next().await {
            match res {
                Ok(res1) => match res1 {
                    Ok(v) => split_res.push(v),
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

        info!(
            "Stage {} commit success, cost {} ms, writer num {}",
            stage_key,
            spend.used_ms(),
            split_res.len()
        );

        Ok(split_res)
    }

    fn get_app_stages(&self, app_id: &str) -> Vec<Arc<StageWriters>> {
        let mut res = vec![];
        for item in self.stages.iter() {
            if item.key().app_id == app_id {
                res.push(item.value().clone());
            }
        }
        res
    }

    pub async fn app_commit(&self, app_id: &str) -> FsResult<()> {
        let stages = self.get_app_stages(app_id);
        for item in &stages {
            self.stages.remove(&item.stage_key);
        }

        info!(
            "App {} commit success, delete stage num {}",
            app_id,
            stages.len()
        );
        Ok(())
    }

    fn get_all_stage(&self) -> Vec<Arc<StageWriters>> {
        let mut res = vec![];
        for item in self.stages.iter() {
            res.push(item.value().clone());
        }
        res
    }

    // Heartbeat reports all stages
    pub async fn get_report_stages(&self) -> Vec<StageReport> {
        let stages = self.get_all_stage();

        let mut res = vec![];
        for item in stages {
            let mut write_len = 0;
            let writers = item.get_all_writer();
            for writer in writers {
                let lock = writer.lock().await;
                write_len += lock.pos();
            }

            res.push(StageReport {
                stage_key: item.stage_key.clone(),
                write_len,
            })
        }

        res
    }
}
