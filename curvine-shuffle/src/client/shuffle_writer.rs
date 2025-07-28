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

use crate::client::{MasterClient, ShuffleContext, WorkerClient};
use crate::protocol::{SplitInfo, StageKey};
use curvine_common::FsResult;
use log::info;
use orpc::sys::DataSlice;
use orpc::try_option_mut;
use std::sync::Arc;

pub struct ShuffleWriter {
    context: Arc<ShuffleContext>,
    master_client: MasterClient,

    stage: StageKey,
    task_id: i32,
    part_id: i32,
    num_tasks: i32,
    split_size: i64,

    cur_split: Option<WorkerClient>,
    split_id: i32,
}

impl ShuffleWriter {
    pub fn new(
        context: Arc<ShuffleContext>,
        stage: StageKey,
        task_id: i32,
        part_id: i32,
        num_tasks: i32,
        split_size: i64,
    ) -> Self {
        let master_client = MasterClient::new(context.connector());
        Self {
            context,
            master_client,

            stage,
            task_id,
            part_id,
            num_tasks,
            split_size,

            cur_split: None,
            split_id: 0,
        }
    }

    pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        let stage = self.stage.clone();
        let part_id = self.part_id;
        let split_id = self.split_id;
        let split_size = self.split_size;

        let client = self.get_cur_split(None).await?;
        let full_split = client
            .write_data(stage, part_id, split_id, split_size, data)
            .await?;
        if full_split.is_some() {
            let _ = self.get_cur_split(full_split).await?;
        }
        Ok(())
    }

    pub async fn commit(&mut self) -> FsResult<()> {
        self.master_client
            .task_commit(&self.stage, self.task_id, self.num_tasks)
            .await
    }

    async fn get_cur_split(
        &mut self,
        full_split: Option<SplitInfo>,
    ) -> FsResult<&mut WorkerClient> {
        let create = full_split.is_some() || self.cur_split.is_none();

        if create {
            let split_info = self
                .master_client
                .alloc_writer(&self.stage, self.part_id, self.split_size, full_split)
                .await?;
            info!(
                "Alloc writer for stage {}, part_id: {}, split_id {}, worker {}",
                self.stage, self.part_id, split_info.split_id, split_info.worker_addr
            );

            let client = self.context.worker_client(&split_info.worker_addr).await?;

            self.split_id = split_info.split_id;
            let _ = self.cur_split.insert(client);
        }

        Ok(try_option_mut!(self.cur_split))
    }
}
