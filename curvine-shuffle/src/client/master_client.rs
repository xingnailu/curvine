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

use crate::common::ShuffleCode;
use crate::proto::{
    AllocWriterRequest, AllocWriterResponse, TaskCommitRequest, TaskCommitResponse,
};
use crate::protocol::{ProtoUtils, SplitInfo, StageKey};
use curvine_common::error::FsError;
use curvine_common::FsResult;
use orpc::client::ClusterConnector;
use std::sync::Arc;

#[derive(Clone)]
pub struct MasterClient {
    connector: Arc<ClusterConnector>,
}

impl MasterClient {
    pub fn new(connector: Arc<ClusterConnector>) -> Self {
        Self { connector }
    }

    pub async fn alloc_writer(
        &self,
        stage: &StageKey,
        part_id: i32,
        split_size: i64,
        full_split: Option<SplitInfo>,
    ) -> FsResult<SplitInfo> {
        let header = AllocWriterRequest {
            stage: ProtoUtils::stage_key_to_proto(stage.clone()),
            part_id,
            split_size,
            full_split: full_split.map(ProtoUtils::split_info_to_proto),
            exclude_workers: vec![],
        };

        let rep_header: AllocWriterResponse = self
            .connector
            .proto_rpc::<_, _, FsError>(ShuffleCode::AllocWriter, header)
            .await?;
        let split = ProtoUtils::split_info_from_proto(rep_header.cur_split);
        Ok(split)
    }

    pub async fn task_commit(
        &self,
        stage: &StageKey,
        task_id: i32,
        num_tasks: i32,
    ) -> FsResult<()> {
        let header = TaskCommitRequest {
            stage: ProtoUtils::stage_key_to_proto(stage.clone()),
            task_id,
            num_tasks,
        };
        let _: TaskCommitResponse = self
            .connector
            .proto_rpc::<_, _, FsError>(ShuffleCode::TaskCommit, header)
            .await?;
        Ok(())
    }
}
