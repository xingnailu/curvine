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
use crate::proto::{StageCommitRequest, StageCommitResponse, WriteDataRequest, WriteDataResponse};
use crate::protocol::{ProtoUtils, SpiltKey, SplitInfo, StageKey};
use curvine_common::FsResult;
use orpc::client::{ClusterConnector, RpcClient};
use orpc::io::net::InetAddr;
use orpc::io::retry::TimeBondedRetryBuilder;
use orpc::message::{Builder, Message};
use orpc::sys::DataSlice;
use std::time::Duration;

#[derive(Clone)]
pub struct WorkerClient {
    client: RpcClient,
    timeout: Duration,
    retry_builder: TimeBondedRetryBuilder,
}

impl WorkerClient {
    pub async fn new(connector: &ClusterConnector, addr: InetAddr) -> FsResult<Self> {
        let client = connector.get_client(&addr).await?;
        Ok(Self {
            client,
            timeout: connector.data_timeout(),
            retry_builder: connector.retry_builder(),
        })
    }

    async fn rpc(&self, msg: Message) -> FsResult<Message> {
        let rep_msg = self
            .client
            .retry_rpc(self.timeout, self.retry_builder.build(), msg)
            .await?;
        Ok(rep_msg)
    }

    pub async fn write_data(
        &self,
        stage: StageKey,
        part_id: i32,
        split_id: i32,
        split_size: i64,
        data: DataSlice,
    ) -> FsResult<Option<SplitInfo>> {
        let header = WriteDataRequest {
            stage: ProtoUtils::stage_key_to_proto(stage),
            part_id,
            split_id,
            split_size,
        };
        let msg = Builder::new_rpc(ShuffleCode::WriteData)
            .proto_header(header)
            .data(data)
            .build();

        let rep_msg = self.rpc(msg).await?;
        let rep_header: WriteDataResponse = rep_msg.parse_header()?;
        let full_split = rep_header.full_split.map(ProtoUtils::split_info_from_proto);
        Ok(full_split)
    }

    pub async fn stage_commit(&self, stage_key: StageKey) -> FsResult<Vec<SpiltKey>> {
        let header = StageCommitRequest {
            stage: ProtoUtils::stage_key_to_proto(stage_key),
        };
        let msg = Builder::new_rpc(ShuffleCode::StageCommit)
            .proto_header(header)
            .build();

        let rep_msg = self.rpc(msg).await?;
        let rep_header: StageCommitResponse = rep_msg.parse_header()?;
        let res: Vec<SpiltKey> = rep_header
            .splits
            .into_iter()
            .map(ProtoUtils::split_key_from_proto)
            .collect();
        Ok(res)
    }
}
