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
use crate::protocol::{ProtoUtils, SpiltKey};
use crate::worker::StageManager;
use curvine_common::error::FsError;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::handler::MessageHandler;
use orpc::message::{Message, MessageBuilder};

pub struct WorkerHandler {
    stage_manager: StageManager,
}

impl WorkerHandler {
    pub fn new(stage_manager: StageManager) -> Self {
        Self { stage_manager }
    }

    pub async fn write_data(&self, msg: Message) -> FsResult<Message> {
        let header: WriteDataRequest = msg.parse_header()?;
        let stage = ProtoUtils::stage_key_from_proto(header.stage);
        let split_key = SpiltKey::new(header.part_id, header.split_id);
        let builder = MessageBuilder::success(&msg);

        let full_split = self
            .stage_manager
            .write_data(&stage, &split_key, header.split_size, msg.data)
            .await?;
        let rep_header = WriteDataResponse {
            full_split: full_split.map(ProtoUtils::split_info_to_proto),
        };

        Ok(builder.proto_header(rep_header).build())
    }

    pub async fn stage_commit(&self, msg: Message) -> FsResult<Message> {
        let header: StageCommitRequest = msg.parse_header()?;
        let state_key = ProtoUtils::stage_key_from_proto(header.stage);
        let builder = MessageBuilder::success(&msg);

        let _splits = self.stage_manager.stage_commit(&state_key).await?;
        let rep_header = StageCommitResponse { splits: vec![] };

        Ok(builder.proto_header(rep_header).build())
    }
}

impl MessageHandler for WorkerHandler {
    type Error = FsError;

    fn is_sync(&self, _: &Message) -> bool {
        false
    }

    fn handle(&mut self, _: &Message) -> FsResult<Message> {
        err_box!("Not support")
    }

    async fn async_handle(&mut self, msg: Message) -> FsResult<Message> {
        let code = ShuffleCode::from(msg.code());

        match code {
            ShuffleCode::WriteData => self.write_data(msg).await,
            ShuffleCode::StageCommit => self.stage_commit(msg).await,
            _ => err_box!("unsupported shuffle code {:?}", code),
        }
    }
}
