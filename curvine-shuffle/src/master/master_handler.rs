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
use crate::master::ShuffleManager;
use crate::proto::{
    AllocWriterRequest, AllocWriterResponse, TaskCommitRequest, TaskCommitResponse,
};
use crate::protocol::ProtoUtils;
use curvine_common::error::FsError;
use curvine_common::FsResult;
use orpc::handler::MessageHandler;
use orpc::message::{Builder, Message, MessageBuilder};
use orpc::runtime::RpcRuntime;
use orpc::{err_box, err_ext};

pub struct MasterHandler {
    shuffle_manager: ShuffleManager,
}

impl MasterHandler {
    pub fn new(shuffle_manager: ShuffleManager) -> Self {
        Self { shuffle_manager }
    }

    pub fn alloc_writer(&self, msg: &Message) -> FsResult<Message> {
        let header: AllocWriterRequest = msg.parse_header()?;
        let stage = ProtoUtils::stage_key_from_proto(header.stage);
        let full_split = header.full_split.map(ProtoUtils::split_info_from_proto);

        let cur_split = self.shuffle_manager.alloc_writer(
            &stage,
            header.part_id,
            header.split_size,
            full_split,
            vec![],
        )?;
        let rep_header = AllocWriterResponse {
            stage: ProtoUtils::stage_key_to_proto(stage),
            part_id: header.part_id,
            cur_split: ProtoUtils::split_info_to_proto(cur_split),
        };

        Ok(Builder::success(msg).proto_header(rep_header).build())
    }

    pub async fn task_commit(&self, msg: &Message) -> FsResult<Message> {
        let header: TaskCommitRequest = msg.parse_header()?;
        let stage = ProtoUtils::stage_key_from_proto(header.stage);
        let _ = self
            .shuffle_manager
            .task_commit(&stage, header.task_id, header.num_tasks)
            .await?;
        let rep_header = TaskCommitResponse {};

        Ok(MessageBuilder::success(msg)
            .proto_header(rep_header)
            .build())
    }
}

impl MessageHandler for MasterHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        if !self.shuffle_manager.is_leader() {
            return err_ext!(FsError::not_leader(
                "Not the leader node of the shuffle master"
            ));
        }

        let code = ShuffleCode::from(msg.code());
        match code {
            ShuffleCode::AllocWriter => self.alloc_writer(msg),
            ShuffleCode::TaskCommit => self.shuffle_manager.rt().block_on(self.task_commit(msg)),
            _ => err_box!("invalid shuffle code {:?}", code),
        }
    }
}
