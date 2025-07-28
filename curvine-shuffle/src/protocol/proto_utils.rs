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

use crate::proto::InetAddrProto;
use crate::proto::{SplitInfoProto, SplitKeyProto, StageKeyProto};
use crate::protocol::{SpiltKey, SplitInfo, StageKey};
use orpc::io::net::InetAddr;

pub struct ProtoUtils;

impl ProtoUtils {
    pub fn stage_key_from_proto(stage: StageKeyProto) -> StageKey {
        StageKey {
            app_id: stage.app_id,
            stage_id: stage.stage_id,
        }
    }

    pub fn stage_key_to_proto(stage: StageKey) -> StageKeyProto {
        StageKeyProto {
            app_id: stage.app_id,
            stage_id: stage.stage_id,
        }
    }

    pub fn split_key_to_proto(split: SpiltKey) -> SplitKeyProto {
        SplitKeyProto {
            part_id: split.part_id,
            split_id: split.split_id,
        }
    }

    pub fn split_key_from_proto(split: SplitKeyProto) -> SpiltKey {
        SpiltKey {
            part_id: split.part_id,
            split_id: split.split_id,
        }
    }

    pub fn split_info_from_proto(split: SplitInfoProto) -> SplitInfo {
        SplitInfo {
            part_id: 0,
            split_id: split.split_id,
            worker_addr: InetAddr::new(split.worker_addr.hostname, split.worker_addr.port as u16),
            write_len: split.write_len,
        }
    }

    pub fn split_info_to_proto(split: SplitInfo) -> SplitInfoProto {
        SplitInfoProto {
            part_id: split.part_id,
            split_id: split.split_id,
            write_len: split.write_len,
            worker_addr: InetAddrProto {
                hostname: split.worker_addr.hostname,
                port: split.worker_addr.port as i32,
            },
        }
    }
}
