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

use crate::master::meta::inode::InodePtr;
use curvine_common::proto::CreateFileOptsProto;
use curvine_common::state::{CreateFlag, FileType, StoragePolicy};
use curvine_common::utils::ProtoUtils;
use orpc::common::ByteUnit;
use std::collections::HashMap;

// adBlock request verification result.
#[derive(Debug, Clone)]
pub struct ValidateAddBlock {
    pub replicas: u16,
    pub block_size: i64,
    pub storage_policy: StoragePolicy,
    pub client_host: String,
    pub file_inode: InodePtr,
}

#[derive(Debug, Clone)]
pub struct CreateFileContext {
    pub path: String,

    pub create_flag: CreateFlag,
    pub create_parent: bool,

    pub file_type: FileType,
    pub replicas: u16,
    pub block_size: i64,
    pub x_attr: HashMap<String, Vec<u8>>,
    pub storage_policy: StoragePolicy,

    pub client_name: String,
}

impl CreateFileContext {
    pub fn from_opts(path: String, opts: CreateFileOptsProto) -> CreateFileContext {
        Self {
            path,
            create_flag: CreateFlag::new(opts.create_flag),
            create_parent: opts.create_parent,
            file_type: FileType::from(opts.file_type),
            replicas: opts.replicas as u16,
            block_size: opts.block_size,
            x_attr: opts.x_attr,
            storage_policy: ProtoUtils::storage_policy_from_pb(opts.storage_policy),
            client_name: opts.client_name,
        }
    }

    pub fn with_path<T: AsRef<str>>(path: T, create_parent: bool) -> Self {
        Self {
            path: path.as_ref().to_string(),
            create_flag: Default::default(),
            file_type: FileType::Dir,
            replicas: 1,
            block_size: ByteUnit::MB as i64 * 64,
            x_attr: Default::default(),
            storage_policy: Default::default(),
            create_parent,
            client_name: "".to_string(),
        }
    }

    pub fn with_append<T: AsRef<str>>(path: T) -> Self {
        let mut opts = Self::with_path(path, false);
        opts.create_flag = CreateFlag::new(CreateFlag::APPEND);
        opts
    }

    pub fn create(&self) -> bool {
        self.create_flag.create()
    }

    pub fn overwrite(&self) -> bool {
        self.create_flag.overwrite()
    }

    pub fn append(&self) -> bool {
        self.create_flag.append()
    }
}
