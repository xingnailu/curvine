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

use crate::master::meta::block_meta::BlockState;
use crate::master::meta::feature::{AclFeature, FileFeature, WriteFeature};
use crate::master::meta::inode::{Inode, EMPTY_PARENT_ID};
use crate::master::meta::{BlockMeta, InodeId};
use curvine_common::state::{CommitBlock, CreateFileOpts, ExtendedBlock, FileType, StoragePolicy};
use orpc::{err_box, ternary, CommonResult};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeFile {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) file_type: FileType,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,

    pub(crate) len: i64,
    pub(crate) block_size: i64,
    pub(crate) replicas: u16,

    pub(crate) storage_policy: StoragePolicy,

    pub(crate) features: FileFeature,

    pub(crate) blocks: Vec<BlockMeta>,

    // Number of hard links to this file
    pub(crate) nlink: u32,

    // Next sequence number for block ID generation (auto-increment)
    pub(crate) next_seq: u32,

    pub(crate) target: Option<String>,
}

impl InodeFile {
    pub fn new(id: i64, time: i64) -> Self {
        Self {
            id,
            file_type: FileType::File,
            mtime: time,
            atime: time,
            len: 0,
            block_size: 0,
            replicas: 0,

            storage_policy: Default::default(),
            features: FileFeature::new(),

            blocks: vec![],
            nlink: 1,
            next_seq: 0,
            target: None,
            parent_id: EMPTY_PARENT_ID,
        }
    }

    pub fn with_opts(id: i64, time: i64, opts: CreateFileOpts) -> InodeFile {
        let mut file = Self {
            id,
            file_type: opts.file_type,
            mtime: time,
            atime: time,
            len: 0,
            block_size: opts.block_size,
            replicas: opts.replicas,

            storage_policy: opts.storage_policy,
            features: FileFeature::new(),

            blocks: vec![],
            nlink: 1,
            next_seq: 0,
            target: None,
            parent_id: EMPTY_PARENT_ID,
        };

        file.features.set_writing(opts.client_name);
        if !opts.x_attr.is_empty() {
            file.features.set_attrs(opts.x_attr);
        }

        file.features.set_mode(opts.mode);

        file
    }

    pub fn with_link(id: i64, time: i64, target: impl Into<String>, mode: u32) -> Self {
        Self {
            id,
            file_type: FileType::Link,
            mtime: time,
            atime: time,
            len: 0,
            block_size: 0,
            replicas: 0,

            storage_policy: Default::default(),
            features: FileFeature {
                x_attr: Default::default(),
                file_write: None,
                acl: AclFeature::with_mode(mode),
            },

            blocks: vec![],
            nlink: 1,
            next_seq: 0,
            target: Some(target.into()),
            parent_id: EMPTY_PARENT_ID,
        }
    }

    pub fn block_ids(&self) -> Vec<i64> {
        self.blocks.iter().map(|x| x.id).collect()
    }

    pub fn is_complete(&self) -> bool {
        self.features.file_write.is_none()
    }

    pub fn is_writing(&self) -> bool {
        self.features.file_write.is_some()
    }

    pub fn write_feature(&self) -> Option<&WriteFeature> {
        self.features.file_write.as_ref()
    }

    pub fn add_block(&mut self, id: BlockMeta) {
        self.blocks.push(id)
    }

    pub fn compute_len(&self) -> i64 {
        let mut sum = 0;
        log::info!(
            "🧮 [InodeFile::compute_len] Starting calculation for file_id={}, total_blocks={}",
            self.id,
            self.blocks.len()
        );
        
        for (i, x) in self.blocks.iter().enumerate() {
            let is_last = i == self.blocks.len() - 1;
            let is_committed = x.is_committed();
            let block_len = if is_last && !is_committed { 0 } else { x.len };
            
            log::info!(
                "📦 [InodeFile::compute_len] Block[{}]: id={}, len={}, committed={}, is_last={}, contributing={}",
                i, x.id, x.len, is_committed, is_last, block_len
            );
            
            sum += block_len;
        }
        
        log::info!(
            "🧮 [InodeFile::compute_len] Final computed length for file_id={}: {}",
            self.id,
            sum
        );
        sum
    }

    pub fn commit_len(&self, last: Option<&CommitBlock>) -> i64 {
        let computed_len = self.compute_len();
        let last_block_len = last.map(|x| x.block_len).unwrap_or(0);
        let total_len = computed_len + last_block_len;
        
        log::info!(
            "📏 [InodeFile::commit_len] file_id={}, computed_len={}, last_block_len={}, total_commit_len={}",
            self.id,
            computed_len,
            last_block_len,
            total_len
        );
        
        if let Some(commit_block) = last {
            log::info!(
                "📋 [InodeFile::commit_len] CommitBlock details: block_id={}, block_len={}",
                commit_block.block_id,
                commit_block.block_len
            );
        }
        
        total_len
    }

    fn calc_pos(&self, pos: i32) -> usize {
        if pos < 0 {
            (self.blocks.len() as i32 + pos) as usize
        } else {
            pos as usize
        }
    }

    pub fn get_block(&self, pos: i32) -> Option<&BlockMeta> {
        let pos = self.calc_pos(pos);
        if pos < self.blocks.len() {
            Some(&self.blocks[pos])
        } else {
            None
        }
    }

    pub fn get_block_mut(&mut self, pos: i32) -> Option<&mut BlockMeta> {
        let pos = self.calc_pos(pos);
        if pos < self.blocks.len() {
            Some(&mut self.blocks[pos])
        } else {
            None
        }
    }

    pub fn get_block_check(&self, pos: i32) -> CommonResult<&BlockMeta> {
        match self.get_block(pos) {
            None => err_box!("Not found block, pos = {}", pos),
            Some(v) => Ok(v),
        }
    }

    pub fn append(&mut self, client_name: impl AsRef<str>) -> Option<ExtendedBlock> {
        let _ = self
            .features
            .file_write
            .insert(WriteFeature::new(client_name.as_ref().to_string()));

        if let Some(last_block) = self.get_block_mut(-1) {
            last_block.state = BlockState::Writing;
            let blk = ExtendedBlock {
                id: last_block.id,
                len: last_block.len,
                storage_type: self.storage_policy.storage_type,
                file_type: self.file_type,
            };
            Some(blk)
        } else {
            None
        }
    }

    /// Create a new block id
    /// It is composed of the inode id + block number of the file, starting from 1.
    /// inode id + serial number 0, is the file id.
    pub fn next_block_id(&mut self) -> CommonResult<i64> {
        let seq = self.next_seq as i64;
        self.next_seq += 1;
        InodeId::create_block_id(self.id, seq)
    }

    pub fn simple_string(&self) -> String {
        format!(
            "id={}, pid={}, len={}, nlink={}, blocks={:?}",
            self.id,
            self.parent_id,
            self.len,
            self.nlink,
            self.block_ids()
        )
    }

    // Increment link count
    pub fn increment_nlink(&mut self) {
        self.nlink += 1;
    }

    // Decrement link count
    pub fn decrement_nlink(&mut self) -> u32 {
        if self.nlink > 0 {
            self.nlink -= 1;
        }
        self.nlink
    }

    // Get current link count
    pub fn nlink(&self) -> u32 {
        self.nlink
    }

    // Check if this is the last link
    pub fn is_last_link(&self) -> bool {
        self.nlink <= 1
    }

    /// Update file metadata for overwrite operation
    pub fn overwrite(&mut self, opts: CreateFileOpts, mtime: i64) {
        // Clear all blocks and reset file size
        self.blocks.clear();
        self.len = 0;

        // Update file metadata with new options
        self.replicas = opts.replicas;
        self.block_size = opts.block_size;
        self.storage_policy = opts.storage_policy;
        self.mtime = mtime;

        // Reset file writing state for new write operation
        self.features.set_writing(opts.client_name);
    }
}

impl Inode for InodeFile {
    fn id(&self) -> i64 {
        self.id
    }

    fn parent_id(&self) -> i64 {
        self.parent_id
    }

    fn is_dir(&self) -> bool {
        false
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }

    fn atime(&self) -> i64 {
        self.atime
    }
}

impl PartialEq for InodeFile {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
