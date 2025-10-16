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

use crate::block::block_client::BlockClient;
use crate::file::FsContext;
use curvine_common::proto::DataHeaderProto;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use log::info;
use orpc::common::Utils;
use orpc::err_box;
use orpc::sys::DataSlice;

pub struct BlockWriterRemote {
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    client: BlockClient,
    pos: i64,
    len: i64,
    seq_id: i32,
    req_id: i64,
    pending_header: Option<DataHeaderProto>,
}

impl BlockWriterRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        // Always start from append mode (block.len position)
        let pos = block.len;
        let len = fs_context.block_size();

        let client = fs_context.block_client(&worker_address).await?;
        let write_context = client
            .write_block(
                &block,
                pos,
                len,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                false,
            )
            .await?;

        if len != write_context.len {
            return err_box!(
                "Abnormal block size, expected length {}, actual length {}",
                len,
                write_context.len
            );
        }

        let writer = Self {
            block,
            client,
            pos,
            len,
            seq_id,
            req_id,
            worker_address,
            pending_header: None,
        };

        info!(
            "[BLOCK_WRITER_REMOTE_CREATED] block_id={} worker={:?} pos={} len={} max_written_pos={} req_id={} seq_id={}",
            writer.block.id,
            writer.worker_address,
            pos,
            len,
            writer.max_written_pos,
            req_id,
            seq_id
        );

        Ok(writer)
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    // Write data.
    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let len = chunk.len() as i64;
        let next_seq_id = self.next_seq_id();

        let header = self.pending_header.take();

        self.client
            .write_data(chunk, self.req_id, next_seq_id, header)
            .await?;

        self.pos += len;
        Ok(())
    }

    // refresh.
    pub async fn flush(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .write_flush(self.pos, self.req_id, next_seq_id)
            .await?;

        Ok(())
    }

    // Write complete
    pub async fn complete(&mut self) -> FsResult<()> {
        info!(
            "[BLOCK_WRITER_REMOTE_COMPLETE] block_id={} pos={} max_written_pos={} len={} req_id={} worker={:?}",
            self.block.id,
            self.pos,
            self.max_written_pos,
            self.len,
            self.req_id,
            self.worker_address
        );

        let next_seq_id = self.next_seq_id();
        let result = self.client
            .write_commit(
                &self.block,
                self.pos,
                self.len,
                self.req_id,
                next_seq_id,
                false,
            )
            .await;

        match &result {
            Ok(_) => {
                info!(
                    "[BLOCK_WRITER_REMOTE_COMPLETE_SUCCESS] block_id={} committed pos={} len={}",
                    self.block.id,
                    self.pos,
                    self.len
                );
            }
            Err(e) => {
                info!(
                    "[BLOCK_WRITER_REMOTE_COMPLETE_ERROR] block_id={} error={}",
                    self.block.id,
                    e
                );
            }
        }

        result?;
        Ok(())
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        info!(
            "[BLOCK_WRITER_REMOTE_CANCEL] block_id={} pos={} len={} req_id={} worker={:?}",
            self.block.id,
            self.pos,
            self.len,
            self.req_id,
            self.worker_address
        );

        let next_seq_id = self.next_seq_id();
        let result = self.client
            .write_commit(
                &self.block,
                self.pos,
                self.len,
                self.req_id,
                next_seq_id,
                true,
            )
            .await;

        match &result {
            Ok(_) => {
                info!(
                    "[BLOCK_WRITER_REMOTE_CANCEL_SUCCESS] block_id={} cancelled",
                    self.block.id
                );
            }
            Err(e) => {
                info!(
                    "[BLOCK_WRITER_REMOTE_CANCEL_ERROR] block_id={} error={}",
                    self.block.id,
                    e
                );
            }
        }

        result
    }

    // Get the number of bytes left to writable in the current block.
    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        info!(
            "[BLOCK_WRITER_REMOTE_SEEK_DEBUG] block_id={} current_pos={} max_written_pos={} seek_to={} len={} worker={:?}",
            self.block.id,
            self.pos,
            self.max_written_pos,
            pos,
            self.len,
            self.worker_address
        );

        if pos < 0 {
            info!(
                "[BLOCK_WRITER_REMOTE_SEEK_ERROR] block_id={} negative position: {}",
                self.block.id,
                pos
            );
            return Err(format!("Cannot seek to negative position: {pos}").into());
        }

        if pos >= self.len {
            info!(
                "[BLOCK_WRITER_REMOTE_SEEK_ERROR] block_id={} position {} exceeds capacity {}",
                self.block.id,
                pos,
                self.len
            );
            return Err(format!("Seek position {pos} exceeds block capacity {}", self.len).into());
        }

        // Set new position and pending header
        self.pos = pos;
        self.pending_header = Some(DataHeaderProto {
            offset: pos,
            flush: false,
            is_last: false,
        });

        info!(
            "[BLOCK_WRITER_REMOTE_SEEK_SUCCESS] block_id={} seek to {} completed, pending_header set",
            self.block.id,
            pos
        );

        Ok(())
    }
}
