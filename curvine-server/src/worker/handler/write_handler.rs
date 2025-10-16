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

use crate::worker::block::BlockStore;
use crate::worker::handler::WriteContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::{BlockWriteResponse, DataHeaderProto};
use curvine_common::state::ExtendedBlock;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::TimeSpent;
use orpc::handler::MessageHandler;
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::{err_box, ternary, try_option_mut};
use std::mem;

pub struct WriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<WriteContext>,
    pub(crate) file: Option<LocalFile>,
    pub(crate) is_commit: bool,
    pub(crate) io_slow_us: u64,
    pub(crate) metrics: &'static WorkerMetrics,
}

impl WriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let conf = Worker::get_conf();
        let metrics = Worker::get_metrics();

        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            io_slow_us: conf.worker.io_slow_us(),
            metrics,
        }
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let context = WriteContext::from_req(msg)?;

        info!(
            "[WRITE_HANDLER_OPEN] block_id={} req_id={} off={} len={} block_len={} is_append={} chunk_size={} short_circuit={}",
            context.block.id,
            context.req_id,
            context.off,
            context.len,
            context.block.len,
            context.is_append,
            context.chunk_size,
            context.short_circuit
        );

        // 1. Append mode: use append_block, continue writing from file end
        // 2. Random write mode: use create_block or reuse existing block, support writing at any position
        let meta = if context.is_append {
            info!(
                "[WRITE_HANDLER_APPEND_BLOCK] block_id={} off={}",
                context.block.id,
                context.off
            );
            self.store.append_block(context.off, &context.block)?
        } else {
            info!(
                "[WRITE_HANDLER_CREATE_BLOCK] block_id={}",
                context.block.id
            );
            self.store.create_block(&context.block)?
        };

        // Only check if it exceeds block capacity limit
        if context.off >= context.len {
            info!(
                "[WRITE_HANDLER_CAPACITY_ERROR] block_id={} off={} exceeds len={}",
                context.block.id,
                context.off,
                context.len
            );
            return err_box!(
                "The write start position {} exceeds the block capacity {}",
                context.off,
                context.len
            );
        }

        let file = if context.is_append {
            // Append mode: use traditional append method
            let file = meta.create_writer(true)?;
            let file_len = file.len();
            info!(
                "[WRITE_HANDLER_APPEND_FILE] block_id={} file_len={} expected_off={}",
                context.block.id,
                file_len,
                context.off
            );
            // Validate file length for append mode
            if file_len != context.off {
                info!(
                    "[WRITE_HANDLER_APPEND_LENGTH_ERROR] block_id={} expected={} actual={}",
                    context.block.id,
                    context.off,
                    file_len
                );
                return err_box!(
                    "Append write initial length error, expected {}, actual {}",
                    context.off,
                    file_len
                );
            }
            file
        } else {
            // Random write mode: use write method with offset support
            let mut file = meta.create_writer(false)?;
            let initial_len = file.len();
            info!(
                "[WRITE_HANDLER_RANDOM_FILE] block_id={} initial_len={} seek_to={}",
                context.block.id,
                initial_len,
                context.off
            );
            // For random writes, seek directly to specified position
            if context.off > 0 {
                file.seek(context.off)?;
                info!(
                    "[WRITE_HANDLER_SEEK_SUCCESS] block_id={} seek_to={} new_pos={}",
                    context.block.id,
                    context.off,
                    file.pos()
                );
            }
            file
        };

        let (label, path, file) = if context.short_circuit {
            ("local", file.path().to_string(), None)
        } else {
            ("remote", file.path().to_string(), Some(file))
        };

        info!(
            "[WRITE_HANDLER_OPEN_SUCCESS] block_id={} req_id={} mode={} path={} meta_id={} meta_len={} meta_storage_type={:?}",
            context.block.id,
            context.req_id,
            label,
            path,
            meta.id,
            meta.len,
            meta.storage_type()
        );

        let response = BlockWriteResponse {
            id: meta.id,
            path: ternary!(context.short_circuit, Some(path), None),
            off: context.off,
            len: context.len,
            storage_type: meta.storage_type().into(),
        };

        let _ = mem::replace(&mut self.file, file);
        let _ = self.context.replace(context);

        self.metrics.write_blocks.with_label_values(&[label]).inc();

        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn check_context(context: &WriteContext, msg: &Message) -> FsResult<()> {
        if context.req_id != msg.req_id() {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                msg.req_id()
            );
        }
        Ok(())
    }

    pub fn write(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);
        Self::check_context(context, msg)?;

        let initial_pos = file.pos();
        let data_len = msg.data_len() as i64;

        info!(
            "[WRITE_HANDLER_WRITE] block_id={} req_id={} initial_pos={} data_len={} header_len={}",
            context.block.id,
            context.req_id,
            initial_pos,
            data_len,
            msg.header_len()
        );

        // msg.header
        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            info!(
                "[WRITE_HANDLER_HEADER] block_id={} header_offset={} flush={} is_last={} current_pos={}",
                context.block.id,
                header.offset,
                header.flush,
                header.is_last,
                file.pos()
            );

            // If header contains offset information, execute seek operation
            if header.offset != file.pos() {
                if header.offset < 0 || header.offset >= context.len {
                    info!(
                        "[WRITE_HANDLER_SEEK_ERROR] block_id={} invalid_offset={} block_len={}",
                        context.block.id,
                        header.offset,
                        context.len
                    );
                    return err_box!(
                        "Invalid seek offset: {}, block length: {}",
                        header.offset,
                        context.len
                    );
                }
                file.seek(header.offset)?;
                info!(
                    "[WRITE_HANDLER_SEEK] block_id={} seek_from={} seek_to={} new_pos={}",
                    context.block.id,
                    initial_pos,
                    header.offset,
                    file.pos()
                );
            }

            // Handle flush request
            if header.flush {
                info!(
                    "[WRITE_HANDLER_FLUSH] block_id={} pos={}",
                    context.block.id,
                    file.pos()
                );
                file.flush()?;
            }
        }

        // Write existing data blocks.
        if data_len > 0 {
            let write_start = file.pos();
            let write_end = write_start + data_len;

            if write_end > context.len {
                info!(
                    "[WRITE_HANDLER_RANGE_ERROR] block_id={} write_range=[{}, {}) exceeds block_len={}",
                    context.block.id,
                    write_start,
                    write_end,
                    context.len
                );
                return err_box!(
                    "Write range [{}, {}) exceeds block size {}",
                    write_start,
                    write_end,
                    context.len
                );
            }

            info!(
                "[WRITE_HANDLER_WRITE_DATA] block_id={} write_range=[{}, {}) data_len={}",
                context.block.id,
                write_start,
                write_end,
                data_len
            );

            let spend = TimeSpent::new();
            file.write_region(&msg.data)?;

            let used = spend.used_us();
            let final_pos = file.pos();

            info!(
                "[WRITE_HANDLER_WRITE_SUCCESS] block_id={} write_range=[{}, {}) final_pos={} time_us={}",
                context.block.id,
                write_start,
                write_end,
                final_pos,
                used
            );

            if used >= self.io_slow_us {
                warn!(
                    "Slow write data from disk cost: {}us (threshold={}us), path: {} ",
                    used,
                    self.io_slow_us,
                    file.path()
                );
                self.metrics.write_slow_count.inc();
            }
            self.metrics.write_bytes.inc_by(msg.data_len() as i64);
            self.metrics.write_time_us.inc_by(used as i64);
        }

        Ok(msg.success())
    }

    fn commit_block(&self, block: &ExtendedBlock, commit: bool) -> FsResult<()> {
        info!(
            "[WRITE_HANDLER_COMMIT_BLOCK] block_id={} commit={} block_len={}",
            block.id,
            commit,
            block.len
        );

        if commit {
            self.store.finalize_block(block)?;
            info!(
                "[WRITE_HANDLER_FINALIZE_SUCCESS] block_id={}",
                block.id
            );
        } else {
            self.store.abort_block(block)?;
            info!(
                "[WRITE_HANDLER_ABORT_SUCCESS] block_id={}",
                block.id
            );
        }
        Ok(())
    }

    pub fn complete(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        info!(
            "[WRITE_HANDLER_COMPLETE] req_id={} commit={} is_already_commit={}",
            msg.req_id(),
            commit,
            self.is_commit
        );

        if self.is_commit {
            info!(
                "[WRITE_HANDLER_ALREADY_COMMIT] req_id={} data_len={}",
                msg.req_id(),
                msg.data_len()
            );
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        let block = match self.context.take() {
            Some(v) => {
                // Remote writing
                Self::check_context(&v, msg)?;
                info!(
                    "[WRITE_HANDLER_REMOTE_COMPLETE] block_id={} req_id={}",
                    v.block.id,
                    v.req_id
                );
                v.block
            }

            None => {
                // Local short-circuit write, block information is in the header.
                let c = WriteContext::from_req(msg)?;
                info!(
                    "[WRITE_HANDLER_LOCAL_COMPLETE] block_id={} req_id={}",
                    c.block.id,
                    c.req_id
                );
                c.block
            }
        };

        // flush and close the file.
        let file = self.file.take();
        if let Some(mut file) = file {
            let final_pos = file.pos();
            let final_len = file.len();
            info!(
                "[WRITE_HANDLER_FILE_CLOSE] block_id={} final_pos={} final_len={} path={}",
                block.id,
                final_pos,
                final_len,
                file.path()
            );
            file.flush()?;
            drop(file);
        }

        // Submit block.
        let final_commit = commit || block.is_stream();
        info!(
            "[WRITE_HANDLER_BLOCK_COMMIT] block_id={} commit={} is_stream={} final_commit={}",
            block.id,
            commit,
            block.is_stream(),
            final_commit
        );

        self.commit_block(&block, final_commit)?;
        self.is_commit = true;

        info!(
            "[WRITE_HANDLER_COMPLETE_SUCCESS] block_id={} req_id={} final_commit={}",
            block.id,
            msg.req_id(),
            final_commit
        );

        Ok(msg.success())
    }
}

// If the write process suddenly interrupts, we will not delete the unwritten block.
/*impl Drop for WriteHandler {
    fn drop(&mut self) {
        if !self.is_commit {
            if let Err(e) = self.commit_block(false) {
                error!("Abort block {}", e);
            }
            self.is_commit = true;
        }
    }
}
*/

impl MessageHandler for WriteHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();

        match request_status {
            RequestStatus::Open => self.open(msg),

            RequestStatus::Running => self.write(msg),

            RequestStatus::Complete => self.complete(msg, true),

            RequestStatus::Cancel => self.complete(msg, false),

            _ => err_box!("Unsupported request type"),
        }
    }
}
