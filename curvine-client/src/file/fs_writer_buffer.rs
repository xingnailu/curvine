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

use crate::file::FsWriterBase;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use log::{error, info};
use orpc::runtime::RpcRuntime;
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
use std::sync::Arc;

// Control task type
enum WriterTask {
    Flush(CallSender<i8>),
    Complete((bool, CallSender<i8>)),
    Seek((i64, CallSender<i8>)),
}

enum SelectTask {
    Control(WriterTask),
    Data(DataSlice),
}

// Reader with buffer.
pub struct FsWriterBuffer {
    path: Path,
    status: FileStatus,
    chunk_sender: AsyncSender<DataSlice>,
    task_sender: AsyncSender<WriterTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    pos: i64,
}

impl FsWriterBuffer {
    pub fn new(writer: FsWriterBase, chunk_num: usize) -> Self {
        let (chunk_sender, chunk_receiver) = AsyncChannel::new(chunk_num).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();

        let buf_writer = Self {
            path: writer.path().clone(),
            status: writer.status().clone(),
            chunk_sender,
            task_sender,
            err_monitor: Arc::new(ErrorMonitor::new()),
            pos: writer.pos(),
        };

        let rt = writer.fs_context().clone_runtime();
        let err_monitor = buf_writer.err_monitor.clone();
        rt.spawn(async move {
            let res = Self::write_future(chunk_receiver, task_receiver, writer).await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!("buffer writer error: {:?}", e);
                    err_monitor.set_error(e);
                }
            }
        });

        buf_writer
    }

    pub fn path_str(&self) -> &str {
        self.path.path()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    pub fn check_error(&self, e: FsError) -> FsError {
        match self.err_monitor.take_error() {
            Some(e) => e,
            None => e,
        }
    }

    pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        let len = data.len();
        if len == 0 {
            return Ok(());
        }
        match self.chunk_sender.send(data).await {
            Ok(_) => {
                self.pos += len as i64;
                Ok(())
            }

            Err(e) => Err(self.check_error(e.into())),
        }
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();
            self.task_sender
                .send(WriterTask::Complete((false, tx)))
                .await?;
            rx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();
            self.task_sender
                .send(WriterTask::Complete((true, tx)))
                .await?;
            rx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Flush(tx)).await?;
            rx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        info!(
            "[FS_WRITER_BUFFER_SEEK_START] path={} current_pos={} seek_to={} status={:?}",
            self.path(),
            self.pos,
            pos,
            self.status
        );

        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();

            info!(
                "[FS_WRITER_BUFFER_SEEK_SEND_TASK] path={} seek_to={} sending_task_to_background",
                self.path(),
                pos
            );

            if let Err(e) = self.task_sender.send(WriterTask::Seek((pos, tx))).await {
                info!(
                    "[FS_WRITER_BUFFER_SEEK_SEND_ERROR] path={} seek_to={} error={}",
                    self.path(),
                    pos,
                    e
                );
                return Err(e.into());
            }

            info!(
                "[FS_WRITER_BUFFER_SEEK_WAIT_RESPONSE] path={} seek_to={} waiting_for_background_task",
                self.path(),
                pos
            );

            if let Err(e) = rx.receive().await {
                info!(
                    "[FS_WRITER_BUFFER_SEEK_RECEIVE_ERROR] path={} seek_to={} error={}",
                    self.path(),
                    pos,
                    e
                );
                return Err(e.into());
            }

            info!(
                "[FS_WRITER_BUFFER_SEEK_BACKGROUND_SUCCESS] path={} seek_to={} background_task_completed",
                self.path(),
                pos
            );
            Ok(())
        };

        // Update position (note: actual seek is executed by background task)
        if res.is_ok() {
            self.pos = pos;
            info!(
                "[FS_WRITER_BUFFER_SEEK_UPDATE_POS] path={} updated_local_pos={}",
                self.path(),
                pos
            );
        }

        match res {
            Err(e) => {
                let checked_error = self.check_error(e);
                info!(
                    "[FS_WRITER_BUFFER_SEEK_ERROR] path={} seek_to={} final_error={}",
                    self.path(),
                    pos,
                    checked_error
                );
                Err(checked_error)
            }
            Ok(_) => {
                info!(
                    "[FS_WRITER_BUFFER_SEEK_SUCCESS] path={} seek_to={} completed_successfully",
                    self.path(),
                    pos
                );
                Ok(())
            }
        }
    }

    async fn write_future(
        mut chunk_receiver: AsyncReceiver<DataSlice>,
        mut task_receiver: AsyncReceiver<WriterTask>,
        mut writer: FsWriterBase,
    ) -> FsResult<()> {
        loop {
            // The queue can be written and controlled to complete any future.
            let select_task = tokio::select! {
                biased;

                chunk = chunk_receiver.recv_check() => {
                   SelectTask::Data(chunk?)
                }

                task = task_receiver.recv_check() => {
                    SelectTask::Control(task?)
                }
            };

            match select_task {
                SelectTask::Control(task) => match task {
                    WriterTask::Flush(cx) => {
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?;
                        }
                        writer.flush().await?;
                        cx.send(1)?;
                    }

                    WriterTask::Complete((_, tx)) => {
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?;
                        }
                        writer.complete().await?;
                        tx.send(1)?;
                        return Ok(());
                    }

                    WriterTask::Seek((pos, tx)) => {
                        info!(
                            "[FS_WRITER_BUFFER_TASK_SEEK_START] path={} seek_to={} current_writer_pos={} processing_buffered_data",
                            writer.path(),
                            pos,
                            writer.pos()
                        );

                        // Process all buffered data first
                        let mut buffered_chunks = 0;
                        let mut buffered_bytes = 0;
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            buffered_chunks += 1;
                            buffered_bytes += chunk.len();
                            writer.write(chunk).await?
                        }

                        info!(
                            "[FS_WRITER_BUFFER_TASK_SEEK_BUFFERED] path={} seek_to={} processed_chunks={} processed_bytes={} new_writer_pos={}",
                            writer.path(),
                            pos,
                            buffered_chunks,
                            buffered_bytes,
                            writer.pos()
                        );

                        // Execute seek operation
                        info!(
                            "[FS_WRITER_BUFFER_TASK_SEEK_EXECUTE] path={} seek_to={} executing_writer_seek",
                            writer.path(),
                            pos
                        );

                        match writer.seek(pos).await {
                            Ok(_) => {
                                info!(
                                    "[FS_WRITER_BUFFER_TASK_SEEK_WRITER_SUCCESS] path={} seek_to={} writer_seek_completed new_pos={}",
                                    writer.path(),
                                    pos,
                                    writer.pos()
                                );
                            }
                            Err(e) => {
                                info!(
                                    "[FS_WRITER_BUFFER_TASK_SEEK_WRITER_ERROR] path={} seek_to={} writer_seek_failed error={}",
                                    writer.path(),
                                    pos,
                                    e
                                );
                                return Err(e);
                            }
                        }

                        if let Err(e) = tx.send(1) {
                            info!(
                                "[FS_WRITER_BUFFER_TASK_SEEK_RESPONSE_ERROR] path={} seek_to={} failed_to_send_response error={}",
                                writer.path(),
                                pos,
                                e
                            );
                            return Err(e.into());
                        }

                        info!(
                            "[FS_WRITER_BUFFER_TASK_SEEK_COMPLETE] path={} seek_to={} task_completed_successfully",
                            writer.path(),
                            pos
                        );
                    }
                },

                SelectTask::Data(chunk) => {
                    writer.write(chunk).await?;
                }
            }
        }
    }
}
