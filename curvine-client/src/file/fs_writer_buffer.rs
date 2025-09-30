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
use log::error;
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
        log::info!(
            "🎯 [FsWriterBuffer::seek] path={}, seek_to={}, current_pos={}",
            self.path.path(),
            pos,
            self.pos
        );
        
        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();
            
            match self.task_sender.send(WriterTask::Seek((pos, tx))).await {
                Err(e) => {
                    log::error!(
                        "❌ [FsWriterBuffer::seek] Failed to send seek task: path={}, error={}",
                        self.path.path(),
                        e
                    );
                    return Err(e.into());
                },
                Ok(_) => {
                    log::info!(
                        "📤 [FsWriterBuffer::seek] Seek task sent, waiting for response: path={}",
                        self.path.path()
                    );
                }
            }
            
            match rx.receive().await {
                Err(e) => {
                    log::error!(
                        "❌ [FsWriterBuffer::seek] Failed to receive seek response: path={}, error={}",
                        self.path.path(),
                        e
                    );
                    return Err(e.into());
                },
                Ok(_) => {
                    log::info!(
                        "📥 [FsWriterBuffer::seek] Seek response received: path={}",
                        self.path.path()
                    );
                }
            }
            Ok(())
        };

        // 更新位置（注意：实际seek由后台任务执行）
        if res.is_ok() {
            self.pos = pos;
            log::info!(
                "✅ [FsWriterBuffer::seek] Seek completed successfully: path={}, new_pos={}",
                self.path.path(),
                self.pos
            );
        }

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
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
                        log::info!(
                            "🎯 [FsWriterBuffer::write_future] Processing seek task: path={}, pos={}",
                            writer.path().path(),
                            pos
                        );
                        
                        // 先处理缓冲区中的所有数据
                        let mut buffered_chunks = 0;
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            buffered_chunks += 1;
                            log::info!(
                                "📦 [FsWriterBuffer::write_future] Processing buffered chunk #{}: path={}, len={}",
                                buffered_chunks,
                                writer.path().path(),
                                chunk.len()
                            );
                            
                            match writer.write(chunk).await {
                                Err(e) => {
                                    log::error!(
                                        "❌ [FsWriterBuffer::write_future] Failed to write buffered chunk: path={}, error={}",
                                        writer.path().path(),
                                        e
                                    );
                                    return Err(e);
                                },
                                Ok(_) => {
                                    log::info!(
                                        "✅ [FsWriterBuffer::write_future] Buffered chunk written successfully: path={}",
                                        writer.path().path()
                                    );
                                }
                            }
                        }
                        
                        if buffered_chunks > 0 {
                            log::info!(
                                "📦 [FsWriterBuffer::write_future] Processed {} buffered chunks before seek: path={}",
                                buffered_chunks,
                                writer.path().path()
                            );
                        }
                        
                        // 执行seek操作
                        match writer.seek(pos).await {
                            Err(e) => {
                                log::error!(
                                    "❌ [FsWriterBuffer::write_future] Seek failed: path={}, pos={}, error={}",
                                    writer.path().path(),
                                    pos,
                                    e
                                );
                                return Err(e);
                            },
                            Ok(_) => {
                                log::info!(
                                    "✅ [FsWriterBuffer::write_future] Seek succeeded: path={}, pos={}",
                                    writer.path().path(),
                                    pos
                                );
                            }
                        }
                        
                        match tx.send(1) {
                            Err(e) => {
                                log::error!(
                                    "❌ [FsWriterBuffer::write_future] Failed to send seek response: path={}, error={}",
                                    writer.path().path(),
                                    e
                                );
                                return Err(e.into());
                            },
                            Ok(_) => {
                                log::info!(
                                    "📤 [FsWriterBuffer::write_future] Seek response sent: path={}",
                                    writer.path().path()
                                );
                            }
                        }
                    }
                },

                SelectTask::Data(chunk) => {
                    writer.write(chunk).await?;
                }
            }
        }
    }
}
