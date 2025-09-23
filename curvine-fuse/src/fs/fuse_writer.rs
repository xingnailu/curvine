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

use crate::fs::operator::Write;
use crate::raw::fuse_abi::fuse_write_out;
use crate::session::FuseResponse;
use curvine_client::unified::UnifiedWriter;
use curvine_common::conf::FuseConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use log::error;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
use std::sync::Arc;

enum WriteTask {
    Write(DataSlice, FuseResponse),
    Flush(CallSender<i8>),
    Complete(CallSender<i8>),
    Seek((i64, CallSender<i8>)), // 🔑 新增：随机写 seek 支持
}

pub struct FuseWriter {
    sender: AsyncSender<WriteTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    path: Path,
    status: FileStatus,
    pos: i64,
}

impl FuseWriter {
    pub fn new(conf: &FuseConf, rt: Arc<Runtime>, writer: UnifiedWriter) -> Self {
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(conf.stream_channel_size).split();

        let path = writer.path().clone();
        let status = writer.status().clone();
        let monitor = err_monitor.clone();

        rt.spawn(async move {
            let res = Self::writer_future(writer, receiver).await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!("fuse writer error: {e}");
                    monitor.set_error(e);
                }
            }
        });

        Self {
            sender,
            err_monitor,
            path,
            status,
            pos: 0,
        }
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn path_str(&self) -> &str {
        self.path.path()
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    fn check_error(&self, e: FsError) -> FsError {
        match self.err_monitor.take_error() {
            Some(e) => e,
            None => e,
        }
    }

    pub async fn write(&mut self, op: Write<'_>, reply: FuseResponse) -> FsResult<()> {
        log::info!(
            "🔥 [FuseWriter::write] path={}, offset={}, size={}, current_pos={}",
            self.path.path(),
            op.arg.offset,
            op.arg.size,
            self.pos
        );
        
        let task = WriteTask::Write(DataSlice::Bytes(op.data), reply);

        match self.sender.send(task).await {
            Err(e) => {
                log::error!(
                    "❌ [FuseWriter::write] Failed to send write task: path={}, error={}",
                    self.path.path(),
                    e
                );
                Err(self.check_error(e.into()))
            },
            Ok(_) => {
                self.pos += op.arg.size as i64;
                log::info!(
                    "✅ [FuseWriter::write] Write task sent successfully: path={}, new_pos={}",
                    self.path.path(),
                    self.pos
                );
                Ok(())
            }
        }
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(WriteTask::Complete(rx)).await?;
            tx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(WriteTask::Flush(rx)).await?;
            tx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    // 🔑 新增：随机写 seek 支持
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        log::info!(
            "🎯 [FuseWriter::seek] path={}, seek_to={}, current_pos={}",
            self.path.path(),
            pos,
            self.pos
        );
        
        let res: FsResult<()> = {
            let (rx, tx) = CallChannel::channel();
            match self.sender.send(WriteTask::Seek((pos, rx))).await {
                Err(e) => {
                    log::error!(
                        "❌ [FuseWriter::seek] Failed to send seek task: path={}, error={}",
                        self.path.path(),
                        e
                    );
                    return Err(e.into());
                },
                Ok(_) => {
                    log::info!(
                        "📤 [FuseWriter::seek] Seek task sent, waiting for response: path={}",
                        self.path.path()
                    );
                }
            }
            
            match tx.receive().await {
                Err(e) => {
                    log::error!(
                        "❌ [FuseWriter::seek] Failed to receive seek response: path={}, error={}",
                        self.path.path(),
                        e
                    );
                    return Err(e.into());
                },
                Ok(_) => {
                    log::info!(
                        "📥 [FuseWriter::seek] Seek response received: path={}",
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
                "✅ [FuseWriter::seek] Seek completed successfully: path={}, new_pos={}",
                self.path.path(),
                self.pos
            );
        }

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    async fn writer_future(
        mut writer: UnifiedWriter,
        mut req_receiver: AsyncReceiver<WriteTask>,
    ) -> FsResult<()> {
        log::info!(
            "🚀 [FuseWriter::writer_future] Started writer future for path={}",
            writer.path().path()
        );
        
        while let Some(task) = req_receiver.recv().await {
            match task {
                WriteTask::Write(data, reply) => {
                    let len = data.len();
                    log::info!(
                        "📝 [FuseWriter::writer_future] Processing write task: path={}, len={}",
                        writer.path().path(),
                        len
                    );
                    
                    let res = match writer.fuse_write(data).await {
                        Err(e) => {
                            log::error!(
                                "❌ [FuseWriter::writer_future] Write failed: path={}, error={}",
                                writer.path().path(),
                                e
                            );
                            Err(e.into())
                        },
                        Ok(()) => {
                            log::info!(
                                "✅ [FuseWriter::writer_future] Write succeeded: path={}, len={}",
                                writer.path().path(),
                                len
                            );
                            let rep = fuse_write_out {
                                size: len as u32,
                                padding: 0,
                            };
                            Ok(rep)
                        }
                    };

                    if let Err(e) = reply.send_rep(res).await {
                        log::error!(
                            "❌ [FuseWriter::writer_future] Failed to send reply: path={}, error={}",
                            writer.path().path(),
                            e
                        );
                        return Err(e.into());
                    }
                }

                WriteTask::Complete(tx) => {
                    log::info!(
                        "🏁 [FuseWriter::writer_future] Processing complete task: path={}",
                        writer.path().path()
                    );
                    
                    match writer.complete().await {
                        Err(e) => {
                            log::error!(
                                "❌ [FuseWriter::writer_future] Complete failed: path={}, error={}",
                                writer.path().path(),
                                e
                            );
                            return Err(e);
                        },
                        Ok(_) => {
                            log::info!(
                                "✅ [FuseWriter::writer_future] Complete succeeded: path={}",
                                writer.path().path()
                            );
                        }
                    }
                    tx.send(1)?;
                }

                WriteTask::Flush(tx) => {
                    log::info!(
                        "💾 [FuseWriter::writer_future] Processing flush task: path={}",
                        writer.path().path()
                    );
                    
                    match writer.flush().await {
                        Err(e) => {
                            log::error!(
                                "❌ [FuseWriter::writer_future] Flush failed: path={}, error={}",
                                writer.path().path(),
                                e
                            );
                            return Err(e);
                        },
                        Ok(_) => {
                            log::info!(
                                "✅ [FuseWriter::writer_future] Flush succeeded: path={}",
                                writer.path().path()
                            );
                        }
                    }
                    tx.send(1)?;
                }

                // 🔑 处理随机写 seek 任务
                WriteTask::Seek((pos, tx)) => {
                    log::info!(
                        "🎯 [FuseWriter::writer_future] Processing seek task: path={}, pos={}",
                        writer.path().path(),
                        pos
                    );
                    
                    match writer.seek(pos).await {
                        Err(e) => {
                            log::error!(
                                "❌ [FuseWriter::writer_future] Seek failed: path={}, pos={}, error={}",
                                writer.path().path(),
                                pos,
                                e
                            );
                            return Err(e);
                        },
                        Ok(_) => {
                            log::info!(
                                "✅ [FuseWriter::writer_future] Seek succeeded: path={}, pos={}",
                                writer.path().path(),
                                pos
                            );
                        }
                    }
                    tx.send(1)?;
                }
            }
        }
        
        log::info!(
            "🔚 [FuseWriter::writer_future] Writer future ended for path={}",
            writer.path().path()
        );
        Ok(())
    }
}
