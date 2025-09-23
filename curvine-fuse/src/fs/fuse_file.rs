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

use crate::fs::operator::{OpenAction, Read, Write};
use crate::fs::{FuseReader, FuseWriter};
use crate::session::FuseResponse;
use crate::{err_fuse, FuseResult, FuseUtils, FUSE_SUCCESS};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::FileStatus;
use log::info;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IoPhase {
    Unset,
    Reading,
    Writing,
    SealedToReading,
    SealedToWriting,
}

pub struct FuseFile {
    fs: UnifiedFileSystem,
    fh: u64,
    path: Path,
    flags: u32,
    writer: Option<FuseWriter>,
    reader: Option<FuseReader>,
    phase: IoPhase,
}

impl FuseFile {
    pub fn path_str(&self) -> &str {
        self.path.path()
    }
    
    pub fn flags(&self) -> u32 {
        self.flags
    }
    
    pub fn phase(&self) -> IoPhase {
        self.phase
    }
    
    pub async fn create(fs: UnifiedFileSystem, path: Path, flags: u32) -> FuseResult<Self> {
        info!(
            "🔧 [FuseFile::create] Creating FuseFile - path: {}, flags: {:#x}",
            path, flags
        );
        
        let action = OpenAction::try_from(flags)?;
        
        info!(
            "📋 [FuseFile::create] Action analysis - read: {}, write: {}, action: {:?}",
            action.read(), action.write(), action
        );
        let (writer, reader) = match action {
            OpenAction::ReadOnly => {
                info!("📖 [FuseFile::create] Creating reader for ReadOnly access");
                let reader = Self::create_reader(&fs, &path).await?;
                (None, Some(reader))
            }

            OpenAction::WriteOnly => {
                info!("✏️ [FuseFile::create] Creating writer for WriteOnly access");
                let writer = Self::create_writer(&fs, &path, flags).await?;
                (Some(writer), None)
            }

            OpenAction::ReadWrite => {
                
                // It is explicitly to create a file.
                let writer = if FuseUtils::has_truncate(flags) || FuseUtils::has_create(flags) {
                    info!("✏️ [FuseFile::create] Creating writer for ReadWrite with create/truncate");
                    let writer = Self::create_writer(&fs, &path, flags).await?;
                    Some(writer)
                } else {
                    None
                };
                (writer, None)
            }
        };

        let phase = if reader.is_some() {
            IoPhase::Reading
        } else if writer.is_some() {
            IoPhase::Writing
        } else {
            IoPhase::Unset
        };

        let file = Self {
            fs,
            fh: 0,
            path,
            flags,
            writer,
            reader,
            phase,
        };
        info!(
            "FuseFile::create path={} flags=0x{:x} action={:?} has_writer={} has_reader={}",
            file.path,
            file.flags,
            action,
            file.writer.is_some(),
            file.reader.is_some()
        );
        Ok(file)
    }

    pub async fn for_write(fs: UnifiedFileSystem, path: Path, flags: u32) -> FuseResult<Self> {
        let writer = Self::create_writer(&fs, &path, flags).await?;
        let file = Self {
            fs,
            fh: 0,
            path,
            flags,
            writer: Some(writer),
            reader: None,
            phase: IoPhase::Writing,
        };
        Ok(file)
    }

    async fn create_writer(
        fs: &UnifiedFileSystem,
        path: &Path,
        flags: u32,
    ) -> FuseResult<FuseWriter> {
        let overwrite = FuseUtils::has_truncate(flags);
        let _want_create = FuseUtils::has_create(flags);
        let exists = fs.exists(path).await.unwrap_or(false);

        let unified_writer = if exists {
            // 文件已存在：
            if overwrite {
                // O_TRUNC 语义：允许覆盖/截断
                fs.create(path, true).await?
            } else {
                // 无 O_TRUNC：遵循 open(O_CREAT, …) 语义 → 打开已有文件，不再创建
                // 使用 append 获取 writer，再由上层 write 中的 seek 实现从任意偏移写入
                fs.append(path).await?
            }
        } else {
            // 文件不存在：
            // 仅当显式要求创建或 O_TRUNC 时才创建，否则不应走到这里
            fs.create(path, overwrite).await?
        };
        Ok(FuseWriter::new(
            &fs.conf().fuse,
            fs.clone_runtime(),
            unified_writer,
        ))
    }

    async fn create_reader(fs: &UnifiedFileSystem, path: &Path) -> FuseResult<FuseReader> {
        let reader = fs.open(path).await?;
        Ok(FuseReader::new(&fs.conf().fuse, fs.clone_runtime(), reader))
    }

    pub async fn write(&mut self, op: Write<'_>, reply: FuseResponse) -> FuseResult<()> {
        let action = OpenAction::try_from(self.flags)?;

        // Phase transition: allow at most one switch; never concurrent
        match self.phase {
            IoPhase::Unset => {
                self.phase = IoPhase::Writing;
            }
            IoPhase::Reading => {
                if let Some(mut reader) = self.reader.take() {
                    reader.complete().await?;
                }
                self.phase = IoPhase::SealedToWriting;
            }
            IoPhase::SealedToReading => {
                return err_fuse!(
                    libc::EBUSY,
                    "Cannot switch from reading back to writing: {}",
                    self.path
                );
            }
            IoPhase::Writing | IoPhase::SealedToWriting => {
                info!("📝 [FuseFile::write] Already in writing phase: {:?}", self.phase);
            }
        }
        let writer = match &mut self.writer {
            Some(v) => {
                info!("✏️ [FuseFile::write] Using existing writer");
                v
            },

            None => {
                info!("🔧 [FuseFile::write] Creating new writer - action.write(): {}", action.write());
                // If opened with write capability (e.g., O_RDWR), allow creating writer
                if !action.write() {
                    return err_fuse!(libc::EBADF, "Write not permitted for fh on {}", self.path);
                }
                let writer = Self::create_writer(&self.fs, &self.path, self.flags).await?;
                self.writer.get_or_insert(writer)
            }
        };

        let off = op.arg.offset;
        let len = op.data.len() as u64;
        
        // 只跳过真正的零长度写入
        if len == 0 {
            return err_fuse!(
                FUSE_SUCCESS,
                "Skip zero-length write to file {} offset={} size={}",
                writer.path_str(),
                off,
                len
            );
        }

        // Support random writes: perform seek operation to specified offset
        log::info!(
            "🎯 [FuseFile::write] About to seek: path={}, offset={}, len={}",
            self.path.path(),
            off,
            len
        );
        
        match writer.seek(off as i64).await {
            Err(e) => {
                log::error!(
                    "❌ [FuseFile::write] Seek failed: path={}, offset={}, error={}",
                    self.path.path(),
                    off,
                    e
                );
                return Err(e.into());
            },
            Ok(_) => {
                log::info!(
                    "✅ [FuseFile::write] Seek succeeded: path={}, offset={}",
                    self.path.path(),
                    off
                );
            }
        }
        
        log::info!(
            "📝 [FuseFile::write] About to write: path={}, offset={}, len={}",
            self.path.path(),
            off,
            len
        );
        
        match writer.write(op, reply).await {
            Err(e) => {
                log::error!(
                    "❌ [FuseFile::write] Write failed: path={}, offset={}, len={}, error={}",
                    self.path.path(),
                    off,
                    len,
                    e
                );
                return Err(e.into());
            },
            Ok(_) => {
                log::info!(
                    "✅ [FuseFile::write] Write succeeded: path={}, offset={}, len={}",
                    self.path.path(),
                    off,
                    len
                );
            }
        }
        
        Ok(())
    }

    pub async fn read(&mut self, op: Read<'_>, rep: FuseResponse) -> FuseResult<()> {
        let action = OpenAction::try_from(self.flags)?;

        // Phase transition: allow at most one switch; never concurrent
        match self.phase {
            IoPhase::Unset => {
                self.phase = IoPhase::Reading;
            }
            IoPhase::Writing => {
                if let Some(mut writer) = self.writer.take() {
                    writer.flush().await?;
                    writer.complete().await?;
                }
                self.phase = IoPhase::SealedToReading;
            }
            IoPhase::SealedToWriting => {
                return err_fuse!(
                    libc::EBUSY,
                    "Cannot switch from writing back to reading: {}",
                    self.path
                );
            }
            IoPhase::Reading | IoPhase::SealedToReading => {}
        }
        let reader = match &mut self.reader {
            Some(v) => v,
            None => {
                // If opened with read capability (e.g., O_RDWR), allow creating reader
                if !action.read() {
                    return err_fuse!(libc::EBADF, "Read not permitted for fh on {}", self.path);
                }
                let reader = Self::create_reader(&self.fs, &self.path).await?;
                self.reader.get_or_insert(reader)
            }
        };
        // Allow random read: just forward to reader
        reader.read(op, rep).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> FuseResult<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> FuseResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.complete().await?;
        }

        if let Some(mut reader) = self.reader.take() {
            reader.complete().await?;
        }
        Ok(())
    }

    pub async fn close(&mut self) -> FuseResult<()> {
        self.complete().await
    }

    pub fn status(&self) -> FuseResult<&FileStatus> {
        if let Some(writer) = &self.writer {
            Ok(writer.status())
        } else {
            err_fuse!(libc::EIO, "{}", "Not found file status")
        }
    }

    pub fn set_fh(&mut self, fh: u64) {
        self.fh = fh;
    }
}
