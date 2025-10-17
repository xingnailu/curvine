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
        let action = OpenAction::try_from(flags)?;

        let (writer, reader) = match action {
            OpenAction::ReadOnly => {
                let reader = Self::create_reader(&fs, &path).await?;
                (None, Some(reader))
            }

            OpenAction::WriteOnly => {
                let writer = Self::create_writer(&fs, &path, flags).await?;
                (Some(writer), None)
            }

            OpenAction::ReadWrite => {
                // It is explicitly to create a file.
                let writer = if FuseUtils::has_truncate(flags) || FuseUtils::has_create(flags) {
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
            // File already exists:
            if overwrite {
                // O_TRUNC semantics: allow overwrite/truncate
                fs.create(path, true).await?
            } else {
                // No O_TRUNC: follow open(O_CREAT, ...) semantics -> open existing file, don't create again
                // Use append to get writer, then implement writing from any offset via seek in upper layer write
                fs.append(path).await?
            }
        } else {
            // File does not exist:
            // Only create when explicitly requested or O_TRUNC, otherwise should not reach here
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
            IoPhase::Writing | IoPhase::SealedToWriting => {}
        }
        let writer = match &mut self.writer {
            Some(v) => v,

            None => {
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

        // Debug log for random write tracking
        info!(
            "[FUSE_WRITE_DEBUG] path={} offset={} size={} current_pos={} phase={:?}",
            writer.path_str(),
            off,
            len,
            writer.pos(),
            self.phase
        );

        // Only skip true zero-length writes
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
        if let Err(e) = writer.seek(off as i64).await {
            info!(
                "[FUSE_SEEK_ERROR] path={} seek_to={} error={}",
                writer.path_str(),
                off,
                e
            );
            return Err(e.into());
        }

        info!(
            "[FUSE_SEEK_SUCCESS] path={} seek_to={} new_pos={}",
            writer.path_str(),
            off,
            writer.pos()
        );

        if let Err(e) = writer.write(op, reply).await {
            return Err(e.into());
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
