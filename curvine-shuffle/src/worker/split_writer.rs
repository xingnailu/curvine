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

use crate::protocol::{SpiltKey, SplitInfo};
use curvine_client::file::FsWriterBase;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::io::net::InetAddr;
use orpc::sys::DataSlice;

pub struct SplitWriter {
    pub split_key: SpiltKey,
    pub split_size: i64,
    pub inner: Option<FsWriterBase>,
    is_complete: bool,
}

impl SplitWriter {
    pub fn new(split_key: SpiltKey, split_size: i64) -> Self {
        Self {
            split_key,
            split_size,
            inner: None,
            is_complete: false,
        }
    }

    pub fn is_init(&self) -> bool {
        self.inner.is_some()
    }

    pub fn set_writer(&mut self, writer: FsWriterBase) {
        let _ = self.inner.insert(writer);
        self.is_complete = false;
    }

    fn get_writer(&mut self) -> FsResult<&mut FsWriterBase> {
        match &mut self.inner {
            None => err_box!("Writer not init"),
            Some(v) => Ok(v),
        }
    }

    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<bool> {
        let writer = self.get_writer()?;
        writer.write(chunk).await?;
        Ok(writer.pos() >= self.split_size)
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        self.get_writer()?.flush().await
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        if !self.is_complete {
            self.get_writer()?.complete().await?;
            self.is_complete = true;
        }
        Ok(())
    }

    pub fn pos(&self) -> i64 {
        self.inner.as_ref().map(|v| v.pos()).unwrap_or(0)
    }

    pub fn path(&self) -> &str {
        self.inner.as_ref().map(|v| v.path_str()).unwrap_or("")
    }

    pub fn to_split_info(&self) -> SplitInfo {
        SplitInfo {
            part_id: self.split_key.part_id,
            split_id: self.split_key.split_id,
            write_len: self.pos(),
            worker_addr: InetAddr::default(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.is_complete
    }
}
