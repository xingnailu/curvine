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

use crate::block::BlockWriter;
use crate::file::{FsClient, FsContext};
use curvine_common::fs::Path;
use curvine_common::state::{FileStatus, FileBlocks, LocatedBlock, SearchFileBlocks};
use curvine_common::FsResult;
use orpc::common::FastHashMap;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::try_option_mut;
use std::mem;
use std::sync::Arc;

pub struct FsWriterBase {
    fs_context: Arc<FsContext>,
    fs_client: FsClient,
    path: Path,
    pos: i64,
    status: FileStatus,
    
    // File block information for random write support
    file_blocks: Option<SearchFileBlocks>,
    
    // Block management
    last_block: Option<LocatedBlock>,
    cur_writer: Option<BlockWriter>,
    
    // Pending commit block from seek operations
    pending_commit_block: Option<curvine_common::state::CommitBlock>,
    
    // Writer cache similar to reader implementation
    close_writer_times: u32,
    close_writer_limit: u32,
    all_writers: FastHashMap<i64, BlockWriter>,
    
    // 🔧 跟踪文件的最大写入位置（用于正确计算文件长度）
    max_written_pos: i64,
    
    // 🔧 跟踪每个块的实际写入范围 (block_id -> (start_pos, end_pos))
    block_write_ranges: FastHashMap<i64, (i64, i64)>,
}

impl FsWriterBase {
    pub fn new(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let close_writer_limit = fs_context.conf.client.close_reader_limit; // 复用读取器的限制配置

        let initial_len = status.len;
        Self {
            fs_context,
            fs_client,
            pos: initial_len,
            path,
            status,
            file_blocks: None, // New file, no block information
            last_block,
            cur_writer: None,
            pending_commit_block: None,
            close_writer_times: 0,
            close_writer_limit,
            all_writers: FastHashMap::default(),
            max_written_pos: initial_len, // 初始化为文件当前长度
            block_write_ranges: FastHashMap::default(),
        }
    }

    // Constructor with random write support, accepts file block information
    pub fn with_blocks(
        fs_context: Arc<FsContext>,
        path: Path,
        status: FileStatus,
        file_blocks: FileBlocks,
        last_block: Option<LocatedBlock>,
    ) -> Self {
        let fs_client = FsClient::new(fs_context.clone());
        let close_writer_limit = fs_context.conf.client.close_reader_limit;

        let initial_len = status.len;
        Self {
            fs_context,
            fs_client,
            pos: initial_len,
            path,
            status,
            file_blocks: Some(SearchFileBlocks::new(file_blocks)), // Support random writes
            last_block,
            cur_writer: None,
            pending_commit_block: None,
            close_writer_times: 0,
            close_writer_limit,
            all_writers: FastHashMap::default(),
            max_written_pos: initial_len, // 初始化为文件当前长度
            block_write_ranges: FastHashMap::default(),
        }
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub fn path_str(&self) -> &str {
        self.path.path()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn fs_context(&self) -> &FsContext {
        &self.fs_context
    }

    // 🔧 维护file_blocks结构的方法
    fn add_block_to_file_blocks(&mut self, block_id: i64, block_len: i64, locs: Vec<curvine_common::state::WorkerAddress>) {
        let new_located_block = LocatedBlock {
            block: curvine_common::state::ExtendedBlock {
                id: block_id,
                len: block_len,
                storage_type: curvine_common::state::StorageType::Disk,
                file_type: curvine_common::state::FileType::File,
            },
            locs,
        };

        if let Some(ref mut file_blocks) = self.file_blocks {
            // 已有file_blocks，添加新块
            log::info!(
                "📋 [FsWriterBase::add_block_to_file_blocks] Adding block to existing file_blocks: path={}, block_id={}, block_len={}",
                self.path.path(),
                block_id,
                block_len
            );
            
            // 需要重新创建SearchFileBlocks以更新索引
            let mut file_blocks_data = FileBlocks {
                status: file_blocks.status.clone(),
                block_locs: file_blocks.block_locs.clone(),
            };
            file_blocks_data.block_locs.push(new_located_block);
            
            // 更新file_blocks
            *file_blocks = SearchFileBlocks::new(file_blocks_data);
        } else {
            // 创建新的file_blocks
            log::info!(
                "🆕 [FsWriterBase::add_block_to_file_blocks] Creating new file_blocks: path={}, block_id={}, block_len={}",
                self.path.path(),
                block_id,
                block_len
            );
            
            let file_blocks_data = FileBlocks {
                status: self.status.clone(),
                block_locs: vec![new_located_block],
            };
            
            self.file_blocks = Some(SearchFileBlocks::new(file_blocks_data));
        }
    }

    // 🔧 计算文件的正确长度（支持随机写和文件空洞）
    fn calculate_file_length(&self) -> i64 {
        if self.file_blocks.is_some() {
            // 已存在文件：基于file_blocks计算长度
            self.calculate_length_from_blocks()
        } else {
            // 新文件：基于写入位置计算长度
            self.calculate_length_from_position()
        }
    }

    // 基于file_blocks计算文件长度
    fn calculate_length_from_blocks(&self) -> i64 {
        if let Some(file_blocks) = &self.file_blocks {
            let block_size = self.fs_context.block_size();
            let mut max_written_pos = 0i64;
            
            // 🔧 遍历所有块，使用实际的写入范围计算最大位置
            for (i, block) in file_blocks.block_locs.iter().enumerate() {
                let block_start = i as i64 * block_size;
                let block_id = block.block.id;
                
                // 检查这个块的实际写入范围
                if let Some((write_start, write_end)) = self.block_write_ranges.get(&block_id) {
                    // 使用实际写入的结束位置
                    max_written_pos = max_written_pos.max(*write_end);
                    
                    log::info!(
                        "📦 [FsWriterBase::calculate_length_from_blocks] Block {}: write_range=({}, {}), contributing_end={}",
                        block_id,
                        write_start,
                        write_end,
                        write_end
                    );
                } else if block.block.len > 0 {
                    // 如果没有写入范围记录，但块有长度，使用原有逻辑（已存在的数据）
                    let block_end = block_start + block.block.len;
                    max_written_pos = max_written_pos.max(block_end);
                    
                    log::info!(
                        "📦 [FsWriterBase::calculate_length_from_blocks] Block {} (existing): block_range=({}, {}), contributing_end={}",
                        block_id,
                        block_start,
                        block_end,
                        block_end
                    );
                }
            }
            
            // 考虑max_written_pos（可能包含新分配但未在file_blocks中的块）
            let final_length = max_written_pos.max(self.max_written_pos);
            
            log::info!(
                "📊 [FsWriterBase::calculate_length_from_blocks] path={}, blocks_max={}, max_written_pos={}, final_length={}",
                self.path.path(),
                max_written_pos,
                self.max_written_pos,
                final_length
            );
            
            final_length
        } else {
            // 如果没有file_blocks，回退到基于位置的计算
            self.calculate_length_from_position()
        }
    }

    // 基于写入位置计算文件长度（新文件场景）
    fn calculate_length_from_position(&self) -> i64 {
        // 🔧 使用max_written_pos来确保包含所有写入的数据（包括随机写产生的空洞）
        let length = self.max_written_pos;
        
        log::info!(
            "📊 [FsWriterBase::calculate_length_from_position] path={}, max_written_pos={}, current_pos={}",
            self.path.path(),
            length,
            self.pos
        );
        
        length
    }

    // 🔧 找到并完成真正的最后一个块
    async fn find_and_complete_last_block(&mut self) -> FsResult<Option<curvine_common::state::CommitBlock>> {
        log::info!(
            "🔍 [FsWriterBase::find_and_complete_last_block] Finding last block: path={}, has_file_blocks={}, has_cur_writer={}, cached_writers={}",
            self.path.path(),
            self.file_blocks.is_some(),
            self.cur_writer.is_some(),
            self.all_writers.len()
        );

        // 收集所有可能的块写入器及其位置信息
        let mut block_writers = Vec::new();
        
        // 1. 添加当前写入器
        if let Some(writer) = self.cur_writer.take() {
            let block_id = writer.block_id();
            let block_pos = self.get_block_position(block_id);
            block_writers.push((block_pos, writer));
            
            log::info!(
                "📦 [FsWriterBase::find_and_complete_last_block] Added cur_writer: path={}, block_id={}, position={}",
                self.path.path(),
                block_id,
                block_pos
            );
        }
        
        // 2. 添加缓存的写入器
        let cached_writers: Vec<_> = self.all_writers.drain().collect();
        for (block_id, writer) in cached_writers {
            let block_pos = self.get_block_position(block_id);
            block_writers.push((block_pos, writer));
            
            log::info!(
                "📦 [FsWriterBase::find_and_complete_last_block] Added cached writer: path={}, block_id={}, position={}",
                self.path.path(),
                block_id,
                block_pos
            );
        }
        
        if block_writers.is_empty() {
            log::info!(
                "📭 [FsWriterBase::find_and_complete_last_block] No writers found: path={}",
                self.path.path()
            );
            return Ok(None);
        }
        
        // 3. 按位置排序，找到最后一个块
        block_writers.sort_by_key(|(pos, _)| *pos);
        
        let mut last_commit_block = None;
        
        // 4. 完成所有写入器，保留最后一个作为CommitBlock
        let total_writers = block_writers.len();
        for (i, (block_pos, mut writer)) in block_writers.into_iter().enumerate() {
            let block_id = writer.block_id();
            let is_last = i == total_writers - 1;
            
            log::info!(
                "🔒 [FsWriterBase::find_and_complete_last_block] Completing writer: path={}, block_id={}, position={}, is_last={} ({}/{})",
                self.path.path(),
                block_id,
                block_pos,
                is_last,
                i + 1,
                total_writers
            );
            
            writer.complete().await?;
            
            if is_last {
                last_commit_block = Some(writer.to_commit_block());
                log::info!(
                    "🎯 [FsWriterBase::find_and_complete_last_block] Selected as last block: path={}, block_id={}, position={}",
                    self.path.path(),
                    block_id,
                    block_pos
                );
            }
        }
        
        Ok(last_commit_block)
    }
    
    // 🔧 获取块在文件中的位置（用于排序）
    fn get_block_position(&self, block_id: i64) -> i64 {
        if let Some(file_blocks) = &self.file_blocks {
            let block_size = self.fs_context.block_size();
            
            // 在file_blocks中查找块的索引
            for (i, located_block) in file_blocks.block_locs.iter().enumerate() {
                if located_block.block.id == block_id {
                    let position = i as i64 * block_size;
                    log::info!(
                        "📍 [FsWriterBase::get_block_position] Found block in file_blocks: path={}, block_id={}, index={}, position={}",
                        self.path.path(),
                        block_id,
                        i,
                        position
                    );
                    return position;
                }
            }
        }
        
        // 如果在file_blocks中找不到，使用block_write_ranges
        if let Some((start_pos, _)) = self.block_write_ranges.get(&block_id) {
            log::info!(
                "📍 [FsWriterBase::get_block_position] Found block in write_ranges: path={}, block_id={}, position={}",
                self.path.path(),
                block_id,
                start_pos
            );
            return *start_pos;
        }
        
        // 默认返回0（新分配的第一个块）
        log::warn!(
            "⚠️ [FsWriterBase::get_block_position] Block not found, using default position: path={}, block_id={}",
            self.path.path(),
            block_id
        );
        0
    }

    pub async fn write(&mut self, mut chunk: DataSlice) -> FsResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        log::info!(
            "📝 [FsWriterBase::write] Starting write: path={}, pos={}, chunk_len={}, total_remaining={}",
            self.path.path(),
            self.pos,
            chunk.len(),
            chunk.len()
        );

        let mut remaining = chunk.len();
        while remaining > 0 {
            let path = self.path.path().to_string();
            let pos = self.pos;
            
            log::info!(
                "🔄 [FsWriterBase::write] Write loop: path={}, pos={}, remaining={}",
                path,
                pos,
                remaining
            );
            
            let cur_writer = self.get_writer().await?;
            let write_len = remaining.min(cur_writer.remaining() as usize);
            
            log::info!(
                "✍️ [FsWriterBase::write] Writing chunk: path={}, pos={}, write_len={}, block_remaining={}",
                path,
                pos,
                write_len,
                cur_writer.remaining()
            );
            
            // Write data request.
            cur_writer.write(chunk.split_to(write_len)).await?;

            remaining -= write_len;
            self.pos += write_len as i64;
            
            // 🔧 更新最大写入位置
            self.max_written_pos = self.max_written_pos.max(self.pos);
            
            // 🔧 记录当前块的写入范围
            if let Some(writer) = &self.cur_writer {
                let block_id = writer.block_id();
                let write_start = self.pos - write_len as i64;
                let write_end = self.pos;
                
                // 更新或插入块的写入范围
                self.block_write_ranges
                    .entry(block_id)
                    .and_modify(|(start, end)| {
                        *start = (*start).min(write_start);
                        *end = (*end).max(write_end);
                    })
                    .or_insert((write_start, write_end));
                
                log::info!(
                    "📝 [FsWriterBase::write] Updated block write range: path={}, block_id={}, range=({}, {})",
                    self.path.path(),
                    block_id,
                    self.block_write_ranges[&block_id].0,
                    self.block_write_ranges[&block_id].1
                );
            }
            
            log::info!(
                "✅ [FsWriterBase::write] Chunk written: path={}, new_pos={}, remaining={}, max_written_pos={}",
                self.path.path(),
                self.pos,
                remaining,
                self.max_written_pos
            );
        }

        log::info!(
            "🎯 [FsWriterBase::write] Write completed: path={}, final_pos={}",
            self.path.path(),
            self.pos
        );

        Ok(())
    }

    /// Block write.
    /// Explain why there is a separate blocking_write instead of rt.block_on(self.write)
    /// We hope to reduce thread switching for writing local files, and the logic of network writing and rt.block_on(self.write) is consistent.
    /// Local write will directly write to the file, without any thread switching.
    pub fn blocking_write(&mut self, rt: &Runtime, mut chunk: DataSlice) -> FsResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        let mut remaining = chunk.len();
        while remaining > 0 {
            let cur_writer = rt.block_on(self.get_writer())?;
            let write_len = remaining.min(cur_writer.remaining() as usize);

            // Write data request.
            cur_writer.blocking_write(rt, chunk.split_to(write_len))?;

            remaining -= write_len;
            self.pos += write_len as i64;
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        // Just flush the block writer in the current write state.
        match &mut self.cur_writer {
            None => Ok(()),
            Some(writer) => writer.flush().await,
        }
    }

    // Write is completed, perform the following operations
    // 1. Submit the last block.
    // 2. Clean up all cached writers.
    pub async fn complete(&mut self) -> FsResult<()> {
        // 🔧 找到真正的最后一个块（按文件偏移量排序）
        let last_block = self.find_and_complete_last_block().await?;

        // Clean up all cached writers
        for (_, mut writer) in self.all_writers.drain() {
            writer.complete().await?;
        }

        // 🔧 计算正确的文件长度
        let file_length = self.calculate_file_length();
        
        log::info!(
            "📏 [FsWriterBase::complete] Completing file: path={}, calculated_length={}, last_pos={}, last_block_id={}",
            self.path.path(),
            file_length,
            self.pos,
            last_block.as_ref().map(|b| b.block_id).unwrap_or(-1)
        );

        self.fs_client
            .complete_file(&self.path, file_length, last_block)
            .await?;
        Ok(())
    }
    
    // Implement seek support for random writes
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(format!("Cannot seek to negative position: {}", pos).into());
        }
        
        log::info!(
            "🎯 [FsWriterBase::seek] Seeking: path={}, from_pos={}, to_pos={}",
            self.path.path(),
            self.pos,
            pos
        );
        
        // Check if we have a current writer
        if let Some(writer) = &mut self.cur_writer {
            let block_start = self.pos - writer.pos();
            let block_end = block_start + writer.len();
            
            log::info!(
                "📦 [FsWriterBase::seek] Current block range: path={}, block_start={}, block_end={}, has_remaining={}",
                self.path.path(),
                block_start,
                block_end,
                writer.has_remaining()
            );
            
            // If seek position is within current block range, seek directly
            if pos >= block_start && pos < block_end {
                let block_offset = pos - block_start;
                log::info!(
                    "✅ [FsWriterBase::seek] Seeking within current block: path={}, block_offset={}",
                    self.path.path(),
                    block_offset
                );
                writer.seek(block_offset).await?;
                self.pos = pos;
                return Ok(());
            }
            
            // If seek position is outside current block, we need to handle the current writer properly
            let block_id = writer.block_id();
            let written_data = writer.pos() > 0; // Check if any data has been written
            
            if !writer.has_remaining() {
                // Block is full, commit it immediately
                log::info!(
                    "🔒 [FsWriterBase::seek] Current block is full, committing before seek: path={}, block_id={}",
                    self.path.path(),
                    block_id
                );
                
                let mut old_writer = self.cur_writer.take().unwrap();
                old_writer.complete().await?;
                let commit_block = old_writer.to_commit_block();
                
                log::info!(
                    "💾 [FsWriterBase::seek] Block committed and saved for next allocation: path={}, block_id={}, block_len={}",
                    self.path.path(),
                    commit_block.block_id,
                    commit_block.block_len
                );
                
                // Save the commit block for the next allocate_new_block call
                self.pending_commit_block = Some(commit_block);
            } else if written_data {
                // Block has data but is not full, cache it for potential reuse
                log::info!(
                    "💾 [FsWriterBase::seek] Current block has data but not full, caching for reuse: path={}, block_id={}, pos={}, remaining={}",
                    self.path.path(),
                    block_id,
                    writer.pos(),
                    writer.remaining()
                );
                
                let old_writer = self.cur_writer.take().unwrap();
                self.all_writers.insert(block_id, old_writer);
            } else {
                // Block has no data, can safely discard
                log::info!(
                    "🔄 [FsWriterBase::seek] Current block has no data, discarding: path={}, block_id={}",
                    self.path.path(),
                    block_id
                );
            }
        }
        
        // Clear current writer and update position
        self.cur_writer = None;
        self.pos = pos;
        
        log::info!(
            "✅ [FsWriterBase::seek] Seek completed: path={}, new_pos={}",
            self.path.path(),
            self.pos
        );
        
        Ok(())
    }

    async fn update_writer(&mut self, cur: Option<BlockWriter>) -> FsResult<()> {
        let _new_block_id = cur.as_ref().map(|w| w.block_id());
        
        if let Some(mut old) = mem::replace(&mut self.cur_writer, cur) {
            let block_id = old.block_id();
            let has_data = old.pos() > 0;
            let is_full = !old.has_remaining();
            
            if is_full {
                // Block is full, must complete it
                log::info!(
                    "🔒 [FsWriterBase::update_writer] Completing full block: path={}, block_id={}",
                    self.path.path(),
                    block_id
                );
                old.complete().await?;
                self.close_writer_times += 1;
            } else if has_data && self.close_writer_times <= self.close_writer_limit {
                // Block has data but not full, cache for reuse
                log::info!(
                    "💾 [FsWriterBase::update_writer] Caching partial block: path={}, block_id={}, pos={}",
                    self.path.path(),
                    block_id,
                    old.pos()
                );
                self.all_writers.insert(block_id, old);
            } else {
                // Either no data or cache limit exceeded, complete and close
                log::info!(
                    "🔒 [FsWriterBase::update_writer] Completing block (no data or cache limit): path={}, block_id={}, has_data={}, cache_times={}",
                    self.path.path(),
                    block_id,
                    has_data,
                    self.close_writer_times
                );
                old.complete().await?;
                self.close_writer_times += 1;
            }
        }
        
        Ok(())
    }

    async fn get_writer(&mut self) -> FsResult<&mut BlockWriter> {
        log::info!(
            "🔍 [FsWriterBase::get_writer] Getting writer: path={}, pos={}, has_current_writer={}",
            self.path.path(),
            self.pos,
            self.cur_writer.is_some()
        );
        
        match &self.cur_writer {
            Some(v) if v.has_remaining() => {
                log::info!(
                    "♻️ [FsWriterBase::get_writer] Reusing current writer: path={}, remaining={}",
                    self.path.path(),
                    v.remaining()
                );
            },
            
            _ => {
                if let Some(v) = &self.cur_writer {
                    log::info!(
                        "🔄 [FsWriterBase::get_writer] Current writer exhausted: path={}, remaining={}",
                        self.path.path(),
                        v.remaining()
                    );
                } else {
                    log::info!(
                        "🆕 [FsWriterBase::get_writer] No current writer: path={}",
                        self.path.path()
                    );
                }
                
                let (_block_off, lb) = if let Some(file_blocks) = &self.file_blocks {
                    log::info!(
                        "🗂️ [FsWriterBase::get_writer] Checking existing file blocks: path={}, pos={}",
                        self.path.path(),
                        self.pos
                    );
                    
                    // Try to find in existing file blocks
                    match file_blocks.get_write_block(self.pos) {
                        Ok((block_off, located_block)) => {
                            log::info!(
                                "✅ [FsWriterBase::get_writer] Found existing block: path={}, block_id={}, block_off={}",
                                self.path.path(),
                                located_block.block.id,
                                block_off
                            );
                            
                            // Found existing block, check if there's a cached writer
                            let new_writer = match self.all_writers.remove(&located_block.block.id) {
                                Some(mut cached_writer) => {
                                    log::info!(
                                        "♻️ [FsWriterBase::get_writer] Using cached writer: path={}, block_id={}",
                                        self.path.path(),
                                        located_block.block.id
                                    );
                                    // Use cached block writer, but need to seek to correct position
                                    cached_writer.seek(block_off).await?;
                                    cached_writer
                                }
                                None => {
                                    log::info!(
                                        "🔧 [FsWriterBase::get_writer] Creating new writer for existing block: path={}, block_id={}",
                                        self.path.path(),
                                        located_block.block.id
                                    );
                                    // Create new block writer, directly pass block_off
                                    BlockWriter::with_offset(
                                        self.fs_context.clone(),
                                        located_block.clone(),
                                        Some(block_off),
                                    )
                                    .await?
                                }
                            };
                            
                            (block_off, new_writer)
                        }
                        Err(_) => {
                            log::info!(
                                "📈 [FsWriterBase::get_writer] Position exceeds file range, allocating new block: path={}, pos={}",
                                self.path.path(),
                                self.pos
                            );
                            // Position exceeds file range, need to allocate new block
                            self.allocate_new_block().await?
                        }
                    }
                } else {
                    log::info!(
                        "📄 [FsWriterBase::get_writer] New file, checking if we can reuse existing blocks: path={}, pos={}",
                        self.path.path(),
                        self.pos
                    );
                    
                    // 🔧 即使是新文件，也可能已经分配了一些块，检查file_blocks
                    if let Some(file_blocks) = &self.file_blocks {
                        log::info!(
                            "🔍 [FsWriterBase::get_writer] Found file_blocks for new file, checking position: path={}, pos={}",
                            self.path.path(),
                            self.pos
                        );
                        
                        match file_blocks.get_write_block(self.pos) {
                            Ok((block_off, located_block)) => {
                                log::info!(
                                    "✅ [FsWriterBase::get_writer] Found existing block in new file: path={}, block_id={}, block_off={}",
                                    self.path.path(),
                                    located_block.block.id,
                                    block_off
                                );
                                
                                // 检查缓存的writer
                                let new_writer = match self.all_writers.remove(&located_block.block.id) {
                                    Some(mut cached_writer) => {
                                        log::info!(
                                            "♻️ [FsWriterBase::get_writer] Reusing cached writer for new file: path={}, block_id={}",
                                            self.path.path(),
                                            located_block.block.id
                                        );
                                        cached_writer.seek(block_off).await?;
                                        cached_writer
                                    }
                                    None => {
                                        log::info!(
                                            "🔧 [FsWriterBase::get_writer] Creating new writer for existing block in new file: path={}, block_id={}",
                                            self.path.path(),
                                            located_block.block.id
                                        );
                                        BlockWriter::with_offset(
                                            self.fs_context.clone(),
                                            located_block.clone(),
                                            Some(block_off),
                                        )
                                        .await?
                                    }
                                };
                                
                                (block_off, new_writer)
                            }
                            Err(_) => {
                                log::info!(
                                    "📈 [FsWriterBase::get_writer] Position exceeds existing blocks in new file, allocating: path={}, pos={}",
                                    self.path.path(),
                                    self.pos
                                );
                                self.allocate_new_block().await?
                            }
                        }
                    } else {
                        log::info!(
                            "🆕 [FsWriterBase::get_writer] No file_blocks yet, allocating first block: path={}, pos={}",
                            self.path.path(),
                            self.pos
                        );
                        // 真正的新文件，分配第一个块
                        self.allocate_new_block().await?
                    }
                };
                
                log::info!(
                    "🔄 [FsWriterBase::get_writer] Updating current writer: path={}",
                    self.path.path()
                );
                // Update current writer
                self.update_writer(Some(lb)).await?;
            }
        }

        let writer = try_option_mut!(self.cur_writer);
        log::info!(
            "✅ [FsWriterBase::get_writer] Writer ready: path={}, block_id={}, remaining={}",
            self.path.path(),
            writer.block_id(),
            writer.remaining()
        );
        
        Ok(writer)
    }

    async fn allocate_new_block(&mut self) -> FsResult<(i64, BlockWriter)> {
        log::info!(
            "🆕 [FsWriterBase::allocate_new_block] Starting new block allocation for path={}, pos={}",
            self.path.path(),
            self.pos
        );
        
        let commit_block = if let Some(mut writer) = self.cur_writer.take() {
            log::info!(
                "🔒 [FsWriterBase::allocate_new_block] Completing previous block for path={}",
                self.path.path()
            );
            
            writer.complete().await?;
            let commit_block = writer.to_commit_block();
            
            log::info!(
                "✅ [FsWriterBase::allocate_new_block] Previous block completed: path={}, block_id={}, block_len={}",
                self.path.path(),
                commit_block.block_id,
                commit_block.block_len
            );
            
            Some(commit_block)
        } else if let Some(pending_commit) = self.pending_commit_block.take() {
            log::info!(
                "💾 [FsWriterBase::allocate_new_block] Using pending commit block from seek: path={}, block_id={}, block_len={}",
                self.path.path(),
                pending_commit.block_id,
                pending_commit.block_len
            );
            
            Some(pending_commit)
        } else {
            log::info!(
                "⭐ [FsWriterBase::allocate_new_block] No previous block to complete for path={}",
                self.path.path()
            );
            None
        };

        // 分配新块
        let lb = if let Some(lb) = self.last_block.take() {
            log::info!(
                "♻️ [FsWriterBase::allocate_new_block] Reusing cached block: path={}, block_id={}",
                self.path.path(),
                lb.block.id
            );
            lb
        } else {
            log::info!(
                "📞 [FsWriterBase::allocate_new_block] Requesting new block from master: path={}, has_commit_block={}",
                self.path.path(),
                commit_block.is_some()
            );
            
            let located_block = self.fs_client
                .add_block(&self.path, commit_block, &self.fs_context.client_addr)
                .await?;
                
            log::info!(
                "🎯 [FsWriterBase::allocate_new_block] Received new block from master: path={}, block_id={}, block_len={}",
                self.path.path(),
                located_block.block.id,
                located_block.block.len
            );
            
            located_block
        };

        log::info!(
            "🔧 [FsWriterBase::allocate_new_block] Creating block writer: path={}, block_id={}",
            self.path.path(),
            lb.block.id
        );
        
        let writer = BlockWriter::new(self.fs_context.clone(), lb.clone()).await?;
        
        // 🔧 更新file_blocks结构
        let block_size = self.fs_context.block_size();
        self.add_block_to_file_blocks(lb.block.id, block_size, lb.locs.clone());
        
        log::info!(
            "✅ [FsWriterBase::allocate_new_block] New block allocation completed: path={}, block_id={}, updated_file_blocks={}",
            self.path.path(),
            writer.block_id(),
            self.file_blocks.is_some()
        );
        
        Ok((0, writer)) // 新块的偏移量为 0
    }
}
