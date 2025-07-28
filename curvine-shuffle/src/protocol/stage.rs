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

use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::io::net::InetAddr;
use std::fmt::Display;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct StageKey {
    pub app_id: String,
    pub stage_id: String,
}

impl StageKey {
    pub fn new(app_id: impl Into<String>, stage_id: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            stage_id: stage_id.into(),
        }
    }
}

impl Display for StageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.app_id, self.stage_id)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct SpiltKey {
    pub part_id: i32,
    pub split_id: i32,
}

impl SpiltKey {
    pub fn new(part_id: i32, split_id: i32) -> Self {
        Self { part_id, split_id }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct SplitInfo {
    pub part_id: i32,
    pub split_id: i32,
    pub write_len: i64,
    pub worker_addr: InetAddr,
}

#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum StageStatus {
    #[num_enum(default)]
    Running = 1,
    Committed = 2,
    Deleted = 3,
}

#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum AppStatus {
    #[num_enum(default)]
    Register = 1,
    Completed = 2,
    Deleted = 3,
}

pub struct StageReport {
    pub stage_key: StageKey,
    pub write_len: i64,
}
