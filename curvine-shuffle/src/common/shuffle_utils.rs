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

use crate::protocol::{SpiltKey, StageKey};
use curvine_common::fs::Path;
use orpc::CommonResult;

pub struct ShuffleUtils;

impl ShuffleUtils {
    pub fn get_stage_dir(root_dir: &str, stage: &StageKey) -> CommonResult<Path> {
        let path = format!("/{}/{}/{}", root_dir, stage.app_id, stage.stage_id,);

        Path::from_str(&path)
    }

    pub fn get_part_dir(root_dir: &str, stage: &StageKey, part_id: i32) -> CommonResult<Path> {
        let stage_dir = Self::get_stage_dir(root_dir, stage)?;
        let path = format!("{}/part_{}", stage_dir.path(), part_id);
        Path::from_str(&path)
    }

    pub fn get_split_path(root_dir: &str, stage: &StageKey, key: &SpiltKey) -> CommonResult<Path> {
        let part_dir = Self::get_part_dir(root_dir, stage, key.part_id)?;
        let path = format!("{}/split_{}.data", part_dir.path(), key.split_id);
        Path::from_str(&path)
    }
}
