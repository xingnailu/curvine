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

use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct CreateFlag(i32);

impl CreateFlag {
    pub const CRATE: i32 = 1;
    pub const OVERWRITE: i32 = 2;
    pub const APPEND: i32 = 4;

    pub fn new(flag: i32) -> Self {
        Self(flag)
    }

    pub fn create(&self) -> bool {
        self.0 & Self::CRATE != 0
    }

    pub fn overwrite(&self) -> bool {
        self.0 & Self::OVERWRITE != 0
    }

    pub fn append(&self) -> bool {
        self.0 & Self::APPEND != 0
    }

    pub fn value(&self) -> i32 {
        self.0
    }
}

impl From<i32> for CreateFlag {
    fn from(value: i32) -> Self {
        CreateFlag(value)
    }
}

impl Default for CreateFlag {
    fn default() -> Self {
        CreateFlag(Self::CRATE)
    }
}

impl Display for CreateFlag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.create() {
            write!(f, "create,")?
        }
        if self.overwrite() {
            write!(f, "overwrite,")?
        }
        if self.append() {
            write!(f, "append,")?
        }
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use crate::state::CreateFlag;

    #[test]
    fn test_flag() {
        let flags = CreateFlag::new(CreateFlag::CRATE | CreateFlag::OVERWRITE);

        assert!(flags.create());
        assert!(flags.overwrite());

        let flags = CreateFlag::new(CreateFlag::APPEND | CreateFlag::OVERWRITE);
        assert!(!flags.create());
        assert!(flags.overwrite());
        assert!(flags.append());
    }
}
