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

use crate::java::JavaUtils;
use crate::{FilesystemConf, LibFilesystem, LibFsReader, LibFsWriter};
use curvine_common::FsResult;
use jni::objects::JString;
use jni::sys::{jarray, jboolean, jstring};
use jni::JNIEnv;

pub struct JavaFilesystem {
    inner: LibFilesystem,
}

impl JavaFilesystem {
    pub fn new(env: &mut JNIEnv, conf: JString) -> FsResult<Self> {
        let toml_str = JavaUtils::jstring_to_string(env, &conf)?;
        let fs_conf = FilesystemConf::from_str(toml_str)?;
        let cluster_conf = fs_conf.into_cluster_conf()?;

        let inner = LibFilesystem::new(cluster_conf)?;
        Ok(Self { inner })
    }

    pub fn create(
        &self,
        env: &mut JNIEnv,
        path: JString,
        overwrite: jboolean,
    ) -> FsResult<LibFsWriter> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.create(path, JavaUtils::jbool_to_bool(overwrite))
    }

    pub fn append(&self, env: &mut JNIEnv, path: JString) -> FsResult<LibFsWriter> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.append(path)
    }

    pub fn open(&self, env: &mut JNIEnv, path: JString) -> FsResult<LibFsReader> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.open(path)
    }

    pub fn mkdir(
        &self,
        env: &mut JNIEnv,
        path: JString,
        create_parent: jboolean,
    ) -> FsResult<bool> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner
            .mkdir(path, JavaUtils::jbool_to_bool(create_parent))
    }

    pub fn get_file_status(&self, env: &mut JNIEnv, path: JString) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let status = self.inner.get_status(path)?;

        let byte_arr = JavaUtils::new_jarray(env, &status)?;
        Ok(byte_arr)
    }

    pub fn list_status(&self, env: &mut JNIEnv, path: JString) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let status = self.inner.list_status(path)?;

        let byte_arr = JavaUtils::new_jarray(env, &status)?;
        Ok(byte_arr)
    }

    pub fn rename(&self, env: &mut JNIEnv, src: JString, dst: JString) -> FsResult<bool> {
        let src = JavaUtils::jstring_to_string(env, &src)?;
        let dst = JavaUtils::jstring_to_string(env, &dst)?;
        self.inner.rename(src, dst)
    }

    pub fn delete(&self, env: &mut JNIEnv, path: JString, recursive: jboolean) -> FsResult<()> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        self.inner.delete(path, JavaUtils::jbool_to_bool(recursive))
    }

    pub fn get_master_info(&self, env: &mut JNIEnv) -> FsResult<jarray> {
        let status = self.inner.get_master_info()?;
        let byte_arr = JavaUtils::new_jarray(env, &status)?;
        Ok(byte_arr)
    }

    pub fn get_mount_info(&self, env: &mut JNIEnv, path: JString) -> FsResult<jarray> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let bytes = self.inner.get_mount_info(path)?;
        let byte_arr = JavaUtils::new_jarray(env, &bytes)?;
        Ok(byte_arr)
    }

    pub fn get_ufs_path(&self, env: &mut JNIEnv, path: JString) -> FsResult<jstring> {
        let path = JavaUtils::jstring_to_string(env, &path)?;
        let ufs_path = self.inner.get_ufs_path(path)?;
        let string = JavaUtils::new_jstring(env, ufs_path)?;
        Ok(string)
    }

    pub fn get_mount_table(&self, env: &mut JNIEnv) -> FsResult<jarray> {
        let bytes = self.inner.get_mount_table()?;
        let byte_arr = JavaUtils::new_jarray(env, &bytes)?;
        Ok(byte_arr)
    }

    pub fn mount(&self, env: &mut JNIEnv, ufs_path: JString, cv_path: JString, properties_json: JString) -> FsResult<()> {
        let ufs_path = JavaUtils::jstring_to_string(env, &ufs_path)?;
        let cv_path = JavaUtils::jstring_to_string(env, &cv_path)?;
        let properties_json = JavaUtils::jstring_to_string(env, &properties_json)?;
        
        // Parse JSON properties to HashMap
        let properties: std::collections::HashMap<String, String> = serde_json::from_str(&properties_json)
            .map_err(|e| curvine_common::error::FsError::common(format!("Failed to parse properties JSON: {}", e)))?;
        
        self.inner.mount(ufs_path, cv_path, properties)
    }

    pub fn umount(&self, env: &mut JNIEnv, cv_path: JString) -> FsResult<()> {
        let cv_path = JavaUtils::jstring_to_string(env, &cv_path)?;
        self.inner.umount(cv_path)
    }

    pub fn cleanup(&self) {
        self.inner.cleanup()
    }
}
