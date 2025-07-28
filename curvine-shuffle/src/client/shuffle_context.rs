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

use crate::client::WorkerClient;
use crate::common::ShuffleConf;
use curvine_common::FsResult;
use orpc::client::ClusterConnector;
use orpc::io::net::InetAddr;
use orpc::runtime::Runtime;
use std::sync::Arc;

pub struct ShuffleContext {
    connector: Arc<ClusterConnector>,
    conf: ShuffleConf,
}

impl ShuffleContext {
    pub fn new(conf: ShuffleConf, connector: Arc<ClusterConnector>) -> Self {
        Self { connector, conf }
    }

    pub async fn worker_client(&self, addr: &InetAddr) -> FsResult<WorkerClient> {
        WorkerClient::new(&self.connector, addr.clone()).await
    }

    pub fn conf(&self) -> &ShuffleConf {
        &self.conf
    }

    pub fn connector(&self) -> Arc<ClusterConnector> {
        self.connector.clone()
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.connector.clone_runtime()
    }

    pub fn root_dir(&self) -> &str {
        &self.conf.root_dir
    }
}
