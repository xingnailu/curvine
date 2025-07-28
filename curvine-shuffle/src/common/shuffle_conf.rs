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

use crate::client::ShuffleContext;
use curvine_common::conf::ClusterConf;
use orpc::client::ClusterConnector;
use orpc::common::{ByteUnit, DurationUnit, Utils};
use orpc::io::net::NodeAddr;
use orpc::runtime::Runtime;
use orpc::server::ServerConf;
use orpc::CommonResult;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShuffleConf {
    pub cluster_conf: ClusterConf,

    pub alone_rt: bool,
    pub master_port: u16,
    pub worker_port: u16,
    pub root_dir: String,

    #[serde(skip)]
    pub split_size: i64,
    #[serde(alias = "split_size")]
    pub split_size_str: String,

    #[serde(skip)]
    pub worker_heartbeat_interval: Duration,
    #[serde(alias = "worker_heartbeat_interval")]
    pub worker_heartbeat_interval_str: String,

    #[serde(skip)]
    pub app_heartbeat_interval: Duration,
    #[serde(alias = "app_heartbeat_interval")]
    pub app_heartbeat_interval_str: String,

    #[serde(skip)]
    pub app_check_interval: Duration,
    #[serde(alias = "app_check_interval")]
    pub app_check_interval_str: String,

    #[serde(skip)]
    pub app_expired_interval: Duration,
    #[serde(alias = "app_expired_interval")]
    pub app_expired_interval_str: String,
}

impl ShuffleConf {
    pub const DEFAULT_RAFT_PORT: u16 = 8998;
    pub const DEFAULT_WORKER_PORT: u16 = 8999;

    pub fn with_file(path: impl AsRef<str>, cluster_conf: ClusterConf) -> CommonResult<Self> {
        let mut conf: ShuffleConf = Utils::read_toml_conf(path.as_ref())?;
        conf.cluster_conf = cluster_conf;
        conf.init()?;

        Ok(conf)
    }

    pub fn with_cluster_conf(cluster_conf: ClusterConf) -> Self {
        Self {
            cluster_conf,
            ..Default::default()
        }
    }

    pub fn init(&mut self) -> CommonResult<()> {
        self.worker_heartbeat_interval =
            DurationUnit::from_str(&self.worker_heartbeat_interval_str)?.as_duration();
        self.app_heartbeat_interval =
            DurationUnit::from_str(&self.app_heartbeat_interval_str)?.as_duration();
        self.app_check_interval =
            DurationUnit::from_str(&self.app_check_interval_str)?.as_duration();
        self.app_expired_interval =
            DurationUnit::from_str(&self.app_expired_interval_str)?.as_duration();
        self.split_size = ByteUnit::from_str(&self.split_size_str)?.as_byte() as i64;

        Ok(())
    }

    pub fn master_server_conf(&self) -> ServerConf {
        let mut conf = self.cluster_conf.master_server_conf();
        conf.name = "shuffle-master".to_string();
        conf.port = self.master_port;
        conf
    }

    pub fn worker_server_conf(&self) -> ServerConf {
        let mut conf = self.cluster_conf.worker_server_conf();
        conf.name = "shuffle-worker".to_string();
        conf.port = self.worker_port;
        conf
    }

    pub fn create_context(&self, rt: Arc<Runtime>) -> CommonResult<Arc<ShuffleContext>> {
        let connector = ClusterConnector::with_rt(self.cluster_conf.client_rpc_conf(), rt);

        let master_nodes = self.cluster_conf.master_nodes();
        for node in master_nodes {
            let new_node = NodeAddr::new(node.id, node.hostname.to_string(), self.master_port);
            connector.add_node(new_node)?;
        }

        let context = ShuffleContext::new(self.clone(), Arc::new(connector));
        Ok(Arc::new(context))
    }
}

impl Default for ShuffleConf {
    fn default() -> Self {
        Self {
            cluster_conf: ClusterConf::default(),
            alone_rt: true,
            master_port: Self::DEFAULT_RAFT_PORT,
            worker_port: Self::DEFAULT_WORKER_PORT,
            root_dir: "/shuffle".to_string(),

            split_size: 0,
            split_size_str: "1GB".to_string(),

            worker_heartbeat_interval: Default::default(),
            worker_heartbeat_interval_str: "60s".to_string(),

            app_heartbeat_interval: Default::default(),
            app_heartbeat_interval_str: "60s".to_string(),

            app_check_interval: Default::default(),
            app_check_interval_str: "10m".to_string(),

            app_expired_interval: Default::default(),
            app_expired_interval_str: "2h".to_string(),
        }
    }
}
