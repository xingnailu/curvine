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

use crate::common::ShuffleConf;
use crate::master::{MasterHandler, ShuffleManager};
use curvine_server::master::Master;
use log::info;
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::RpcRuntime;
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;
use std::sync::Arc;

pub struct MasterService {
    shuffle_manager: ShuffleManager,
}

impl MasterService {
    pub fn new(shuffle_manager: ShuffleManager) -> Self {
        Self { shuffle_manager }
    }
}

impl HandlerService for MasterService {
    type Item = MasterHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        MasterHandler::new(self.shuffle_manager.clone())
    }
}

pub struct ShuffleMaster {
    rpc_server: RpcServer<MasterService>,
    fs_master: Master,
}

impl ShuffleMaster {
    pub fn with_conf(conf: ShuffleConf) -> CommonResult<Self> {
        let fs_master = Master::with_conf(conf.cluster_conf.clone())?;
        let fs_service = fs_master.service();

        let server_conf = conf.master_server_conf();
        let rt = if conf.alone_rt {
            Arc::new(server_conf.create_runtime())
        } else {
            fs_service.clone_rt()
        };

        let context = conf.create_context(rt.clone())?;
        let shuffle_manager = ShuffleManager::new(
            rt.clone(),
            fs_service.clone_worker_manager(),
            fs_service.master_monitor(),
            context,
        );
        let shuffle_service = MasterService::new(shuffle_manager);
        let rpc_server = RpcServer::with_rt(rt.clone(), server_conf, shuffle_service);

        Ok(Self {
            rpc_server,
            fs_master,
        })
    }

    pub async fn start(self) -> (ServerStateListener, ServerStateListener) {
        let fs_status = self.fs_master.start().await;
        let mut shuffle_status = self.rpc_server.start();
        shuffle_status.wait_running().await.unwrap();

        (fs_status, shuffle_status)
    }

    pub fn block_on_start(self) {
        let fs_rt = self.fs_master.service().clone_rt();
        let shuffle_rt = self.rpc_server.clone_rt();

        let (fs_master, rpc_server) = (self.fs_master, self.rpc_server);
        let mut fs_status = fs_rt.block_on(async move { fs_master.start().await });

        let mut shuffle_status = shuffle_rt.block_on(async move {
            let mut shuffle_status = rpc_server.start();
            shuffle_status.wait_running().await.unwrap();
            shuffle_status
        });

        shuffle_rt.block_on(async move {
            tokio::select! {
                _ = fs_status.wait_stop() => {
                    info!("fs_master stop");
                }
                _ = shuffle_status.wait_stop() => {
                    info!("fs_worker stop");
                }
            }
        })
    }
}
