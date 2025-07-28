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
use crate::worker::{StageManager, WorkerHandler};
use curvine_client::file::CurvineFileSystem;
use curvine_server::worker::Worker;
use log::info;
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::RpcRuntime;
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct WorkerService {
    stage_manager: StageManager,
}

impl WorkerService {
    pub fn new(stage_manager: StageManager) -> Self {
        Self { stage_manager }
    }
}

impl HandlerService for WorkerService {
    type Item = WorkerHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        WorkerHandler::new(self.stage_manager.clone())
    }
}

pub struct ShuffleWorker {
    rpc_server: RpcServer<WorkerService>,
    fs_worker: Worker,
}

impl ShuffleWorker {
    pub fn with_conf(conf: ShuffleConf) -> CommonResult<Self> {
        let fs_worker = Worker::with_conf(conf.cluster_conf.clone())?;
        let fs_service = fs_worker.service();

        let server_conf = conf.worker_server_conf();
        let rt = if conf.alone_rt {
            Arc::new(server_conf.create_runtime())
        } else {
            fs_service.clone_rt()
        };

        let fs = CurvineFileSystem::with_rt(conf.cluster_conf.clone(), rt.clone())?;
        let context = conf.create_context(rt.clone())?;
        let stage_manager = StageManager::new(fs, context);
        let shuffle_service = WorkerService::new(stage_manager);
        let rpc_server = RpcServer::with_rt(rt.clone(), server_conf, shuffle_service);

        Ok(Self {
            rpc_server,
            fs_worker,
        })
    }

    pub async fn start(self) -> (ServerStateListener, ServerStateListener) {
        let fs_status = self.fs_worker.start().await;
        let mut shuffle_status = self.rpc_server.start();
        shuffle_status.wait_running().await.unwrap();

        (fs_status, shuffle_status)
    }

    pub fn block_on_start(self) {
        let fs_rt = self.fs_worker.service().clone_rt();
        let shuffle_rt = self.rpc_server.clone_rt();

        let (fs_worker, rpc_server) = (self.fs_worker, self.rpc_server);
        let mut fs_status = fs_rt.block_on(async move { fs_worker.start().await });

        let mut shuffle_status = shuffle_rt.block_on(async move {
            let mut shuffle_status = rpc_server.start();
            shuffle_status.wait_running().await.unwrap();
            shuffle_status
        });

        shuffle_rt.block_on(async move {
            tokio::select! {
                _ = fs_status.wait_stop() => {
                    info!("fs_worker stop");
                }
                _ = shuffle_status.wait_stop() => {
                    info!("shuffle_worker stop");
                }
            }
        })
    }
}
