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

use curvine_common::conf::ClusterConf;
use curvine_shuffle::common::ShuffleConf;
use curvine_shuffle::master::ShuffleMaster;
use curvine_shuffle::worker::ShuffleWorker;
use orpc::common::Logger;
use orpc::CommonResult;
use std::thread;
use std::time::Duration;

fn main() -> CommonResult<()> {
    Logger::default();
    let mut conf = ClusterConf::from("etc/curvine-cluster.toml")?;
    conf.format_master = true;
    conf.format_worker = true;
    conf.journal.snapshot_interval = "10s".to_owned();
    conf.master.min_block_size = 1024 * 1024;

    let shuffle_conf = ShuffleConf {
        cluster_conf: conf.clone(),
        ..Default::default()
    };

    let master = ShuffleMaster::with_conf(shuffle_conf.clone())?;

    thread::spawn(move || {
        master.block_on_start();
    });

    let worker = ShuffleWorker::with_conf(shuffle_conf)?;
    thread::spawn(move || {
        worker.block_on_start();
    });

    thread::sleep(Duration::from_secs(u64::MAX));
    Ok(())
}
