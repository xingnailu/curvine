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

use curvine_shuffle::client::{ShuffleContext, ShuffleWriter};
use curvine_shuffle::common::ShuffleConf;
use curvine_shuffle::protocol::StageKey;
use orpc::common::{Logger, Utils};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;
use std::thread;

fn create_context() -> Arc<ShuffleContext> {
    let conf = ShuffleConf::default();
    let rt = Arc::new(AsyncRuntime::single());
    conf.create_context(rt).unwrap()
}
#[test]
fn test() {
    Logger::default();
    let h1 = thread::spawn(move || run(1));
    let h2 = thread::spawn(move || run(2));
    let h3 = thread::spawn(move || run(3));

    h1.join().unwrap().unwrap();
    h2.join().unwrap().unwrap();
    h3.join().unwrap().unwrap();
}

fn run(task_id: i32) -> CommonResult<()> {
    let context = create_context();
    let rt = context.clone_runtime();
    let stage = StageKey::new("app_1", "stage_1");
    let mut writer = ShuffleWriter::new(context, stage, task_id, 0, 3, 4096);

    rt.block_on(async move {
        for _ in 0..10 {
            writer.write(Utils::rand_str(1024).into()).await.unwrap();
        }
        writer.commit().await.unwrap();
    });
    Ok(())
}
