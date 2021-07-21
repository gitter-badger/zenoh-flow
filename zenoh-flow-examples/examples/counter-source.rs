//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use async_std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use zenoh_flow::{
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnOutputRule, FnSourceRun, FutRunResult, RunResult, SourceTrait, StateTrait,
        ZFContext, ZFPortDescriptor, ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_empty_state,
};
use zenoh_flow_examples::RandomData;

static SOURCE: &str = "Counter";

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize, Deserialize, Debug, ZFState)]
struct CountSource {}

impl CountSource {
    fn new(configuration: Option<HashMap<String, String>>) -> Self {
        match configuration {
            Some(conf) => {
                let initial = conf.get("initial").unwrap().parse::<u64>().unwrap();
                COUNTER.store(initial, Ordering::AcqRel);
                CountSource {}
            }
            None => CountSource {},
        }
    }

    async fn run_1(_ctx: ZFContext) -> RunResult {
        let mut results: HashMap<String, Arc<dyn DataTrait>> = HashMap::new();
        let d = RandomData {
            d: COUNTER.fetch_add(1, Ordering::AcqRel),
        };
        results.insert(String::from(SOURCE), zf_data!(d));
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(results)
    }
}

impl SourceTrait for CountSource {
    fn get_run(&self, ctx: ZFContext) -> FnSourceRun {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(|ctx: ZFContext| -> FutRunResult { Box::pin(Self::run_1(ctx)) }),
            _ => panic!("No way"),
        }
    }

    fn get_output_rule(&self, _ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(zenoh_flow::default_output_rule)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        zf_empty_state!()
    }
}

// //Also generated by macro
zenoh_flow::export_source!(register);

extern "C" fn register(
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn zenoh_flow::SourceTrait + Send>> {
    Ok(Box::new(CountSource::new(configuration)) as Box<dyn zenoh_flow::SourceTrait + Send>)
}
