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

use std::io;
use rand::Rng;
use zenoh_flow::{
    serde::{Deserialize, Serialize},
    operator::{SinkTrait, FnSinkRun, FnInputRule, InputRuleResult, DataTrait, StateTrait},
    types::{ZFLinkId, ZFContext, Token},
    zenoh_flow_macros::ZFState,
    downcast_mut, zf_spin_lock, downcast,
};
use zenoh_flow_examples::ZFOpenCVBytes;
use std::collections::HashMap;
use async_std::sync::{Arc, Mutex};
use std::any::Any;

use opencv::{
    highgui,
    prelude::*,
};

#[derive(Debug)]
struct VideoSink {
    // pub state : VideoState,
}

#[derive(ZFState, Clone, Debug)]
struct VideoState {
    pub window_name : String,

}

impl VideoSink {

    pub fn new() -> Self {
        let window_name = &format!("Video-Sink");
        highgui::named_window(window_name, 1).unwrap();
        // let state = VideoState {
        //     window_name: window_name.to_string(),
        // };
        // Self {
        //     state
        // }
        Self {}
    }

    pub fn ir_1(
        _ctx: &mut ZFContext,
        _inputs: &mut HashMap<ZFLinkId, Token>,
    ) -> InputRuleResult {
        Ok(true)
    }


    pub fn run_1(ctx: &mut ZFContext, inputs: HashMap<ZFLinkId, Arc<dyn DataTrait>>) -> () {
        let mut results: HashMap<ZFLinkId, Arc<dyn DataTrait>> = HashMap::new();
        // let mut state = ctx.get_state(); //getting state, rename take
        // let mut _state = downcast_mut!(VideoState, state).unwrap(); //downcasting to right type

        let window_name = &format!("Video-Sink");

        if let Some(data) = inputs.get(&0) {
            match downcast!(ZFOpenCVBytes, data) {
                Some(d) => {

                    let data = (zf_spin_lock!(d.bytes)).try_borrow().unwrap().to_owned();

                    let decoded = match opencv::imgcodecs::imdecode(
                        &opencv::types::VectorOfu8::from_iter(data),
                        opencv::imgcodecs::IMREAD_COLOR) {
                            Ok(d) => d,
                            Err(e) => panic!("Unable to decode {:?}", e),
                        };

                    if decoded.size().unwrap().width > 0 {
                        match highgui::imshow(window_name, &decoded){
                            Ok(_) => (),
                            Err(e) => eprintln!("Error when display {:?}", e),
                        };
                    }

                    highgui::wait_key(10);

                }
                None => panic!("Unable to downcast!"),
            }
        } else {
            panic!("No input!")
        }
        // ctx.set_state(state); //storing new state
    }
}





impl SinkTrait for VideoSink {

    fn get_input_rule(&self, ctx: &ZFContext) -> Box<FnInputRule> {
        match ctx.mode {
            0 => Box::new(Self::ir_1),
            _ => panic!("No way"),
        }
    }

    fn get_run(&self, ctx: &ZFContext) -> Box<FnSinkRun> {
        match ctx.mode {
            0 => Box::new(Self::run_1),
            _ => panic!("No way"),
        }
    }

    fn get_state(&self) -> Option<Box<dyn StateTrait>> {
        None
    }

}

// //Also generated by macro
zenoh_flow::export_sink!(register);

extern "C" fn register(registrar: &mut dyn zenoh_flow::loader::ZFSinkRegistrarTrait) {
    registrar.register_zfsink(
        "video-sink",
        Box::new(VideoSink::new()) as Box<dyn zenoh_flow::operator::SinkTrait + Send>,
    );
}