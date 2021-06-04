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

use zenoh_flow::InputRuleResult;
use zenoh_flow::{
    message::ZFCtrlMessage,
    serde::{Deserialize, Serialize},
    ZFContext,
};

// #[zfoperator(
//     modes = {"default": {"ir" : 'BuzzOperator::ir_1, "run": BuzzOperator::run_1, "or" :BuzzOperator::or_1}},
//     inputs = [u128, String], // Can also pass a shared memory as input/output in that case we do not serialize/deserialize
//     output = [String, u128],
// )]
#[derive(Serialize, Deserialize)]
struct BuzzOperator {}

impl BuzzOperator {
    fn run_1(
        &mut self,
        _ctx: &mut ZFContext,
        data: (Option<String>, Option<u128>),
    ) -> (Option<String>,) {
        match data {
            (Some(word), Some(val)) => {
                if val % 3 == 0 {
                    return (Some(format!("{}Buzz", word)),);
                }

                return (Some(word),);
            }
            (None, Some(val)) => {
                if val % 3 == 0 {
                    return (Some("Buzz".to_string()),);
                }

                return (None,);
            }
            _ => unreachable!("Nope. Because nope."),
        }
    }

    fn ir_1(
        &self,
        _ctx: &mut ZFContext,
        data: &(Option<String>, Option<u128>), //Pass the "box" with the data that you may need, the user can drop them if not needed
    ) -> (bool, (InputRuleResult, InputRuleResult)) {
        /*
            enum Sample {
                NotReady(NotReadySample)
                Ready(ReadySample)
            }

            eg. impl NotReadySample<T> {
            fn wait(&mut self) ....
        }

            eg. impl ReadySample<T> {
            fn drop(&mut self) ....
        }

        */
        match data {
            (Some(_), Some(_)) => (true, (InputRuleResult::Consume, InputRuleResult::Consume)),
            (None, Some(_)) => (false, (InputRuleResult::Wait, InputRuleResult::Consume)),
            (Some(_), None) => (false, (InputRuleResult::Consume, InputRuleResult::Wait)),
            (None, None) => (true, (InputRuleResult::Consume, InputRuleResult::Drop)),
        }
    }

    fn or_1(
        &mut self,
        _ctx: &mut ZFContext,
        data: (Option<String>,),
    ) -> (
        (Option<String>,),
        (Option<ZFCtrlMessage>, Option<ZFCtrlMessage>),
    ) {
        (data, (None, None))
    }
}

// Generated by Macro
impl BuzzOperator {
    fn deserialize_input_0(data: &[u8]) -> String {
        zenoh_flow::bincode::deserialize::<String>(data).unwrap()
    }

    fn deserialize_input_1(data: &[u8]) -> u128 {
        zenoh_flow::bincode::deserialize::<u128>(data).unwrap()
    }

    fn serialize_output_0(data: String) -> Vec<u8> {
        zenoh_flow::bincode::serialize(&data).unwrap()
    }

    fn deserialize_state(data: &[u8]) -> BuzzOperator {
        zenoh_flow::bincode::deserialize::<BuzzOperator>(data).unwrap()
    }

    fn serialize_state(data: BuzzOperator) -> Vec<u8> {
        zenoh_flow::bincode::serialize(&data).unwrap()
    }
}

// Generated by Macro
impl zenoh_flow::ZFOperator for BuzzOperator {
    fn make_run(&self, ctx: &mut zenoh_flow::ZFContext) -> Box<zenoh_flow::OperatorRun> {
        match ctx.mode {
            // mode id comes from the enum
            0 => {
                Box::new(
                    |ctx: &mut zenoh_flow::ZFContext,
                     data: Vec<Option<&zenoh_flow::message::Message>>|
                     -> zenoh_flow::OperatorResult {
                        // Zenoh Flow Ctrl IR - Inner
                        let inputs = (
                            Some(BuzzOperator::deserialize_input_0(data[0].unwrap().data())),
                            Some(BuzzOperator::deserialize_input_1(data[1].unwrap().data())),
                        );
                        let mut state = BuzzOperator::deserialize_state(&ctx.state);

                        // User IR
                        match BuzzOperator::ir_1(&state, ctx, &inputs) {
                            (false, (x, y)) => {
                                // We should recover information in case of InputRuleResult::Drop, because we need to drop data
                                return zenoh_flow::OperatorResult::InResult(Ok((
                                    false,
                                    vec![x, y],
                                )));
                            }
                            _ => {
                                // User Run
                                let run_res = BuzzOperator::run_1(&mut state, ctx, inputs);

                                //User OR
                                let (results, zf_results) =
                                    BuzzOperator::or_1(&mut state, ctx, run_res);

                                // Zenoh Flow Ctrl OR - Inner
                                let outputs = vec![zenoh_flow::message::Message::new(
                                    BuzzOperator::serialize_output_0(results.0.unwrap()),
                                )];

                                let zf_results = vec![zf_results.0, zf_results.1];
                                ctx.state = BuzzOperator::serialize_state(state);
                                zenoh_flow::OperatorResult::OutResult(Ok((outputs, zf_results)))
                            }
                        }
                    },
                )
            }
            _ => panic!("Mode not found!"),
        }
    }
}
