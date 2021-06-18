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

use zenoh_flow::{graph, ZFZenohConnectorDescription, ZFZenohConnectorReceiver};

#[test]
fn it_generates_the_dataflow_graph() -> Result<(), String> {
    let dataflow_description_yaml = r#"
uuid: 1980C7E2-37E0-4BB5-A57D-0B5C81A4DE50
flow: TestPipeline

operators:
- name: Operator1
  id: IDOperator1
  uri: /path/to/operator1.so
  inputs: [type1]
  outputs: [type2]
  runtime: runtime

sources:
- name: Source0
  id: IDSource0
  uri: /path/to/source0.so
  output: type1
  runtime: runtime

sinks:
- name: Sink2
  id: IDSink2
  uri: /path/to/sink2.so
  input: type2
  runtime: runtime

links:
- from:
    name: Source0
    output: type1
  to:
    name: Operator1
    input: type1
- from:
    name: Operator1
    output: type2
  to:
    name: Sink2
    input: type2
"#;

    let dataflow_description = graph::deserialize_dataflow_description(dataflow_description_yaml);
    match graph::DataFlowGraph::new(Some(dataflow_description)) {
        Ok(_) => Ok(()),
        Err(_) => Err("Unexpected error".to_string()),
    }
}

#[test]
fn it_generates_the_dataflow_graph_with_connectors() -> Result<(), String> {
    let dataflow_description_yaml = r#"
uuid: 1980C7E2-37E0-4BB5-A57D-0B5C81A4DE50
flow: TestPipeline

operators:
- name: Operator1
  id: IDOperator1
  uri: /path/to/operator1.so
  inputs: [Type1]
  outputs: [Type2]
  runtime: runtime1

sources:
- name: Source0
  id: IDSource0
  uri: /path/to/source0.so
  output: Type1
  runtime: runtime0

sinks:
- name: Sink2
  id: IDSink2
  uri: /path/to/sink2.so
  input: Type2
  runtime: runtime2

links:
- from:
    name: Source0
    output: Type1
  to:
    name: Operator1
    input: Type1
- from:
    name: Operator1
    output: Type2
  to:
    name: Sink2
    input: Type2
"#;

    let dataflow_description = graph::deserialize_dataflow_description(dataflow_description_yaml);
    match graph::DataFlowGraph::new(Some(dataflow_description)) {
        Ok(graph) => {
            let connectors_type1 = vec!["Source0OutputType1", "Operator1InputType1"];
            let resource_type1 =
                "/zf/TestPipeline/1980C7E2-37E0-4BB5-A57D-0B5C81A4DE50/Source0/Type1";

            for connector in connectors_type1 {
                match graph
                    .operators
                    .iter()
                    .find(|(_, node)| node.get_name() == connector)
                {
                    Some((_, node)) => match node {
                        graph::DataFlowNode::ZenohConnector(zc) => match zc {
                            ZFZenohConnectorDescription::Receiver(rx) => {
                                assert_eq!(
                                    rx.resource, resource_type1,
                                    "{} != {}",
                                    rx.resource, resource_type1
                                )
                            }
                            ZFZenohConnectorDescription::Sender(tx) => {
                                assert_eq!(
                                    tx.resource, resource_type1,
                                    "{} != {}",
                                    tx.resource, resource_type1
                                )
                            }
                        },
                        _ => return Err("Operator should be a ZenohConnector".to_string()),
                    },

                    None => return Err(format!("Missing ZenohConnector {}", connector)),
                }
            }

            // TODO Refactor into dedicated function.
            let connectors_type2 = vec!["Operator1OutputType2", "Sink2InputType2"];
            let resource_type2 =
                "/zf/TestPipeline/1980C7E2-37E0-4BB5-A57D-0B5C81A4DE50/Operator1/Type2";

            for connector in connectors_type2 {
                match graph
                    .operators
                    .iter()
                    .find(|(_, node)| node.get_name() == connector)
                {
                    Some((_, node)) => match node {
                        graph::DataFlowNode::ZenohConnector(zc) => match zc {
                            ZFZenohConnectorDescription::Receiver(rx) => {
                                assert_eq!(rx.resource, resource_type2)
                            }
                            ZFZenohConnectorDescription::Sender(tx) => {
                                assert_eq!(tx.resource, resource_type2)
                            }
                        },
                        _ => return Err("Operator should be a ZenohConnector".to_string()),
                    },

                    None => return Err(format!("Missing ZenohConnector {}", connector)),
                }
            }

            Ok(())
        }
        Err(_) => Err("Unexpected error while parsing".to_string()),
    }
}
