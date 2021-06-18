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

use async_std::sync::{Arc, Mutex};
use petgraph::dot::{Config, Dot};
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use std::collections::HashMap;

use crate::types::{ZFError, ZFLinkId, ZFOperatorName, ZFPortFrom, ZFPortTo, ZFResult};
use crate::{
    link::{link, ZFLinkReceiver, ZFLinkSender},
    ZFLink,
};
use crate::{
    loader::{
        load_operator, load_sink, load_source, ZFOperatorRunner, ZFSinkRunner, ZFSourceRunner,
    },
    ZFZenohConnectorDescription,
};
use crate::{message::ZFMessage, ZFZenohConnectorReceiver};
use crate::{
    serde::{Deserialize, Serialize},
    ZFZenohConnectorSender,
};
use crate::{ZFOperatorDescription, ZFOperatorId, ZFSinkDescription, ZFSourceDescription};

pub enum Runner {
    Operator(ZFOperatorRunner),
    Source(ZFSourceRunner),
    Sink(ZFSinkRunner),
}

impl Runner {
    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            Runner::Operator(runner) => runner.run().await,
            Runner::Source(runner) => runner.run().await,
            Runner::Sink(runner) => runner.run().await,
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        match self {
            Runner::Operator(runner) => runner.add_input(input),
            Runner::Source(_runner) => panic!("Sources does not have inputs!"),
            Runner::Sink(runner) => runner.add_input(input),
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        match self {
            Runner::Operator(runner) => runner.add_output(output),
            Runner::Source(runner) => runner.add_output(output),
            Runner::Sink(_runner) => panic!("Sinks does not have output!"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataFlowNode {
    Operator(ZFOperatorDescription),
    Source(ZFSourceDescription),
    Sink(ZFSinkDescription),
    ZenohConnector(ZFZenohConnectorDescription),
}

impl std::fmt::Display for DataFlowNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataFlowNode::Operator(inner) => write!(f, "{}", inner),
            DataFlowNode::Source(inner) => write!(f, "{}", inner),
            DataFlowNode::Sink(inner) => write!(f, "{}", inner),
            DataFlowNode::ZenohConnector(inner) => write!(f, "{}", inner),
        }
    }
}

impl DataFlowNode {
    pub fn has_input(&self, id: ZFLinkId) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.inputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Source(_) => false,
            DataFlowNode::Sink(sink) => sink.input == id,
            DataFlowNode::ZenohConnector(zlink) => match zlink {
                ZFZenohConnectorDescription::Sender(tx) => tx.input == id,
                ZFZenohConnectorDescription::Receiver(_) => false,
            },
        }
    }

    pub fn has_output(&self, id: ZFLinkId) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.outputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Sink(_) => false,
            DataFlowNode::Source(source) => source.output == id,
            DataFlowNode::ZenohConnector(zlink) => match zlink {
                ZFZenohConnectorDescription::Sender(_) => false,
                ZFZenohConnectorDescription::Receiver(rx) => rx.output == id,
            },
        }
    }

    // pub fn get_id(&self) -> ZFOperatorId {
    //     match self {
    //         DataFlowNode::Operator(op) => op.id.clone(),
    //         DataFlowNode::Sink(s) => s.id.clone(),
    //         DataFlowNode::Source(s) => s.id.clone(),
    //     }
    // }

    pub fn get_name(&self) -> ZFOperatorName {
        match self {
            DataFlowNode::Operator(op) => op.name.clone(),
            DataFlowNode::Sink(s) => s.name.clone(),
            DataFlowNode::Source(s) => s.name.clone(),
            DataFlowNode::ZenohConnector(zc) => match zc {
                ZFZenohConnectorDescription::Receiver(rx) => rx.name.clone(),
                ZFZenohConnectorDescription::Sender(tx) => tx.name.clone(),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescription {
    pub flow: String,
    pub uuid: String,
    pub operators: Vec<ZFOperatorDescription>,
    pub sources: Vec<ZFSourceDescription>,
    pub sinks: Vec<ZFSinkDescription>,
    pub links: Vec<ZFLink>,
}

impl DataFlowDescription {
    pub fn from_yaml(data: String) -> Self {
        serde_yaml::from_str::<DataFlowDescription>(&data).unwrap()
    }

    pub fn find_node(&self, id: ZFOperatorId) -> Option<DataFlowNode> {
        match self.get_operator(id.clone()) {
            Some(o) => Some(DataFlowNode::Operator(o)),
            None => match self.get_source(id.clone()) {
                Some(s) => Some(DataFlowNode::Source(s)),
                None => match self.get_sink(id) {
                    Some(s) => Some(DataFlowNode::Sink(s)),
                    None => None,
                },
            },
        }
    }

    fn get_operator(&self, id: ZFOperatorId) -> Option<ZFOperatorDescription> {
        self.operators.iter().find(|&o| o.id == id).cloned()
    }

    fn get_source(&self, id: ZFOperatorId) -> Option<ZFSourceDescription> {
        self.sources.iter().find(|&o| o.id == id).cloned()
    }

    fn get_sink(&self, id: ZFOperatorId) -> Option<ZFSinkDescription> {
        self.sinks.iter().find(|&o| o.id == id).cloned()
    }
}

pub struct DataFlowGraph {
    pub flow: String,
    pub uuid: String,
    pub operators: Vec<(NodeIndex, DataFlowNode)>,
    pub links: Vec<(EdgeIndex, ZFLink)>,
    pub graph: StableGraph<DataFlowNode, ZFLinkId>,
    pub operators_runners: HashMap<ZFOperatorName, Arc<Mutex<Runner>>>,
}

pub fn deserialize_dataflow_description(data: &str) -> DataFlowDescription {
    serde_yaml::from_str::<DataFlowDescription>(data).unwrap()
}

impl DataFlowGraph {
    pub fn new(df: Option<DataFlowDescription>) -> ZFResult<Self> {
        match df {
            Some(df) => {
                let mut graph = StableGraph::<DataFlowNode, ZFLinkId>::new();
                let mut operators =
                    Vec::with_capacity(df.operators.len() + df.sinks.len() + df.sources.len());
                let mut links = Vec::new();
                for o in df.operators {
                    operators.push((
                        graph.add_node(DataFlowNode::Operator(o.clone())),
                        DataFlowNode::Operator(o),
                    ));
                }

                for o in df.sources {
                    operators.push((
                        graph.add_node(DataFlowNode::Source(o.clone())),
                        DataFlowNode::Source(o),
                    ));
                }

                for o in df.sinks {
                    operators.push((
                        graph.add_node(DataFlowNode::Sink(o.clone())),
                        DataFlowNode::Sink(o),
                    ));
                }

                for l in &df.links {
                    // First check if the LinkId are the same
                    if l.from.output != l.to.input {
                        log::error!(
                            "[YAML] Ports do not match:\n\t{:?}.{:?} != {:?}.{:?}",
                            l.from.name,
                            l.from.output,
                            l.to.name,
                            l.to.input
                        );
                        return Err(ZFError::PortIdNotMatching((
                            l.from.output.clone(),
                            l.to.input.clone(),
                        )));
                    }

                    let from_index =
                        match operators.iter().find(|&(_, o)| o.get_name() == l.from.name) {
                            Some((idx, op)) => match op.has_output(l.from.output.clone()) {
                                true => *idx,
                                false => {
                                    log::error!(
                                        "[YAML] Output {:?} not found for operator {:?}.",
                                        &l.from.output,
                                        op
                                    );
                                    return Err(ZFError::PortNotFound((
                                        l.from.output.clone(),
                                        format!("{:?}", op),
                                    )));
                                }
                            },
                            None => {
                                log::error!(
                                    "[YAML] Operator {:?} not found in flow description.",
                                    l.from.name
                                );
                                return Err(ZFError::OperatorNotFound(l.from.name.clone()));
                            }
                        };

                    let to_index = match operators.iter().find(|&(_, o)| o.get_name() == l.to.name)
                    {
                        Some((idx, op)) => match op.has_input(l.to.input.clone()) {
                            true => *idx,
                            false => {
                                log::error!(
                                    "[YAML] Input {:?} not found for operator {:?}",
                                    l.to.input,
                                    op
                                );
                                return Err(ZFError::PortNotFound((
                                    l.to.input.clone(),
                                    format!("{:?}", op),
                                )));
                            }
                        },
                        None => {
                            log::error!(
                                "[YAML] Operator {:?} not found in flow description.",
                                l.to.input
                            );
                            return Err(ZFError::OperatorNotFound(l.to.input.clone()));
                        }
                    };

                    // Check if the runtime for both operators is the same.
                    let from_runtime: String = match operators
                        .iter()
                        .find(|&(_, o)| o.get_name() == l.from.name)
                    {
                        Some((_, op)) => match op {
                            // Only an Operator and a Source can forward data.
                            DataFlowNode::Operator(op) => op.runtime.clone(),
                            DataFlowNode::Source(op) => op.runtime.clone(),
                            _ => {
                                return Err(ZFError::InvalidData(
                                    "Wrong DataflowNode type".to_string(),
                                ))
                            }
                        },
                        None => return Err(ZFError::InvalidData("Invalid operator".to_string())),
                    };

                    let to_runtime: String = match operators
                        .iter()
                        .find(|&(_, o)| o.get_name() == l.to.name)
                    {
                        Some((_, op)) => match op {
                            DataFlowNode::Operator(op) => op.runtime.clone(),
                            DataFlowNode::Sink(op) => op.runtime.clone(),
                            // Only an Operator and a Sink can receive data.
                            _ => {
                                return Err(ZFError::InvalidData(
                                    "Wrong DataflowNode type".to_string(),
                                ))
                            }
                        },
                        None => return Err(ZFError::InvalidData("Invalid operator".to_string())),
                    };

                    if from_runtime == to_runtime {
                        links.push((
                            graph.add_edge(from_index, to_index, l.from.output.clone()),
                            l.clone(),
                        ));
                    } else {
                        // from -> ZFConnectorSender -> ZFConnectorReceiver -> to
                        let resource = format!(
                            "/zf/{}/{}/{}/{}",
                            df.flow, df.uuid, l.from.name, l.from.output
                        );

                        let connector_send = ZFZenohConnectorSender {
                            kind: "Sender".to_string(),
                            name: format!("{}Input{}", l.to.name, l.to.input),
                            resource: resource.clone(),
                            input: l.to.input.clone(),
                            runtime: to_runtime,
                        };

                        let connector_send_index = graph.add_node(DataFlowNode::ZenohConnector(
                            ZFZenohConnectorDescription::Sender(connector_send.clone()),
                        ));

                        let mut link_send = l.clone();
                        link_send.to = ZFPortTo {
                            name: connector_send.name.clone(),
                            input: connector_send.input.clone(),
                        };

                        operators.push((
                            connector_send_index,
                            DataFlowNode::ZenohConnector(ZFZenohConnectorDescription::Sender(
                                connector_send,
                            )),
                        ));

                        links.push((
                            graph.add_edge(
                                from_index,
                                connector_send_index,
                                link_send.from.output.clone(),
                            ),
                            link_send,
                        ));

                        let connector_recv = ZFZenohConnectorReceiver {
                            kind: "Receiver".to_string(),
                            name: format!("{}Output{}", l.from.name, l.from.output),
                            resource: resource.clone(),
                            output: l.from.output.clone(),
                            runtime: from_runtime,
                        };

                        let connector_recv_index = graph.add_node(DataFlowNode::ZenohConnector(
                            ZFZenohConnectorDescription::Receiver(connector_recv.clone()),
                        ));

                        let mut link_recv = l.clone();
                        link_recv.from = ZFPortFrom {
                            name: connector_recv.name.clone(),
                            output: connector_recv.output.clone(),
                        };

                        operators.push((
                            connector_recv_index,
                            DataFlowNode::ZenohConnector(ZFZenohConnectorDescription::Receiver(
                                connector_recv,
                            )),
                        ));

                        links.push((
                            graph.add_edge(
                                connector_recv_index,
                                to_index,
                                link_recv.from.output.clone(),
                            ),
                            link_recv,
                        ));
                    }
                }

                Ok(Self {
                    flow: df.flow,
                    uuid: df.uuid,
                    operators,
                    links,
                    graph,
                    operators_runners: HashMap::new(),
                })
            }

            None => Ok(Self {
                flow: "".to_string(),
                uuid: "".to_string(),
                operators: Vec::new(),
                links: Vec::new(),
                graph: StableGraph::<DataFlowNode, ZFLinkId>::new(),
                operators_runners: HashMap::new(),
            }),
        }
    }

    pub fn set_flow(&mut self, name: String) {
        self.flow = name;
    }

    pub fn to_dot_notation(&self) -> String {
        format!("{}", Dot::with_config(&self.graph, &[Config::EdgeNoLabel]))
    }

    pub fn add_static_operator(
        &mut self,
        id: ZFOperatorId,
        name: String,
        inputs: Vec<ZFLinkId>,
        outputs: Vec<ZFLinkId>,
        operator: Box<dyn crate::operator::OperatorTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFOperatorDescription {
            id,
            name: name.clone(),
            inputs,
            outputs,
            uri: None,
            configuration,
            runtime: "".to_string(),
        };
        self.operators.push((
            self.graph
                .add_node(DataFlowNode::Operator(descriptor.clone())),
            DataFlowNode::Operator(descriptor),
        ));
        let runner = Runner::Operator(crate::loader::ZFOperatorRunner::new_static(operator));
        self.operators_runners
            .insert(name, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_static_source(
        &mut self,
        id: ZFOperatorId,
        name: String,
        output: ZFLinkId,
        source: Box<dyn crate::operator::SourceTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFSourceDescription {
            id,
            name: name.clone(),
            output,
            uri: None,
            configuration,
            runtime: "".to_string(),
        };
        self.operators.push((
            self.graph
                .add_node(DataFlowNode::Source(descriptor.clone())),
            DataFlowNode::Source(descriptor),
        ));
        let runner = Runner::Source(crate::loader::ZFSourceRunner::new_static(source));
        self.operators_runners
            .insert(name, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_static_sink(
        &mut self,
        id: ZFOperatorId,
        name: String,
        input: ZFLinkId,
        sink: Box<dyn crate::operator::SinkTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFSinkDescription {
            id,
            name: name.clone(),
            input,
            uri: None,
            configuration,
            runtime: "".to_string(),
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Sink(descriptor.clone())),
            DataFlowNode::Sink(descriptor),
        ));
        let runner = Runner::Sink(crate::loader::ZFSinkRunner::new_static(sink));
        self.operators_runners
            .insert(name, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_link(
        &mut self,
        from: ZFPortFrom,
        to: ZFPortTo,
        size: Option<usize>,
        queueing_policy: Option<String>,
        priority: Option<usize>,
    ) -> ZFResult<()> {
        let connection = ZFLink {
            from,
            to,
            size,
            queueing_policy,
            priority,
        };

        if connection.from.output == connection.to.input {
            let from_index = match self
                .operators
                .iter()
                .find(|&(_, o)| o.get_name() == connection.from.name.clone())
            {
                Some((idx, op)) => match op.has_output(connection.from.output.clone()) {
                    true => idx,
                    false => {
                        return Err(ZFError::PortNotFound((
                            connection.from.name.clone(),
                            connection.from.output.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(connection.from.name.clone())),
            };

            let to_index = match self
                .operators
                .iter()
                .find(|&(_, o)| o.get_name() == connection.to.name.clone())
            {
                Some((idx, op)) => match op.has_input(connection.to.input.clone()) {
                    true => idx,
                    false => {
                        return Err(ZFError::PortNotFound((
                            connection.to.name.clone(),
                            connection.to.input.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(connection.to.name.clone())),
            };

            self.links.push((
                self.graph
                    .add_edge(*from_index, *to_index, connection.from.output.clone()),
                connection,
            ));
        } else {
            return Err(ZFError::PortIdNotMatching((
                connection.from.output,
                connection.to.input,
            )));
        }

        Ok(())
    }

    pub fn load(&mut self) -> std::io::Result<()> {
        unsafe {
            for (_, op) in &self.operators {
                match op {
                    DataFlowNode::Operator(inner) => match &inner.uri {
                        Some(uri) => {
                            let (_operator_id, runner) = load_operator(uri.clone())?;
                            let runner = Runner::Operator(runner);
                            self.operators_runners
                                .insert(inner.name.clone(), Arc::new(Mutex::new(runner)));
                        }
                        None => break,
                    },
                    DataFlowNode::Source(inner) => match &inner.uri {
                        Some(uri) => {
                            let (_operator_id, runner) = load_source(uri.clone())?;
                            let runner = Runner::Source(runner);
                            self.operators_runners
                                .insert(inner.name.clone(), Arc::new(Mutex::new(runner)));
                        }
                        None => break,
                    },
                    DataFlowNode::Sink(inner) => match &inner.uri {
                        Some(uri) => {
                            let (_operator_id, runner) = load_sink(uri.clone())?;
                            let runner = Runner::Sink(runner);
                            self.operators_runners
                                .insert(inner.name.clone(), Arc::new(Mutex::new(runner)));
                        }
                        None => break,
                    },
                    DataFlowNode::ZenohConnector(_) => break,
                }
            }
            Ok(())
        }
    }

    pub async fn make_connections(&mut self) {
        // Connects the operators via our FIFOs

        for (idx, up_op) in &self.operators {
            let mut up_runner = self
                .operators_runners
                .get(&up_op.get_name())
                .unwrap()
                .lock()
                .await;

            log::debug!("Creating links for:\n\t< {:?} > Operator: {:?}", idx, up_op);

            if self.graph.contains_node(*idx) {
                let mut downstreams = self
                    .graph
                    .neighbors_directed(*idx, Direction::Outgoing)
                    .detach();
                while let Some((down_edge_index, down_node_index)) = downstreams.next(&self.graph) {
                    let (_, down_link) = self
                        .links
                        .iter()
                        .find(|&(edge_index, _)| *edge_index == down_edge_index)
                        .unwrap();
                    let link_id = down_link.from.output.clone();

                    let down_op = match self
                        .operators
                        .iter()
                        .find(|&(idx, _)| *idx == down_node_index)
                    {
                        Some((_, op)) => op,
                        None => panic!("To not found"),
                    };

                    let mut down_runner = self
                        .operators_runners
                        .get(&down_op.get_name())
                        .unwrap()
                        .lock()
                        .await;

                    log::debug!(
                        "\t Creating link between {:?} -> {:?}: {:?}",
                        idx,
                        down_node_index,
                        link_id
                    );
                    let (tx, rx) = link::<ZFMessage>(1024, link_id);

                    up_runner.add_output(tx);
                    down_runner.add_input(rx);
                }
            }
        }
    }

    pub fn get_runner(&self, operator_id: &ZFOperatorId) -> Arc<Mutex<Runner>> {
        self.operators_runners.get(operator_id).unwrap().clone()
    }

    pub fn get_runners(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];

        for runner in self.operators_runners.values() {
            runners.push(runner.clone());
        }
        runners
    }
}
