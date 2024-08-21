// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, BINARY_PATH};
use crate::{benchmark::BenchmarkParameters, client::Instance, settings::Settings};

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct RemoraNodeParameters;

impl Debug for RemoraNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Display for RemoraNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProtocolParameters for RemoraNodeParameters {}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct RemoraClientParameters;

impl Debug for RemoraClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Display for RemoraClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProtocolParameters for RemoraClientParameters {}

pub struct RemoraProtocol {
    working_dir: PathBuf,
}

impl ProtocolCommands for RemoraProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        todo!()
    }

    fn db_directories(&self) -> Vec<std::path::PathBuf> {
        todo!()
    }

    async fn genesis_command<'a, I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        todo!()
    }

    fn node_command<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }

    fn client_command<I>(
        &self,
        _instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }
}

impl ProtocolMetrics for RemoraProtocol {
    const BENCHMARK_DURATION: &'static str = remora::metrics::BENCHMARK_DURATION;
    const TOTAL_TRANSACTIONS: &'static str = "latency_s_count";
    const LATENCY_BUCKETS: &'static str = "latency_s";
    const LATENCY_SUM: &'static str = "latency_s_sum";
    const LATENCY_SQUARED_SUM: &'static str = remora::metrics::LATENCY_SQUARED_SUM;

    fn nodes_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }

    fn clients_metrics_path<I>(
        &self,
        instances: I,
        parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        todo!()
    }
}

impl RemoraProtocol {
    /// Make a new instance of the Remora protocol commands generator.
    pub fn new(settings: &Settings) -> Self {
        Self {
            working_dir: settings.working_dir.clone(),
        }
    }
}
