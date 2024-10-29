// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    net::{IpAddr, Ipv4Addr},
    ops::Deref,
    path::PathBuf,
};

use remora::config::{ValidatorConfig, ValidatorParameters};
use serde::{Deserialize, Serialize};

use super::{ProtocolCommands, ProtocolMetrics, ProtocolParameters, BINARY_PATH};
use crate::{benchmark::BenchmarkParameters, client::Instance, settings::Settings};

#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(transparent)]
pub struct RemoraNodeParameters(ValidatorParameters);

impl Deref for RemoraNodeParameters {
    type Target = ValidatorParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for RemoraNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}",
            self.collocated_pre_executors.primary, self.collocated_pre_executors.proxy
        )
    }
}

impl Display for RemoraNodeParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} pre-executor(s) per proxy and {} on the primary",
            self.collocated_pre_executors.proxy, self.collocated_pre_executors.primary
        )
    }
}

impl ProtocolParameters for RemoraNodeParameters {}

#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(transparent)]
pub struct RemoraClientParameters(remora::config::BenchmarkParameters);

impl Deref for RemoraClientParameters {
    type Target = remora::config::BenchmarkParameters;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for RemoraClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.workload)
    }
}

impl Display for RemoraClientParameters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Workload: {:?}", self.workload)
    }
}

impl ProtocolParameters for RemoraClientParameters {}

pub struct RemoraProtocol {
    working_dir: PathBuf,
}

impl ProtocolCommands for RemoraProtocol {
    fn protocol_dependencies(&self) -> Vec<&'static str> {
        vec![
            // Install typical sui dependencies.
            "sudo apt-get -y install curl git-all clang cmake gcc libssl-dev pkg-config libclang-dev",
            // This dependency is missing from the Sui docs.
            "sudo apt-get -y install libpq-dev",
        ]
    }

    fn db_directories(&self) -> Vec<PathBuf> {
        vec![]
    }

    async fn genesis_command<'a, I>(
        &self,
        mut instances: I,
        parameters: &BenchmarkParameters,
    ) -> String
    where
        I: Iterator<Item = &'a Instance>,
    {
        // Build the validator configuration.
        let first_node = instances.next().unwrap();
        let mut proxy_server_address = remora::config::default_primary_address_for_proxies();
        proxy_server_address.set_ip(IpAddr::V4(first_node.main_ip));
        let mut client_server_address = remora::config::default_primary_address_for_clients();
        client_server_address.set_ip(IpAddr::V4(first_node.main_ip));

        let validator_config = ValidatorConfig {
            proxy_server_address,
            client_server_address,
            metrics_address: remora::config::default_metrics_address(),
            validator_parameters: parameters.node_parameters.deref().clone(),
        };

        let validator_config_string = serde_yaml::to_string(&validator_config).unwrap();
        let validator_config_path = self.working_dir.join("validator_config.yml");
        let upload_validator_config = format!(
            "echo -e '{validator_config_string}' > {}",
            validator_config_path.display()
        );

        // Build the benchmark configuration.
        let benchmark_config = &parameters.client_parameters;
        let benchmark_config_string = serde_yaml::to_string(benchmark_config).unwrap();
        let benchmark_config_path = self.working_dir.join("benchmark_config.yml");
        let upload_benchmark_config = format!(
            "echo -e '{benchmark_config_string}' > {}",
            benchmark_config_path.display()
        );

        let log = "export RUST_LOG=info";
        [
            "source $HOME/.cargo/env",
            log,
            &upload_validator_config,
            &upload_benchmark_config,
        ]
        .join(" && ")
    }

    fn node_command<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        let validator_config_path = self.working_dir.join("validator_config.yml");
        let benchmark_config_path = self.working_dir.join("benchmark_config.yml");
        let binding_address = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

        instances
            .into_iter()
            .enumerate()
            .map(|(i, instance)| {
                let mut metrics_address = remora::config::default_metrics_address();
                metrics_address.set_ip(IpAddr::V4(instance.main_ip));

                let mut run = vec![
                    format!("./{BINARY_PATH}/remora"),
                    format!("--validator-config {}", validator_config_path.display()),
                    format!("--benchmark-config {}", benchmark_config_path.display()),
                    format!("--binding-address {binding_address}"),
                ];

                if i == 0 {
                    run.push("primary".to_string());
                } else {
                    run.push(format!("proxy {i}"));
                };

                let log = "export RUST_LOG=info";
                let string = run.join(" ");
                let command = ["source $HOME/.cargo/env", log, &string].join(" && ");
                (instance, command)
            })
            .collect()
    }

    fn client_command<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        let validator_config_path = self.working_dir.join("validator_config.yml");
        let benchmark_config_path = self.working_dir.join("benchmark_config.yml");

        let mut metrics_address = remora::load_generator::default_metrics_address();
        metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        instances
            .into_iter()
            .map(|instance| {
                let run = [
                    format!("./{BINARY_PATH}/load_generator"),
                    format!("--validator-config {}", validator_config_path.display()),
                    format!("--benchmark-config {}", benchmark_config_path.display()),
                    format!("--metrics-address {metrics_address}"),
                ];

                let log = "export RUST_LOG=info";
                let string = run.join(" ");
                let command = ["source $HOME/.cargo/env", log, &string].join(" && ");
                (instance, command)
            })
            .collect()
    }

    fn gen_log_command<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        // call log_generation
        let validator_config_path = self.working_dir.join("validator_config.yml");
        let benchmark_config_path = self.working_dir.join("benchmark_config.yml");

        let mut metrics_address = remora::load_generator::default_metrics_address();
        metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        instances
            .into_iter()
            .map(|instance| {
                let run = [
                    format!("./{BINARY_PATH}/log_generator"),
                    format!("--validator-config {}", validator_config_path.display()),
                    format!("--benchmark-config {}", benchmark_config_path.display()),
                    format!("--metrics-address {metrics_address}"),
                ];

                let log = "export RUST_LOG=info";
                let string = run.join(" ");
                let command = ["source $HOME/.cargo/env", log, &string].join(" && ");
                (instance, command)
            })
            .collect()
    }

    fn get_log_path(&self, parameters: &BenchmarkParameters) -> String {
        let log_dir = remora::executor::sui::LOG_DIR;
        let log_dir_path: PathBuf = log_dir.into();

        // TODO: copy from remora/src/exeuctor/sui.rs
        // Create a separate dir to indicate workload
        // the path is in the format of <log_dir>/<workload>/*.dat
        // where <workload> is denoted by {txn_cnt}-{contention_level}
        let mut cont_level: usize = 1;
        let txn_cnt: u64 =
            parameters.client_parameters.load * parameters.client_parameters.duration.as_secs();
        if let remora::config::WorkloadType::SharedObjects { txs_per_counter } =
            parameters.client_parameters.workload
        {
            cont_level = txs_per_counter;
        }
        let workload_path: PathBuf = log_dir_path.join(format!("{}-{}", txn_cnt, cont_level));
        workload_path.to_str().unwrap().to_string()
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
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .map(|instance| {
                let mut metrics_address = remora::config::default_metrics_address();
                metrics_address.set_ip(IpAddr::V4(instance.main_ip));
                let metrics_path = format!("{metrics_address}/metrics");
                (instance, metrics_path)
            })
            .collect()
    }

    fn clients_metrics_path<I>(
        &self,
        instances: I,
        _parameters: &BenchmarkParameters,
    ) -> Vec<(Instance, String)>
    where
        I: IntoIterator<Item = Instance>,
    {
        instances
            .into_iter()
            .map(|instance| {
                let mut metrics_address = remora::load_generator::default_metrics_address();
                metrics_address.set_ip(IpAddr::V4(instance.main_ip));
                let metrics_path = format!("{metrics_address}/metrics");
                (instance, metrics_path)
            })
            .collect()
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
