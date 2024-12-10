// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use sui_types::transaction::InputObjectKind;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    config::ProxyMode,
    error::{NodeError, NodeResult},
    executor::{
        api::{ExecutableTransaction, ExecutionResults, Executor, Store, Transaction},
        dependency_controller::DependencyController,
    },
    metrics::Metrics,
};

pub type ProxyId = String;

/// A proxy is responsible for pre-executing transactions.
pub struct ProxyCore<E: Executor> {
    /// The ID of the proxy.
    id: ProxyId,
    /// The executor for the transactions.
    executor: E,
    /// The mode of proxy (parallel or sequential).
    mode: ProxyMode,
    /// The object store.
    store: Store<E>,
    /// The receiver for transactions.
    rx_transactions: Receiver<Transaction<E>>,
    /// The sender for transactions with results.
    tx_results: Sender<ExecutionResults<E>>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: Option<DependencyController>,
    /// The  metrics for the proxy
    metrics: Arc<Metrics>,
}

impl<E: Executor> ProxyCore<E> {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        mode: ProxyMode,
        store: Store<E>,
        rx_transactions: Receiver<Transaction<E>>,
        tx_results: Sender<ExecutionResults<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let dependency_controller = match mode {
            ProxyMode::MultiThreaded => Some(DependencyController::new()),
            ProxyMode::SingleThreaded => None,
        };

        Self {
            id,
            executor,
            mode,
            store,
            rx_transactions,
            tx_results,
            dependency_controller,
            metrics,
        }
    }

    /// Run the proxy.
    pub async fn run(&mut self) -> NodeResult<()>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        Transaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        tracing::info!("Proxy {} started", self.id);
        match self.mode {
            ProxyMode::SingleThreaded => {
                while let Some(transaction) = self.rx_transactions.recv().await {
                    self.metrics.increase_proxy_load(&self.id);
                    // check authentication first. If the tx fails authentication, no need to execute
                    let execution_result = if !E::verify_transaction(self.executor.context(), &transaction) {
                        // send an empty result if the transaction is invalid
                        ExecutionResults::<E>::new_from_failed_verification(*transaction.digest())
                    } else {
                        E::execute(self.executor.context(), self.store.clone(), &transaction).await
                    };
                    self.metrics.decrease_proxy_load(&self.id);
                    if self.tx_results.send(execution_result).await.is_err() {
                        tracing::warn!(
                            "Failed to send execution result, stopping proxy {}",
                            self.id
                        );
                        break;
                    }
                }
            }
            ProxyMode::MultiThreaded => {
                let mut task_id = 0;
                let ctx = self.executor.context();
                while let Some(transaction) = self.rx_transactions.recv().await {
                    self.metrics.increase_proxy_load(&self.id);
                    task_id += 1;

                    // filter pkg id from the obj_id
                    let obj_ids = transaction
                        .input_objects()
                        .into_iter()
                        .filter_map(|kind| {
                            match kind {
                                InputObjectKind::ImmOrOwnedMoveObject((obj_id, _, _)) => {
                                    Some(obj_id)
                                }
                                InputObjectKind::SharedMoveObject {
                                    id: obj_id,
                                    initial_shared_version: _,
                                    mutable: _,
                                } => Some(obj_id),
                                _ => None, // filter out move package
                            }
                        })
                        .collect::<Vec<_>>();

                    let (prior_handles, current_handles) = self
                        .dependency_controller
                        .as_mut()
                        .expect("DependencyController should be initialized")
                        .get_dependencies(task_id, obj_ids);

                    let store = self.store.clone();
                    let id = self.id.clone();
                    let tx_results = self.tx_results.clone();
                    let ctx = ctx.clone();
                    let metrics = self.metrics.clone();
                    tokio::spawn(async move {
                        for prior_notify in prior_handles {
                            prior_notify.notified().await;
                        }

                        // check the version ID for shared objects
                        // skip if versions don't match
                        let mut ready_to_execute = true;
                        if transaction.input_objects().iter().any(|input_object| {
                            matches!(
                                input_object,
                                InputObjectKind::SharedMoveObject {
                                    id: _,
                                    initial_shared_version: _,
                                    mutable: _,
                                }
                            )
                        }) && !E::pre_execute_check(ctx.clone(), store.clone(), &transaction)
                        {
                            ready_to_execute = false;
                        }

                        if ready_to_execute {
                            // TODO igor: perhaps we can move the authentication verification earlier, so as not to waste time and resources on scheduling a tx that will fail authentication anyway
                            let execution_result = if !E::verify_transaction(ctx.clone(), &transaction) {
                            
                                ExecutionResults::<E>::new_from_failed_verification(*transaction.digest())
                            } else {
                                E::execute(ctx, store, &transaction).await
                            };

                            for notify in current_handles {
                                notify.notify_one();
                            }

                            tx_results
                                .send(execution_result)
                                .await
                                .map_err(|_| NodeError::ShuttingDown)?;
                        }
                        metrics.decrease_proxy_load(&id);
                        Ok::<_, NodeError>(())
                    });
                }
            }
        }
        Ok(())
    }

    /// Spawn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        Transaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }

    pub fn spawn_with_threads(mut self) -> std::thread::JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        Transaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let num_threads = num_cpus::get();

        // spawn the custom runtime in a dedicated thread to ensure active
        std::thread::spawn(move || {
            // Build a custom Tokio runtime with the specified number of worker threads.
            // Then block on the runtime to keep it alive and process tasks.
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_threads)
                .enable_all()
                .build()
                .expect("Failed to build runtime")
                .block_on(self.run())
        })
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use sui_types::{message_envelope::Envelope, transaction::SenderSignedData};
    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::sui::{generate_transactions, SuiExecutor, SuiTransaction},
        metrics::Metrics,
        proxy::core::{ProxyCore, ProxyMode},
    };

    async fn new_signed_txs() -> Vec<SuiTransaction> {
        let config = BenchmarkParameters::new_for_tests();
        // Send transactions to the proxy.
        let txs = generate_transactions(&config).await;
        // wrap txs in SuiTransaction
        txs.into_iter()
            .map(|tx| SuiTransaction::new_for_tests(tx))
            .collect::<Vec<_>>()
    }

    async fn new_unsigned_txs() -> Vec<SuiTransaction> {
        let config = BenchmarkParameters::new_for_tests();

        // Send transactions to the proxy.
        let transactions = generate_transactions(&config).await;

        transactions
            .into_iter()
            .map(|tx| {
                let unsigned_sender_data = SenderSignedData::new(tx.transaction_data().clone(), vec![]);
                let unsigned_tx = Envelope::new_from_data_and_sig(unsigned_sender_data, tx.auth_sig().clone());
                SuiTransaction::new_for_tests(unsigned_tx)
            })
            .collect::<Vec<_>>()
    }

    async fn pre_execute(mode: ProxyMode, signed: bool) {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let store = Arc::new(executor.create_in_memory_store());
        let metrics = Arc::new(Metrics::new_for_tests());
        let proxy_id = "0".to_string();
        let proxy = ProxyCore::new(
            proxy_id, executor, mode, store, rx_proxy, tx_results, metrics,
        );

        let transactions = if signed {
            new_signed_txs().await
        } else {
            new_unsigned_txs().await
        };

        // Send transactions to the proxy.
        // let transactions = generate_transactions(&config).await;
        for tx in transactions {
            // let transaction = SuiTransaction::new_for_tests(tx);
            tx_proxy.send(tx).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        
        if signed {
            assert!(results.authentication_success);
            assert!(results.success());
        } else {
            assert!(!results.authentication_success);
            assert!(!results.success())
        }
    }

    #[tokio::test]
    async fn test_single_threaded_proxy_signed() {
        pre_execute(ProxyMode::SingleThreaded, true).await;
    }

    #[tokio::test]
    async fn test_multi_threaded_proxy_signed() {
        pre_execute(ProxyMode::MultiThreaded, true).await;
    }

    #[tokio::test]
    async fn test_single_threaded_proxy_unsigned() {
        pre_execute(ProxyMode::SingleThreaded, false).await;
    }

    #[tokio::test]
    async fn test_multi_threaded_proxy_unsigned() {
        pre_execute(ProxyMode::MultiThreaded, false).await;
    }
}
