// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::{
        api::{ExecutableTransaction, ExecutionEffects, Executor, TransactionWithTimestamp},
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
    /// The object store.
    store: Arc<E::Store>,
    /// The receiver for transactions.
    rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
    /// The sender for transactions with results.
    tx_results: Sender<ExecutionEffects<E::StateChanges>>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: DependencyController,
    /// The  metrics for the proxy
    metrics: Arc<Metrics>,
}

impl<E: Executor> ProxyCore<E> {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        store: Arc<E::Store>,
        rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
        tx_results: Sender<ExecutionEffects<E::StateChanges>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            id,
            executor,
            store,
            rx_transactions,
            tx_results,
            dependency_controller: DependencyController::new(),
            metrics,
        }
    }

    /// Run the proxy.
    pub async fn run(&mut self) -> NodeResult<()>
    where
        E: Send + 'static,
        <E as Executor>::Store: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::StateChanges: Send,
    {
        tracing::info!("Proxy {} started", self.id);

        let mut task_id = 0;
        let ctx = self.executor.get_context();
        while let Some(transaction) = self.rx_transactions.recv().await {
            self.metrics.increase_proxy_load(&self.id);
            task_id += 1;
            let (prior_handles, current_handles) = self
                .dependency_controller
                .get_dependencies(task_id, transaction.input_object_ids());

            let store = self.store.clone();
            let id = self.id.clone();
            let tx_results = self.tx_results.clone();
            let ctx = ctx.clone();
            let metrics = self.metrics.clone();
            tokio::spawn(async move {
                for prior_notify in prior_handles {
                    prior_notify.notified().await;
                }

                let execution_result = E::execute(ctx, store, &transaction).await;

                for notify in current_handles {
                    notify.notify_one();
                }

                tx_results
                    .send(execution_result)
                    .await
                    .map_err(|_| NodeError::ShuttingDown)?;
                metrics.decrease_proxy_load(&id);
                Ok::<_, NodeError>(())
            });
        }
        Ok(())
    }

    /// Spawn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        <E as Executor>::Store: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::StateChanges: Send,
    {
        tokio::spawn(async move { self.run().await })
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::sui::{generate_transactions, SuiExecutor, SuiTransactionWithTimestamp},
        metrics::Metrics,
        proxy::core::ProxyCore,
    };

    #[tokio::test]
    async fn pre_execute() {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let store = Arc::new(executor.create_in_memory_store());
        let metrics = Arc::new(Metrics::new_for_tests());
        let proxy_id = "0".to_string();
        let proxy = ProxyCore::new(proxy_id, executor, store, rx_proxy, tx_results, metrics);

        // Send transactions to the proxy.
        let transactions = generate_transactions(&config).await;
        for tx in transactions {
            let transaction = SuiTransactionWithTimestamp::new_for_tests(tx);
            tx_proxy.send(transaction).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        assert!(results.success());
    }
}
