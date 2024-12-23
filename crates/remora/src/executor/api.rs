// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Debug, future::Future, ops::Deref, sync::Arc};

use serde::{Deserialize, Serialize};
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
    effects::TransactionEffectsAPI,
    object::Object,
    transaction::InputObjectKind,
};

/// A transaction that can be executed.
pub trait ExecutableTransaction {
    /// The digest of the transaction.
    fn digest(&self) -> &TransactionDigest;

    /// The input objects kind of the transaction.
    fn input_objects(&self) -> Vec<InputObjectKind>;

    /// The object IDs for the input objects.
    fn input_object_ids(&self) -> Vec<ObjectID> {
        self.input_objects()
            .iter()
            .map(|kind| kind.object_id())
            .collect()
    }
}

/// A transaction with a timestamp. This is used to compute performance.
#[derive(Clone, Serialize, Deserialize)]
pub struct TransactionWithTimestamp<T: ExecutableTransaction + Clone> {
    /// The transaction.
    transaction: T,
    /// The timestamp when the transaction was created.
    timestamp: f64,
}

impl<T: ExecutableTransaction + Clone> TransactionWithTimestamp<T> {
    /// Create a new transaction with a timestamp.
    pub fn new(transaction: T, timestamp: f64) -> Self {
        Self {
            transaction,
            timestamp,
        }
    }

    /// Get the timestamp of the transaction.
    pub fn timestamp(&self) -> f64 {
        self.timestamp
    }

    /// Create a new transaction with a fake timestamp for tests.
    pub fn new_for_tests(transaction: T) -> Self {
        Self {
            transaction,
            timestamp: 0.0,
        }
    }
}

impl<T: ExecutableTransaction + Clone> Deref for TransactionWithTimestamp<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionResultsAndEffects<U: Clone + Debug> {
    pub updates: Option<U>,
    pub new_state: BTreeMap<ObjectID, Object>,
    pub authentication_success: bool,
    // we need the transaction digest here because a transaction that fails authentication will not have any TransactionEffects (and therefore no way for the primary to know which transaction this is)
    pub transaction_digest: TransactionDigest,
}

impl<U: TransactionEffectsAPI + Clone + Debug> ExecutionResultsAndEffects<U> {
    pub fn new(updates: U, new_state: BTreeMap<ObjectID, Object>) -> Self {
        Self {
            updates: Some(updates.clone()),
            new_state,
            authentication_success: true,
            transaction_digest: *updates.transaction_digest(),
        }
    }

    pub fn new_from_failed_verification(transaction_digest: TransactionDigest) -> Self {
        Self {
            updates: None,
            new_state: BTreeMap::new(),
            authentication_success: false,
            transaction_digest,
        }
    }

    pub fn success(&self) -> bool {
        if self.authentication_success {
            if let Some(updates) = &self.updates {
                return updates.status().is_ok();
            }
            return false;
        }
        false
    }

    pub fn transaction_digest(&self) -> &TransactionDigest {
        &self.transaction_digest
    }

    pub fn modified_at_versions(&self) -> Vec<(ObjectID, SequenceNumber)> {
        if let Some(updates) = &self.updates {
            updates.modified_at_versions()
        } else {
            vec![]
        }
    }
}

pub trait StateStore<U> {
    fn read_object(
        &self,
        id: &ObjectID,
    ) -> Result<Option<Object>, sui_types::storage::error::Error>;
    /// Commit the objects to the store.
    fn commit_objects(&self, updates: U, new_state: BTreeMap<ObjectID, Object>);
}

/// The executor is responsible for executing transactions and generating new transactions.
pub trait Executor {
    /// The type of transaction to execute.
    type Transaction: Clone + ExecutableTransaction;
    /// The type of results from executing a transaction.
    type ExecutionResults: Clone + TransactionEffectsAPI + Debug;
    /// The type of store to store objects.
    type Store: StateStore<Self::ExecutionResults>;
    /// The benchmark context.
    type ExecutionContext;

    /// Get the context for the benchmark.
    fn context(&self) -> Arc<Self::ExecutionContext>;

    /// Execute a transaction and return the results.
    fn execute(
        ctx: Arc<Self::ExecutionContext>,
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::ExecutionResults>> + Send;

    /// Check version ID check prior to execution
    fn pre_execute_check(
        ctx: Arc<Self::ExecutionContext>,
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool;

    /// Verify the transaction authentication prior to execution
    fn verify_transaction(
        ctx: Arc<Self::ExecutionContext>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool;
}

/// Short for a transaction with a timestamp.
pub type Transaction<E> = TransactionWithTimestamp<<E as Executor>::Transaction>;

/// Short for the results of executing a transaction.
pub type ExecutionResults<E> = ExecutionResultsAndEffects<<E as Executor>::ExecutionResults>;

/// Short for the store used by the executor.
pub type Store<E> = Arc<<E as Executor>::Store>;
