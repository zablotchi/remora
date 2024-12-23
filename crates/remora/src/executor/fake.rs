// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    future::Future,
    marker::PhantomData,
    sync::{Arc, RwLock},
    time::Duration,
};

use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber, SuiAddress},
    committee::EpochId,
    digests::{TransactionDigest, TransactionEventsDigest},
    effects::{InputSharedObject, ObjectChange, TransactionEffectsAPI},
    execution_status::ExecutionStatus,
    gas::GasCostSummary,
    object::{MoveObject, Object, Owner},
    transaction::InputObjectKind,
};
use tokio::time::Instant;

use super::api::{
    ExecutableTransaction, ExecutionResultsAndEffects, Executor, StateStore,
    TransactionWithTimestamp,
};

/// A fake owned object for testing.
pub fn fake_owned_object(version: u64) -> Object {
    let id = ObjectID::random();
    let object_version = SequenceNumber::from_u64(version);
    let owner = SuiAddress::random_for_testing_only();
    Object::with_id_owner_version_for_testing(id, object_version, owner)
}

/// A fake shared object for testing.
pub fn fake_shared_object(version: u64) -> Object {
    let id = ObjectID::random();
    let object_version = SequenceNumber::from_u64(version);
    let obj = MoveObject::new_gas_coin(object_version, id, 10);
    let owner = Owner::Shared {
        initial_shared_version: obj.version(),
    };
    Object::new_move(obj, owner, TransactionDigest::genesis_marker())
}

#[derive(Clone)]
pub struct FakeTransaction {
    pub digest: TransactionDigest,
    inputs: Vec<InputObjectKind>,
}

impl FakeTransaction {
    pub fn new(inputs: Vec<InputObjectKind>) -> Self {
        Self {
            digest: TransactionDigest::random(),
            inputs,
        }
    }

    pub fn from_store(
        store: &FakeObjectStore<FakeTransactionEffects>,
        inputs: Vec<ObjectID>,
    ) -> Self {
        let inputs = inputs
            .iter()
            .map(|id| {
                let object = store
                    .read_object(id)
                    .expect("Failed to access store")
                    .expect(&format!("Unknown object {id}"));
                if object.is_shared() {
                    InputObjectKind::SharedMoveObject {
                        id: object.id(),
                        initial_shared_version: object.version(),
                        mutable: true,
                    }
                } else {
                    InputObjectKind::ImmOrOwnedMoveObject(object.compute_object_reference())
                }
            })
            .collect();
        Self::new(inputs)
    }
}

impl ExecutableTransaction for FakeTransaction {
    fn digest(&self) -> &TransactionDigest {
        &self.digest
    }

    fn input_objects(&self) -> Vec<InputObjectKind> {
        self.inputs.clone()
    }
}

#[derive(Debug, Clone)]
pub struct FakeTransactionEffects {
    transaction_digest: TransactionDigest,
    modified_at_versions: Vec<(ObjectID, SequenceNumber)>,
}

/// TODO: We may get away with using the TransactionEffectAPI trait.
impl TransactionEffectsAPI for FakeTransactionEffects {
    fn status(&self) -> &ExecutionStatus {
        &ExecutionStatus::Success
    }

    fn into_status(self) -> ExecutionStatus {
        unreachable!()
    }

    fn executed_epoch(&self) -> EpochId {
        unreachable!()
    }

    fn modified_at_versions(&self) -> Vec<(ObjectID, SequenceNumber)> {
        self.modified_at_versions.clone()
    }

    fn lamport_version(&self) -> SequenceNumber {
        unreachable!()
    }

    fn old_object_metadata(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn input_shared_objects(&self) -> Vec<InputSharedObject> {
        unreachable!()
    }

    fn created(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn mutated(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn unwrapped(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn deleted(&self) -> Vec<ObjectRef> {
        unreachable!()
    }

    fn unwrapped_then_deleted(&self) -> Vec<ObjectRef> {
        unreachable!()
    }

    fn wrapped(&self) -> Vec<ObjectRef> {
        unreachable!()
    }

    fn object_changes(&self) -> Vec<ObjectChange> {
        unreachable!()
    }

    fn gas_object(&self) -> (ObjectRef, Owner) {
        unreachable!()
    }

    fn events_digest(&self) -> Option<&TransactionEventsDigest> {
        unreachable!()
    }

    fn dependencies(&self) -> &[TransactionDigest] {
        unreachable!()
    }

    fn transaction_digest(&self) -> &TransactionDigest {
        &self.transaction_digest
    }

    fn gas_cost_summary(&self) -> &GasCostSummary {
        unreachable!()
    }

    fn status_mut_for_testing(&mut self) -> &mut ExecutionStatus {
        unreachable!()
    }

    fn gas_cost_summary_mut_for_testing(&mut self) -> &mut GasCostSummary {
        unreachable!()
    }

    fn transaction_digest_mut_for_testing(&mut self) -> &mut TransactionDigest {
        unreachable!()
    }

    fn dependencies_mut_for_testing(&mut self) -> &mut Vec<TransactionDigest> {
        unreachable!()
    }

    fn unsafe_add_input_shared_object_for_testing(&mut self, _kind: InputSharedObject) {
        unreachable!()
    }

    fn unsafe_add_deleted_live_object_for_testing(&mut self, _obj_ref: ObjectRef) {
        unreachable!()
    }

    fn unsafe_add_object_tombstone_for_testing(&mut self, _obj_ref: ObjectRef) {
        unreachable!()
    }
}

pub struct FakeObjectStore<FakeTransactionEffects> {
    _phantom: PhantomData<FakeTransactionEffects>,
    objects: Arc<RwLock<BTreeMap<ObjectID, Object>>>,
}

impl FakeObjectStore<FakeTransactionEffects> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
            objects: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn write_object(&self, object: Object) {
        let mut objects = self.objects.write().unwrap();
        objects.insert(object.id(), object);
    }
}

impl<FakeTransactionEffects> StateStore<FakeTransactionEffects>
    for FakeObjectStore<FakeTransactionEffects>
{
    fn commit_objects(
        &self,
        _updates: FakeTransactionEffects,
        new_state: BTreeMap<ObjectID, Object>,
    ) {
        let mut objects = self.objects.write().unwrap();
        for (object_id, object) in new_state {
            objects.insert(object_id, object);
        }
    }

    fn read_object(
        &self,
        id: &ObjectID,
    ) -> Result<Option<Object>, sui_types::storage::error::Error> {
        let objects = self.objects.read().unwrap();
        Ok(objects.get(id).cloned())
    }
}

pub struct FakeExecutionContext {
    /// The duration of the transaction execution (in number of spins).
    pub execution_spins: u64,
    /// The duration of the checks that are done before executing the transaction (in number of spins).
    pub checks_spins: u64,
}

impl FakeExecutionContext {
    pub fn new(execution_duration: Duration, checks_duration: Duration) -> Self {
        Self {
            execution_spins: Self::calibrate(execution_duration),
            checks_spins: Self::calibrate(checks_duration),
        }
    }

    /// Simulate CPU-bound work by running a computation for the duration of cpu_time
    fn calibrated_work(iterations: u64) {
        for _ in 0..iterations {
            std::hint::spin_loop();
        }
    }

    /// Calibrate the fake executor to run for the target_duration
    fn calibrate(target_duration: Duration) -> u64 {
        let mut iterations = 1_000_000;
        let mut step_size = iterations;

        loop {
            let start = Instant::now();

            Self::calibrated_work(iterations);

            let elapsed = start.elapsed();

            match elapsed.cmp(&target_duration) {
                Ordering::Greater => {
                    // Use a binary reduction approach to converge faster
                    step_size = (step_size as f64 * 0.5) as u64;
                    if step_size == 0 {
                        break; // Stop when step size is too small to adjust further
                    }
                    iterations -= step_size;
                }
                Ordering::Less => {
                    // Increase faster initially, then adjust slowly
                    step_size = (step_size as f64 * 0.5) as u64;
                    iterations += step_size;
                }
                Ordering::Equal => break,
            }

            // If the duration is very close to target, break early
            if (elapsed.as_secs_f64() - target_duration.as_secs_f64()).abs() < 0.000_001 {
                break;
            }
        }
        iterations
    }
}

pub struct FakeExecutor {
    execution_context: Arc<FakeExecutionContext>,
}

impl FakeExecutor {
    pub fn new(execution_context: FakeExecutionContext) -> Self {
        Self {
            execution_context: Arc::new(execution_context),
        }
    }

    pub fn update_object(input: Object) -> Object {
        let id = ObjectID::random();
        let version = SequenceNumber::from_u64(input.version().value() + 1);
        let obj = MoveObject::new_gas_coin(version, id, 10);
        let owner = if input.is_shared() {
            Owner::Shared {
                initial_shared_version: version,
            }
        } else {
            input
                .as_inner()
                .get_owner_and_id()
                .expect("Should be single owner")
                .0
        };
        Object::new_move(obj, owner, TransactionDigest::genesis_marker())
    }
}

impl Executor for FakeExecutor {
    type Transaction = FakeTransaction;
    type ExecutionResults = FakeTransactionEffects;
    type Store = FakeObjectStore<FakeTransactionEffects>;
    type ExecutionContext = FakeExecutionContext;

    fn context(&self) -> Arc<FakeExecutionContext> {
        self.execution_context.clone()
    }

    fn execute(
        ctx: Arc<FakeExecutionContext>,
        store: Arc<FakeObjectStore<FakeTransactionEffects>>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::ExecutionResults>> + Send {
        // Simulate execution
        for _ in 0..ctx.execution_spins {
            std::hint::spin_loop();
        }

        let mut modified_at_versions = Vec::new();
        let mut new_state = BTreeMap::new();
        for reference in &transaction.inputs {
            // Read input objects.
            let id = reference.object_id();
            let input_object = store
                .read_object(&id)
                .expect("Failed to access store")
                .expect(&format!("Unknown object {id}"));
            modified_at_versions.push((id, input_object.version()));

            // Create output objects.
            let output_object = Self::update_object(input_object);
            new_state.insert(id, output_object);
        }

        // Update the store.
        let updates = FakeTransactionEffects {
            transaction_digest: transaction.digest().clone(),
            modified_at_versions,
        };
        store.commit_objects(updates.clone(), new_state.clone());

        async move { ExecutionResultsAndEffects::new(updates, new_state) }
    }

    fn pre_execute_check(
        ctx: Arc<FakeExecutionContext>,
        _store: Arc<Self::Store>,
        _transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        for _ in 0..ctx.checks_spins {
            std::hint::spin_loop();
        }
        true
    }

    // busy loop to simulate transaction verification
    fn verify_transaction(
        ctx: Arc<FakeExecutionContext>,
        _transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        for _ in 0..ctx.checks_spins {
            std::hint::spin_loop();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use sui_types::transaction::InputObjectKind;
    use tokio::time::Instant;

    use crate::executor::{
        api::{Executor, TransactionWithTimestamp},
        fake::{
            fake_owned_object, fake_shared_object, FakeExecutionContext, FakeExecutor,
            FakeObjectStore, FakeTransaction,
        },
    };

    #[tokio::test]
    async fn check_fake_transaction() {
        let store = Arc::new(FakeObjectStore::new());
        let execution_duration = Duration::from_millis(3);
        let checks_duration = Duration::from_millis(1);
        let execution_context = FakeExecutionContext::new(execution_duration, checks_duration);
        let executor = FakeExecutor::new(execution_context);
        let ctx = executor.context();

        let inputs: Vec<_> = (0..2)
            .map(|_| {
                let object = fake_owned_object(0);
                let reference = object.compute_object_reference();
                InputObjectKind::ImmOrOwnedMoveObject(reference)
            })
            .collect();
        let transaction = FakeTransaction::new(inputs);
        let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

        let start = Instant::now();
        let result = FakeExecutor::pre_execute_check(ctx, store, &transaction_with_timestamp);
        let duration = start.elapsed();

        assert!(result);
        assert!(duration >= checks_duration);
    }

    #[tokio::test]
    async fn execute_fake_owned_object_transaction() {
        let store = Arc::new(FakeObjectStore::new());
        let execution_duration = Duration::from_millis(3);
        let checks_duration = Duration::from_millis(1);
        let execution_context = FakeExecutionContext::new(execution_duration, checks_duration);
        let executor = FakeExecutor::new(execution_context);
        let ctx = executor.context();

        let inputs: Vec<_> = (0..2)
            .map(|_| {
                let object = fake_owned_object(0);
                let id = object.id();
                store.write_object(object);
                id
            })
            .collect();
        let transaction = FakeTransaction::from_store(&store, inputs);
        let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

        let start = Instant::now();
        let result = FakeExecutor::execute(ctx, store, &transaction_with_timestamp).await;
        let duration = start.elapsed();

        assert!(result.success());
        assert!(duration >= execution_duration);
    }

    #[tokio::test]
    async fn execute_fake_shared_object_transaction() {
        let store = Arc::new(FakeObjectStore::new());
        let execution_duration = Duration::from_millis(3);
        let checks_duration = Duration::from_millis(1);
        let execution_context = FakeExecutionContext::new(execution_duration, checks_duration);
        let executor = FakeExecutor::new(execution_context);
        let ctx = executor.context();

        let inputs: Vec<_> = (0..2)
            .map(|_| {
                let object = fake_shared_object(0);
                let id = object.id();
                store.write_object(object);
                id
            })
            .collect();
        let transaction = FakeTransaction::from_store(&store, inputs);
        let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

        let start = Instant::now();
        let result = FakeExecutor::execute(ctx, store, &transaction_with_timestamp).await;
        let duration = start.elapsed();

        assert!(result.success());
        assert!(duration >= execution_duration);
    }
}
