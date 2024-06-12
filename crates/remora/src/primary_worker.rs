use core::panic;
use dashmap::DashMap;
use move_binary_format::CompiledModule;
use move_bytecode_utils::module_cache::GetModule;
use move_vm_runtime::move_vm::MoveVM;
use prometheus::proto;
use sui_types::in_memory_storage::InMemoryStorage;
use sui_types::inner_temporary_store::InnerTemporaryStore;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use sui_adapter_latest::programmable_transactions::context;
use sui_adapter_latest::{adapter, execution_engine};
use sui_config::genesis::Genesis;
use sui_core::authority::authority_store_tables::LiveObject;
use sui_core::authority::test_authority_builder::TestAuthorityBuilder;
use sui_move_natives;
use sui_protocol_config::ProtocolConfig;
use sui_single_node_benchmark::benchmark_context::BenchmarkContext;
use sui_single_node_benchmark::mock_storage::InMemoryObjectStore;
use sui_types::executable_transaction::VerifiedExecutableTransaction;
use sui_types::transaction::{
    CertifiedTransaction, Transaction, TransactionDataAPI, VerifiedCertificate,
    VerifiedTransaction, DEFAULT_VALIDATOR_GAS_PRICE,
};
use sui_types::base_types::{ObjectID, ObjectRef, SequenceNumber};
use sui_types::digests::{ChainIdentifier, ObjectDigest, TransactionDigest};
use sui_types::effects::{TransactionEffects, TransactionEffectsAPI};
use sui_types::epoch_data::EpochData;
use sui_types::error::SuiError;
use sui_types::execution_mode;
use sui_types::message_envelope::Message;
use sui_types::messages_checkpoint::CheckpointDigest;
use sui_types::metrics::LimitsMetrics;
use sui_types::object::Object;
use sui_types::storage::{
    BackingPackageStore, ChildObjectResolver, DeleteKind, GetSharedLocks, ObjectStore, ParentSync,
    WriteKind,
};
use sui_types::sui_system_state::{get_sui_system_state, SuiSystemStateTrait};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

use super::types::*;
use crate::tx_gen_agent::{generate_benchmark_ctx_workload, WORKLOAD, COMPONENT};
use crate::tx_gen_agent::generate_benchmark_txs;
use crate::{metrics::Metrics, types::WritableObjectStore};

/*****************************************************************************************
 *                                    Primary Worker                                   *
 *****************************************************************************************/

pub struct PrimaryWorkerState {
    pub memory_store: Arc<InMemoryObjectStore>,
    pub context: Arc<BenchmarkContext>,
    pub ready_txs: DashMap<TransactionDigest, ()>,
    pub waiting_child_objs: DashMap<TransactionDigest, HashSet<ObjectID>>,
    pub received_objs: DashMap<TransactionDigest, Vec<Option<(ObjectRef, Object)>>>,
    pub received_child_objs: DashMap<TransactionDigest, Vec<Option<(ObjectRef, Object)>>>,
    pub locked_exec_count: DashMap<TransactionDigest, u8>,
    pub genesis_digest: CheckpointDigest,
    pub pending_transactions: Vec<TransactionWithEffects>,
}

impl PrimaryWorkerState
{
    pub fn new(new_store: InMemoryObjectStore, genesis_digest: CheckpointDigest, ctx: Arc<BenchmarkContext>) -> Self {
        Self {
            memory_store: Arc::new(new_store),
            context: ctx,
            ready_txs: DashMap::new(),
            waiting_child_objs: DashMap::new(),
            received_objs: DashMap::new(),
            received_child_objs: DashMap::new(),
            locked_exec_count: DashMap::new(),
            genesis_digest,
            pending_transactions: Vec::new(),
        }
    }

    async fn async_exec(
        full_tx: TransactionWithEffects,
        memstore: Arc<InMemoryObjectStore>,
        protocol_config: &ProtocolConfig,
        reference_gas_price: u64,
        ctx: Arc<BenchmarkContext>,
    ) -> TransactionEffects {
        let tx = full_tx.tx.clone();

        ctx.validator().execute_raw_transaction(tx).await
        
        //ctx.validator().execute_dry_run(tx).await
        
        // let input_objects = tx.transaction_data().input_objects().unwrap();
        // // FIXME: ugly deref
        // let objects = memstore
        //     .read_objects_for_execution(&**(ctx.validator().get_epoch_store()), &tx.key(), &input_objects)
        //     .unwrap();

        // let executable = VerifiedExecutableTransaction::new_from_certificate(
        //     VerifiedCertificate::new_unchecked(tx),
        // );
       
        // ctx.validator().get_validator()
        //     .try_execute_immediately(&executable, None, ctx.validator().get_epoch_store())
        //     .await
        //     .unwrap()
        //     .0
        // let validator = ctx.validator();
        // let (gas_status, input_objects) = sui_transaction_checks::check_certificate_input(
        //     &executable,
        //     objects,
        //     protocol_config,
        //     reference_gas_price,
        // )
        // .unwrap();
        // let (kind, signer, gas) = executable.transaction_data().execution_parts();
        // let (inner_temp_store, _, effects, _) =
        //     ctx.validator().get_epoch_store().executor().execute_transaction_to_effects(
        //         &memstore,
        //         protocol_config,
        //         ctx.validator().get_validator().metrics.limits_metrics.clone(),
        //         false,
        //         &HashSet::new(),
        //         &ctx.validator().get_epoch_store().epoch(),
        //         0,
        //         input_objects,
        //         gas,
        //         gas_status,
        //         kind,
        //         signer,
        //         *executable.digest(),
        //     );
        // assert!(effects.status().is_ok());
        // memstore.commit_objects(inner_temp_store);
        // println!("finish exec a txn");
        // effects
    }

    pub async fn run(&mut self,
        tx_count: u64,
        duration: Duration,
        in_traffic_manager: &mut mpsc::UnboundedReceiver<RemoraMessage>,
        in_consensus: &mut mpsc::UnboundedReceiver<Vec<TransactionWithEffects>>,
        out_channel: &mpsc::Sender<NetworkMessage>,
        my_id: u16,
    ) {
        loop {
            tokio::select! {
                Some(msg) = in_consensus.recv() => {
                    println!("Primary worker receive from the consensus engine");
                
                    // signal Pre to forward results
                    out_channel                
                        .send(NetworkMessage {
                            src: my_id,
                            dst: vec![2], // FIXME: forward to all PRE
                            payload: RemoraMessage::RequestPreExecResult(),
                        })
                        .await
                        .expect("PRI sending to PRE failed");

                    // receive a stream of sequenced txn from consensus until the channel is empty
                    self.pending_transactions = msg; 
                    println!("PRI recv from consensus channel done");
                },

                Some(msg) = in_traffic_manager.recv() => {

                }
            }
        }

    }
}
