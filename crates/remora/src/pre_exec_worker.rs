use core::panic;
use std::{collections::HashSet, sync::Arc};

use sui_protocol_config::ProtocolConfig;
use sui_single_node_benchmark::{
    benchmark_context::BenchmarkContext, mock_storage::InMemoryObjectStore,
};
use sui_types::{
    effects::TransactionEffectsAPI,
    executable_transaction::VerifiedExecutableTransaction,
    transaction::{TransactionDataAPI, VerifiedCertificate},
};
use tokio::{sync::mpsc, time::Duration};

use crate::metrics::Metrics;

use super::types::*;

/*****************************************************************************************
 *                                    PreExec Worker                                     *
 *****************************************************************************************/
pub const PRI_ID: u16 = 1;
pub const INTERVAL: u64 = 2;
pub const RES_BATCH_SIZE: usize = 10;

pub struct PreExecWorkerState {
    pub memory_store: Arc<InMemoryObjectStore>,
    pub context: Arc<BenchmarkContext>,
}

async fn send_pre_exec_res(
    my_id: UniqueId,
    buffer: &[TransactionWithResults],
    out_channel: &mpsc::Sender<NetworkMessage>,
) {
    out_channel
        .send(NetworkMessage {
            src: my_id,
            dst: vec![PRI_ID], // Primary worker ID
            payload: RemoraMessage::PreExecResult(buffer.to_owned()), // Clone to avoid ownership issues
        })
        .await
        .expect("sending failed");
}

impl PreExecWorkerState {
    pub fn new(new_store: InMemoryObjectStore, ctx: Arc<BenchmarkContext>) -> Self {
        Self {
            memory_store: Arc::new(new_store),
            context: ctx,
        }
    }

    async fn async_exec(
        full_tx: TransactionWithEffects,
        memstore: Arc<InMemoryObjectStore>,
        protocol_config: &ProtocolConfig,
        reference_gas_price: u64,
        ctx: Arc<BenchmarkContext>,
        in_buffer: &mpsc::UnboundedSender<TransactionWithResults>,
        metrics: Arc<Metrics>,
    ) {
        let tx = full_tx.tx.clone();

        let input_objects = tx.transaction_data().input_objects().unwrap();
        // FIXME: ugly deref
        let objects = memstore
            .read_objects_for_execution(
                &**(ctx.validator().get_epoch_store()),
                &tx.key(),
                &input_objects,
            )
            .unwrap();

        let executable = VerifiedExecutableTransaction::new_from_certificate(
            VerifiedCertificate::new_unchecked(tx),
        );

        let _validator = ctx.validator();
        let (gas_status, input_objects) = sui_transaction_checks::check_certificate_input(
            &executable,
            objects,
            protocol_config,
            reference_gas_price,
        )
        .unwrap();
        let (kind, signer, gas) = executable.transaction_data().execution_parts();
        let (inner_temp_store, _, effects, _) = ctx
            .validator()
            .get_epoch_store()
            .executor()
            .execute_transaction_to_effects(
                &memstore,
                protocol_config,
                ctx.validator()
                    .get_validator()
                    .metrics
                    .limits_metrics
                    .clone(),
                false,
                &HashSet::new(),
                &ctx.validator().get_epoch_store().epoch(),
                0,
                input_objects,
                gas,
                gas_status,
                kind,
                signer,
                *executable.digest(),
            );
        assert!(effects.status().is_ok());

        let tx_res = TransactionWithResults {
            tx_effects: effects,
            written: inner_temp_store.written.clone(),
        };

        memstore.commit_objects(inner_temp_store);

        Metrics::update_metrics(&full_tx.clone(), &metrics);

        if let Err(e) = in_buffer.send(tx_res) {
            eprintln!("PRE failed to forward in-channel exec res: {:?}", e);
        }
    }

    async fn run_timer(
        my_id: u16,
        out_buffer: &mut mpsc::UnboundedReceiver<TransactionWithResults>,
        out_channel: &mpsc::Sender<NetworkMessage>,
    ) {
        let mut timer = tokio::time::interval(Duration::from_millis(INTERVAL));
        loop {
            tokio::select! {
                _ = timer.tick() => {
                    // drain the exec results and send it out
                    let mut message_buffer = Vec::with_capacity(RES_BATCH_SIZE);

                    while let Ok(msg) = out_buffer.try_recv() {
                        message_buffer.push(msg);

                        if message_buffer.len() == RES_BATCH_SIZE {
                            send_pre_exec_res(my_id, &message_buffer, &out_channel.clone()).await;
                            message_buffer.clear();
                        }
                    }

                    if !message_buffer.is_empty() {
                        send_pre_exec_res(my_id, &message_buffer, &out_channel.clone()).await;
                    }
                }
            }
        }
    }

    pub async fn run(
        &mut self,
        _tx_count: u64,
        _duration: Duration,
        in_channel: &mut mpsc::Receiver<NetworkMessage>,
        out_channel: &mpsc::Sender<NetworkMessage>,
        my_id: u16,
        metrics: Arc<Metrics>,
    ) {
        let (in_buffer, mut out_buffer) = mpsc::unbounded_channel::<TransactionWithResults>();

        let out_channel = out_channel.clone();
        tokio::spawn(async move { Self::run_timer(my_id, &mut out_buffer, &out_channel).await });

        let mut num_txn = 0;
        loop {
            tokio::select! {
                Some(msg) = in_channel.recv() => {
                    
                    let msg = msg.payload;
                    if let RemoraMessage::ProposeExec(full_tx) = msg {
                        num_txn += 1;
                        if num_txn == 1 {
                            metrics.register_start_time();
                        }

                        let memstore = self.memory_store.clone();
                        let context = self.context.clone();
                        let in_buffer = in_buffer.clone();
                        let metrics = metrics.clone();
                        tokio::spawn(async move {
                            Self::async_exec(
                                full_tx.clone(),
                                memstore,
                                context.validator().get_epoch_store().protocol_config(),
                                context.validator().get_epoch_store().reference_gas_price(),
                                context,
                                &in_buffer,
                                metrics,
                            ).await
                        });
                    } else {
                        eprintln!("EW {} received unexpected message from: {:?}", my_id, msg);
                        panic!("unexpected message");
                    };
                }
            }
        }
    }
}
