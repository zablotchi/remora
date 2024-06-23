use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use super::types::*;

#[async_trait]
pub trait Agent {
    fn new(
        id: UniqueId,
        in_channel: mpsc::Receiver<NetworkMessage>,
        out_channel: mpsc::Sender<NetworkMessage>,
        network_config: GlobalConfig,
        metrics: Arc<Metrics>,
    ) -> Self;

    async fn run(&mut self);
}