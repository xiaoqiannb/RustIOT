use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::sync::broadcast;
use tokio_modbus::prelude::*;
use tokio_modbus::client::{self, tcp};
use tracing::{error, info, warn};

use crate::protocol::{
    BridgeMessage, ModbusDataType, ModbusReadKind, ModbusRegisterMapping, ModbusValue,
};

pub struct ModbusPollerConfig {
    pub host: String,
    pub port: u16,
    pub poll_interval_ms: u64,
    pub mappings: Vec<ModbusRegisterMapping>,
}

pub struct ModbusPoller {
    config: ModbusPollerConfig,
    tx: broadcast::Sender<BridgeMessage>,
}

impl ModbusPoller {
    pub fn new(config: ModbusPollerConfig, tx: broadcast::Sender<BridgeMessage>) -> Self {
        Self { config, tx }
    }

    pub async fn run(self) -> Result<()> {
        info!(
            host = %self.config.host,
            port = self.config.port,
            mappings = self.config.mappings.len(),
            "Modbus poller starting"
        );

        let addr: SocketAddr = format!("{}:{}", self.config.host, self.config.port).parse()
            .with_context(|| "parse modbus socket addr")?;
        let mut interval = tokio::time::interval(Duration::from_millis(self.config.poll_interval_ms));

        loop {
            interval.tick().await;

            let ctx = match tcp::connect(addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!(%addr, error = %e, "Modbus connect failed, will retry");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            if let Err(e) = self.poll_all(ctx).await {
                warn!(error = %e, "Poll round failed");
            }
        }
    }

    async fn poll_all(&self, mut ctx: client::Context) -> Result<()> {
        for mapping in &self.config.mappings {
            let value = match mapping.data_type {
                ModbusReadKind::Coil => {
                    let r = ctx.read_coils(mapping.address, mapping.len).await.unwrap_or_default();
                    ModbusValue {
                        register: mapping.address,
                        data_type: ModbusDataType::Coil,
                        coils: r,
                        registers: vec![],
                    }
                }
                ModbusReadKind::DiscreteInput => {
                    let r = ctx.read_discrete_inputs(mapping.address, mapping.len).await.unwrap_or_default();
                    ModbusValue {
                        register: mapping.address,
                        data_type: ModbusDataType::DiscreteInput,
                        coils: r,
                        registers: vec![],
                    }
                }
                ModbusReadKind::HoldingRegister => {
                    let r = ctx.read_holding_registers(mapping.address, mapping.len).await.unwrap_or_default();
                    ModbusValue {
                        register: mapping.address,
                        data_type: ModbusDataType::HoldingRegister,
                        coils: vec![],
                        registers: r,
                    }
                }
                ModbusReadKind::InputRegister => {
                    let r = ctx.read_input_registers(mapping.address, mapping.len).await.unwrap_or_default();
                    ModbusValue {
                        register: mapping.address,
                        data_type: ModbusDataType::InputRegister,
                        coils: vec![],
                        registers: r,
                    }
                }
            };

            let result = crate::protocol::ModbusReadResult::new(
                mapping.name.clone(),
                mapping.slave,
                value,
            );

            let _ = self.tx.send(BridgeMessage::ModbusReadResult(result));
        }
        Ok(())
    }
}