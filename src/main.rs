mod modbus_client;
mod mqtt_client;
mod protocol;
mod ws_server;

use anyhow::Result;
use tokio::sync::broadcast;
use tracing_subscriber::EnvFilter;

use crate::modbus_client::{ModbusPoller, ModbusPollerConfig};
use crate::mqtt_client::MqttService;
use crate::protocol::{BridgeMessage, ModbusReadKind, ModbusRegisterMapping, MqttConfig};
use crate::ws_server::WsServer;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let (tx, _) = broadcast::channel::<BridgeMessage>(512);

    let modbus_config = ModbusPollerConfig {
        host: "127.0.0.1".into(),
        port: 502,
        poll_interval_ms: 1000,
        mappings: vec![
            ModbusRegisterMapping {
                name: "temperature".into(),
                slave: 1,
                address: 0x0000,
                len: 2,
                data_type: ModbusReadKind::HoldingRegister,
                topic: "modbus/1/temperature".into(),
            },
            ModbusRegisterMapping {
                name: "pressure".into(),
                slave: 1,
                address: 0x0002,
                len: 2,
                data_type: ModbusReadKind::HoldingRegister,
                topic: "modbus/1/pressure".into(),
            },
        ],
    };

    let mqtt_config = MqttConfig {
        broker: "127.0.0.1".into(),
        port: 1883,
        client_id: "rustiot-edge-01".into(),
        username: None,
        password: None,
        subscribe_topics: vec!["modbus/cmd/#".into()],
    };

    let ws_bind = "0.0.0.0:8080".parse().unwrap();

    let tx_modbus = tx.clone();
    let tx_mqtt = tx.clone();
    let rx_mqtt = tx.subscribe();
    let rx_ws = tx.subscribe();

    let poller = ModbusPoller::new(modbus_config, tx_modbus);
    let mqtt = MqttService::new(mqtt_config, tx_mqtt, rx_mqtt);
    let ws = WsServer::new(ws_bind, rx_ws);

    let t1 = tokio::spawn(async move { poller.run().await });
    let t2 = tokio::spawn(async move { mqtt.run().await });
    let t3 = tokio::spawn(async move { ws.run().await });

    tokio::select! {
        r = t1 => { tracing::error!("modbus exit: {:?}", r); }
        r = t2 => { tracing::error!("mqtt exit: {:?}", r); }
        r = t3 => { tracing::error!("ws exit: {:?}", r); }
    }

    Ok(())
}