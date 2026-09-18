//! RustIOT - 一个基于 Rust 的边缘网关示例项目。
//! 
//! 架构：
//! ```text
//! Modbus TCP 从站  ←→  ModbusPoller  ──┐
//!                                       │ broadcast
//!                                       ▼
//!                                   BridgeMessage 总线
//!                                       │
//!                    ┌──────────────────┼──────────────────┐
//!                    ▼                  ▼                  ▼
//!              MqttService          WsServer          (其他组件)
//!                    │                  │
//!                    ▼                  ▼
//!              MQTT Broker        WebSocket 前端
//! ```

// 模块声明
mod modbus_client;
mod mqtt_client;
mod protocol;
mod ws_server;
mod web_server;

use anyhow::Result;
use tokio::sync::broadcast;
use tracing_subscriber::EnvFilter;

use crate::modbus_client::{ModbusPoller, ModbusPollerConfig};
use crate::mqtt_client::MqttService;
use crate::protocol::{BridgeMessage, ModbusReadKind, ModbusRegisterMapping, MqttConfig};
use crate::ws_server::WsServer;

/// 程序入口。
/// 
/// 启动三个并发组件：Modbus 轮询器、MQTT 服务、WebSocket 服务，
/// 通过一个全局的广播通道（`BridgeMessage`）相互解耦。
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志：默认 info 级别，可通过 RUST_LOG 环境变量覆盖
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // 全局广播通道（容量 512），作为各组件之间的消息总线
    let (tx, _) = broadcast::channel::<BridgeMessage>(512);

    // 构造 Modbus 轮询配置（示例：从站 1 的 temperature、pressure 两个点位）
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

    // MQTT 连接配置（示例：无认证，订阅 modbus/cmd/# 下行命令）
    let mqtt_config = MqttConfig {
        broker: "127.0.0.1".into(),
        port: 1883,
        client_id: "rustiot-edge-01".into(),
        username: None,
        password: None,
        subscribe_topics: vec!["modbus/cmd/#".into()],
    };

    // WebSocket 监听地址
    let ws_bind = "0.0.0.0:8080".parse().unwrap();

    // 分发广播通道：
    // - ModbusPoller 只负责发送（读取结果注入总线）
    // - MqttService 既能发送（入站 MQTT 消息）也能接收（需要发布的消息）
    // - WsServer 只负责接收（推给前端）
    let tx_modbus = tx.clone();
    let tx_mqtt = tx.clone();
    let rx_mqtt = tx.subscribe();
    let rx_ws = tx.subscribe();

    let poller = ModbusPoller::new(modbus_config, tx_modbus);
    let mqtt = MqttService::new(mqtt_config, tx_mqtt, rx_mqtt);
    let ws = WsServer::new(ws_bind, rx_ws);

    // 启动三个并发任务
    let t1 = tokio::spawn(async move { poller.run().await });
    let t2 = tokio::spawn(async move { mqtt.run().await });
    let t3 = tokio::spawn(async move { ws.run().await });

    // 任意一个任务退出都视为异常，记录日志后程序结束
    tokio::select! {
        r = t1 => { tracing::error!("modbus exit: {:?}", r); }
        r = t2 => { tracing::error!("mqtt exit: {:?}", r); }
        r = t3 => { tracing::error!("ws exit: {:?}", r); }
    }

    Ok(())
}