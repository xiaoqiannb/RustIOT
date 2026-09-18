//! MQTT 客户端服务。
//! 
//! 职责：
//! 1. 订阅配置中的 topic，接收云端下行消息；
//! 2. 监听广播总线中的 [`BridgeMessage`]，将 Modbus 读取结果发布到云端。

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::protocol::{BridgeMessage, MqttConfig, MqttMessage};

/// MQTT 服务。
/// 
/// 内部同时持有广播发送端和接收端：
/// - `tx`：将 MQTT 入站消息注入总线（供 WebSocket 等消费）
/// - `rx`：从总线接收桥接消息并决定是否发布到云端
pub struct MqttService {
    config: MqttConfig,
    /// 向总线注入 MQTT 入站消息
    tx: broadcast::Sender<BridgeMessage>,
    /// 从总线读取需要发布的消息
    rx: broadcast::Receiver<BridgeMessage>,
}

impl MqttService {
    /// 创建 MQTT 服务实例。
    pub fn new(
        config: MqttConfig,
        tx: broadcast::Sender<BridgeMessage>,
        rx: broadcast::Receiver<BridgeMessage>,
    ) -> Self {
        Self { config, tx, rx }
    }

    /// 启动 MQTT 服务。
    /// 
    /// 会同时启动两个异步任务：
    /// - `handle_event_loop`：处理 rumqttc 事件循环（入站消息、连接状态等）
    /// - `publish_loop`：消费广播总线中的消息并发布到云端
    pub async fn run(self) -> Result<()> {
        // 构建 MQTT 连接参数
        let mut opts = MqttOptions::new(&self.config.client_id, &self.config.broker, self.config.port);
        opts.set_keep_alive(std::time::Duration::from_secs(30));
        // 可选的用户名/密码认证
        if let (Some(u), Some(p)) = (&self.config.username, &self.config.password) {
            opts.set_credentials(u.clone(), p.clone());
        }

        let (client, eventloop) = AsyncClient::new(opts, 10);

        // 订阅所有配置的 topic
        for t in &self.config.subscribe_topics {
            client.subscribe(t, QoS::AtLeastOnce).await
                .with_context(|| format!("subscribe {}", t))?;
        }

        info!(
            broker = %self.config.broker,
            port = self.config.port,
            "MQTT client started"
        );

        let publish_rx = self.rx;
        let tx = self.tx.clone();

        // 事件循环放到后台任务，主任务负责发布
        tokio::spawn(handle_event_loop(eventloop, tx));

        publish_loop(client, publish_rx).await
    }
}

/// 处理 rumqttc 的事件循环。
/// 
/// 只关心入站的 Publish 包，将其封装为 [`BridgeMessage::MqttIncoming`] 注入广播总线。
/// 连接错误时等待 2 秒后继续，rumqttc 内部会自动重连。
async fn handle_event_loop(mut eventloop: EventLoop, tx: broadcast::Sender<BridgeMessage>) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                let msg = MqttMessage {
                    topic: p.topic.clone(),
                    payload: p.payload.to_vec(),
                    qos: p.qos as u8,
                    retain: p.retain,
                };
                info!(topic = %msg.topic, bytes = msg.payload.len(), "MQTT incoming");
                let _ = tx.send(BridgeMessage::MqttIncoming(msg));
            }
            // 忽略其他入站包（SUBACK、PINGRESP 等）
            Ok(Event::Incoming(_)) => {}
            // 忽略出站事件
            Ok(Event::Outgoing(_)) => {}
            Err(e) => {
                error!(error = %e, "MQTT eventloop error, reconnecting");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

/// 发布循环：从广播总线接收消息并根据类型发布到 MQTT broker。
async fn publish_loop(client: AsyncClient, mut rx: broadcast::Receiver<BridgeMessage>) -> Result<()> {
    loop {
        match rx.recv().await {
            // Modbus 读取结果 → 发布到 modbus/{slave}/{name}
            Ok(BridgeMessage::ModbusReadResult(r)) => {
                let topic = format!("modbus/{}/{}", r.slave, r.mapping_name);
                let payload = serde_json::to_vec(&r)
                    .unwrap_or_else(|_| Vec::new());
                if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, false, payload).await {
                    warn!(error = %e, topic = %topic, "MQTT publish failed");
                } else {
                    info!(topic = %topic, "published Modbus result");
                }
            }
            // Modbus 写入请求 → 转发到云端方便日志追踪
            Ok(BridgeMessage::ModbusWriteRequest(req)) => {
                let topic = format!("modbus/{}/write/{}", req.slave, req.mapping_name);
                let payload = serde_json::to_vec(&req).unwrap_or_default();
                let _ = client.publish(&topic, QoS::AtLeastOnce, false, payload).await;
            }
            // 这些类型不需要发布到云端
            Ok(BridgeMessage::MqttIncoming(_)) => {}
            Ok(BridgeMessage::Log(_)) => {}
            // 接收端落后了 N 条，记录告警继续
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(lagged = n, "MQTT publish channel lagged");
            }
            // 发送端全部关闭，退出循环
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    Ok(())
}

/// 辅助函数：尝试将 MQTT payload 解析为任意 JSON 值。
pub fn parse_mqtt_to_value(payload: &[u8]) -> Option<Value> {
    serde_json::from_slice::<Value>(payload).ok()
}