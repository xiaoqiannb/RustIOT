use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::protocol::{BridgeMessage, MqttConfig, MqttMessage};

pub struct MqttService {
    config: MqttConfig,
    tx: broadcast::Sender<BridgeMessage>,
    rx: broadcast::Receiver<BridgeMessage>,
}

impl MqttService {
    pub fn new(
        config: MqttConfig,
        tx: broadcast::Sender<BridgeMessage>,
        rx: broadcast::Receiver<BridgeMessage>,
    ) -> Self {
        Self { config, tx, rx }
    }

    pub async fn run(self) -> Result<()> {
        let mut opts = MqttOptions::new(&self.config.client_id, &self.config.broker, self.config.port);
        opts.set_keep_alive(std::time::Duration::from_secs(30));
        if let (Some(u), Some(p)) = (&self.config.username, &self.config.password) {
            opts.set_credentials(u.clone(), p.clone());
        }

        let (client, eventloop) = AsyncClient::new(opts, 10);

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

        tokio::spawn(handle_event_loop(eventloop, tx));

        publish_loop(client, publish_rx).await
    }
}

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
            Ok(Event::Incoming(_)) => {}
            Ok(Event::Outgoing(_)) => {}
            Err(e) => {
                error!(error = %e, "MQTT eventloop error, reconnecting");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

async fn publish_loop(client: AsyncClient, mut rx: broadcast::Receiver<BridgeMessage>) -> Result<()> {
    loop {
        match rx.recv().await {
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
            Ok(BridgeMessage::ModbusWriteRequest(req)) => {
                let topic = format!("modbus/{}/write/{}", req.slave, req.mapping_name);
                let payload = serde_json::to_vec(&req).unwrap_or_default();
                let _ = client.publish(&topic, QoS::AtLeastOnce, false, payload).await;
            }
            Ok(BridgeMessage::MqttIncoming(_)) => {}
            Ok(BridgeMessage::Log(_)) => {}
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(lagged = n, "MQTT publish channel lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    Ok(())
}

pub fn parse_mqtt_to_value(payload: &[u8]) -> Option<Value> {
    serde_json::from_slice::<Value>(payload).ok()
}