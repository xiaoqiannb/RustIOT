use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{info, warn};

use crate::protocol::{BridgeMessage, WsDataFrame};

pub struct WsServer {
    bind: SocketAddr,
    rx: broadcast::Receiver<BridgeMessage>,
    senders: Arc<Mutex<Vec<broadcast::Sender<BridgeMessage>>>>,
}

impl WsServer {
    pub fn new(bind: SocketAddr, rx: broadcast::Receiver<BridgeMessage>) -> Self {
        Self {
            bind,
            rx,
            senders: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.bind).await
            .with_context(|| format!("bind websocket {}", self.bind))?;
        info!(addr = %self.bind, "WebSocket server listening");

        let senders = self.senders.clone();
        let mut rx = self.rx;

        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                let mut list = senders.lock().await;
                let mut dead = Vec::new();
                for (i, tx) in list.iter().enumerate() {
                    if tx.send(msg.clone()).is_err() {
                        dead.push(i);
                    }
                }
                for i in dead.into_iter().rev() {
                    list.remove(i);
                }
            }
        });

        loop {
            let (stream, peer) = listener.accept().await?;
            let senders = self.senders.clone();

            tokio::spawn(async move {
                match accept_async(stream).await {
                    Ok(ws) => {
                        info!(peer = %peer, "WebSocket connected");
                        let (mut write, mut read) = ws.split();
                        let (tx, mut rx) = broadcast::channel::<BridgeMessage>(256);
                        senders.lock().await.push(tx);

                        let mut send_task = tokio::spawn(async move {
                            while let Ok(msg) = rx.recv().await {
                                let json = match msg {
                                    BridgeMessage::ModbusReadResult(r) => {
                                        let frame: WsDataFrame = r.into();
                                        serde_json::to_string(&frame).ok()
                                    }
                                    BridgeMessage::ModbusWriteRequest(req) => {
                                        serde_json::to_string(&req).ok()
                                    }
                                    BridgeMessage::MqttIncoming(_) => None,
                                    BridgeMessage::Log(l) => Some(l),
                                };
                                if let Some(j) = json {
                                    if write.send(Message::Text(j.into())).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        });

                        let mut recv_task = tokio::spawn(async move {
                            while let Ok(Some(msg)) = read.next().await.transpose() {
                                if matches!(msg, Message::Close(_)) {
                                    break;
                                }
                            }
                        });

                        tokio::select! {
                            _ = (&mut send_task) => recv_task.abort(),
                            _ = (&mut recv_task) => send_task.abort(),
                        }
                        info!(peer = %peer, "WebSocket disconnected");
                    }
                    Err(e) => {
                        warn!(peer = %peer, error = %e, "WebSocket accept failed");
                    }
                }
            });
        }
    }
}