//! WebSocket 服务端模块。
//! 
//! 提供实时数据推流能力：
//! 1. 订阅广播总线中的 [`BridgeMessage`]；
//! 2. 将 Modbus 读取结果实时推送给所有连接的 WebSocket 客户端（前端/调试工具）；
//! 3. 多客户端通过一个 "发送端注册表"（`senders`）实现扇出广播。

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{info, warn};

use crate::protocol::{BridgeMessage, WsDataFrame};

/// WebSocket 服务。
pub struct WsServer {
    /// 监听地址（如 "0.0.0.0:8080"）
    bind: SocketAddr,
    /// 主总线接收端（用于驱动广播扇出）
    rx: broadcast::Receiver<BridgeMessage>,
    /// 每个已连接客户端对应的独立发送端注册表（Mutex 保护并发更新）
    senders: Arc<Mutex<Vec<broadcast::Sender<BridgeMessage>>>>,
}

impl WsServer {
    /// 创建 WebSocket 服务实例。
    pub fn new(bind: SocketAddr, rx: broadcast::Receiver<BridgeMessage>) -> Self {
        Self {
            bind,
            rx,
            senders: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 启动 WebSocket 服务。
    /// 
    /// 内部会：
    /// 1. 启动一个后台任务订阅主总线并向所有已连接客户端扇出消息；
    /// 2. 主循环接受新的 TCP 连接，升级为 WebSocket 并注册到 `senders`。
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.bind).await
            .with_context(|| format!("bind websocket {}", self.bind))?;
        info!(addr = %self.bind, "WebSocket server listening");

        let senders = self.senders.clone();
        let mut rx = self.rx;

        // 后台扇出任务：主总线 → 所有 WebSocket 客户端
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                let mut list = senders.lock().await;
                let mut dead = Vec::new();
                for (i, tx) in list.iter().enumerate() {
                    // send 失败说明对端已关闭，标记为待清理
                    if tx.send(msg.clone()).is_err() {
                        dead.push(i);
                    }
                }
                // 逆序删除避免索引错位
                for i in dead.into_iter().rev() {
                    list.remove(i);
                }
            }
        });

        // 主循环：接受新连接
        loop {
            let (stream, peer) = listener.accept().await?;
            let senders = self.senders.clone();

            tokio::spawn(async move {
                match accept_async(stream).await {
                    Ok(ws) => {
                        info!(peer = %peer, "WebSocket connected");
                        let (mut write, mut read) = ws.split();
                        // 为这个客户端创建独立的广播通道
                        let (tx, mut rx) = broadcast::channel::<BridgeMessage>(256);
                        senders.lock().await.push(tx);

                        // 发送任务：将桥接消息序列化并推给客户端
                        let mut send_task = tokio::spawn(async move {
                            while let Ok(msg) = rx.recv().await {
                                let json = match msg {
                                    // Modbus 读取结果：转为扁平的 WsDataFrame
                                    BridgeMessage::ModbusReadResult(r) => {
                                        let frame: WsDataFrame = r.into();
                                        serde_json::to_string(&frame).ok()
                                    }
                                    // 写入请求：直接 JSON 序列化
                                    BridgeMessage::ModbusWriteRequest(req) => {
                                        serde_json::to_string(&req).ok()
                                    }
                                    // MQTT 入站消息和 Log 暂不推送给前端
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

                        // 接收任务：监听客户端发来的 Close 帧
                        let mut recv_task = tokio::spawn(async move {
                            while let Ok(Some(msg)) = read.next().await.transpose() {
                                if matches!(msg, Message::Close(_)) {
                                    break;
                                }
                            }
                        });

                        // 任意一个任务结束就终止另一个，完成连接清理
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