//! MQTT 协议相关的数据类型定义。

use serde::{Deserialize, Serialize};

/// 一条 MQTT 消息的抽象表示（入站或出站通用）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttMessage {
    /// Topic
    pub topic: String,
    /// 原始负载字节
    pub payload: Vec<u8>,
    /// QoS 等级（0 / 1 / 2）
    pub qos: u8,
    /// 是否为保留消息
    pub retain: bool,
}

impl MqttMessage {
    /// 创建一条最简 MQTT 消息（默认 QoS 0、非保留）。
    pub fn new(topic: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            topic: topic.into(),
            payload,
            qos: 0,
            retain: false,
        }
    }

    /// 将一个可序列化的值编码为 JSON 并封装为 MQTT 消息。
    pub fn from_json<T: Serialize>(topic: impl Into<String>, value: &T) -> anyhow::Result<Self> {
        let payload = serde_json::to_vec(value)?;
        Ok(Self::new(topic, payload))
    }

    /// 以 UTF-8 字符串形式读取 payload（非法字节会被替换为 �）。
    pub fn payload_as_text(&self) -> String {
        String::from_utf8_lossy(&self.payload).into_owned()
    }
}

/// MQTT 客户端连接配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    /// Broker 地址
    pub broker: String,
    /// Broker 端口（通常 1883）
    pub port: u16,
    /// 客户端 ID
    pub client_id: String,
    /// 可选用户名
    pub username: Option<String>,
    /// 可选密码
    pub password: Option<String>,
    /// 需要订阅的 topic 列表（支持通配符）
    pub subscribe_topics: Vec<String>,
}