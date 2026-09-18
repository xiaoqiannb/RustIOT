use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

impl MqttMessage {
    pub fn new(topic: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            topic: topic.into(),
            payload,
            qos: 0,
            retain: false,
        }
    }

    pub fn from_json<T: Serialize>(topic: impl Into<String>, value: &T) -> anyhow::Result<Self> {
        let payload = serde_json::to_vec(value)?;
        Ok(Self::new(topic, payload))
    }

    pub fn payload_as_text(&self) -> String {
        String::from_utf8_lossy(&self.payload).into_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    pub broker: String,
    pub port: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub subscribe_topics: Vec<String>,
}