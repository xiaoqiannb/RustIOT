use serde::{Deserialize, Serialize};

use super::modbus::ModbusValue;
use super::mqtt::MqttMessage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BridgeMessage {
    ModbusReadResult(ModbusReadResult),
    ModbusWriteRequest(ModbusWriteRequest),
    MqttIncoming(MqttMessage),
    Log(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModbusReadResult {
    pub mapping_name: String,
    pub timestamp_ms: i64,
    pub slave: u8,
    pub value: ModbusValue,
}

impl ModbusReadResult {
    pub fn new(mapping_name: impl Into<String>, slave: u8, value: ModbusValue) -> Self {
        Self {
            mapping_name: mapping_name.into(),
            timestamp_ms: chrono_now_ms(),
            slave,
            value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModbusWriteRequest {
    pub mapping_name: String,
    pub slave: u8,
    pub address: u16,
    pub registers: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsDataFrame {
    pub kind: String,
    pub mapping: String,
    pub slave: u8,
    pub timestamp_ms: i64,
    pub coils: Vec<bool>,
    pub registers: Vec<u16>,
}

impl From<ModbusReadResult> for WsDataFrame {
    fn from(r: ModbusReadResult) -> Self {
        Self {
            kind: "data".into(),
            mapping: r.mapping_name,
            slave: r.slave,
            timestamp_ms: r.timestamp_ms,
            coils: r.value.coils,
            registers: r.value.registers,
        }
    }
}

fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}