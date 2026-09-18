//! 组件间桥接消息定义。
//! 
//! RustIOT 内部使用 `tokio::sync::broadcast` 作为消息总线，
//! 各组件（Modbus 轮询器、MQTT 客户端、WebSocket 服务）
//! 通过 [`BridgeMessage`] 相互解耦、协作。

use serde::{Deserialize, Serialize};

use super::modbus::ModbusValue;
use super::mqtt::MqttMessage;

/// 广播通道上流转的统一消息类型。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BridgeMessage {
    /// Modbus 读取结果（上报方向：Modbus → MQTT / WebSocket）
    ModbusReadResult(ModbusReadResult),
    /// Modbus 写入请求（下发方向：MQTT / WebSocket → Modbus）
    ModbusWriteRequest(ModbusWriteRequest),
    /// 收到的 MQTT 消息
    MqttIncoming(MqttMessage),
    /// 日志/提示文本（仅 WebSocket 端消费）
    Log(String),
}

/// 一次 Modbus 读取的完整结果，附带时间戳和点位名。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModbusReadResult {
    /// 点位名称（对应 [`ModbusRegisterMapping::name`]）
    pub mapping_name: String,
    /// 采集时间戳（毫秒，UNIX epoch）
    pub timestamp_ms: i64,
    /// 从站地址
    pub slave: u8,
    /// 读取到的值
    pub value: ModbusValue,
}

impl ModbusReadResult {
    /// 构造函数，时间戳自动填充为当前时间。
    pub fn new(mapping_name: impl Into<String>, slave: u8, value: ModbusValue) -> Self {
        Self {
            mapping_name: mapping_name.into(),
            timestamp_ms: chrono_now_ms(),
            slave,
            value,
        }
    }
}

/// Modbus 写入请求（简化版，目前只支持写保持寄存器）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModbusWriteRequest {
    /// 点位名称
    pub mapping_name: String,
    /// 目标从站
    pub slave: u8,
    /// 起始地址
    pub address: u16,
    /// 要写入的寄存器值列表
    pub registers: Vec<u16>,
}

/// 推送给 WebSocket 客户端的数据帧。
/// 与 [`BridgeMessage`] 相比字段更扁平，方便前端直接消费。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsDataFrame {
    /// 帧类型，目前固定为 "data"
    pub kind: String,
    /// 点位名称
    pub mapping: String,
    /// 从站地址
    pub slave: u8,
    /// 采集时间戳（毫秒）
    pub timestamp_ms: i64,
    /// 线圈值
    pub coils: Vec<bool>,
    /// 寄存器值
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

/// 获取当前时间的 UNIX epoch 毫秒数。
fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}