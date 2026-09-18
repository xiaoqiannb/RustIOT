//! Modbus 协议相关的数据类型定义。

use serde::{Deserialize, Serialize};

/// Modbus 数据类型。
/// 标识寄存器/线圈的四种标准类型（对应 Modbus 四种地址空间）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModbusDataType {
    /// 线圈（读/写，1 bit）
    Coil,
    /// 离散输入（只读，1 bit）
    DiscreteInput,
    /// 保持寄存器（读/写，16 bit）
    HoldingRegister,
    /// 输入寄存器（只读，16 bit）
    InputRegister,
}

/// Modbus 读取类型。
/// 功能上等同于 [`ModbusDataType`]，但专门用于表示"读"操作的目标类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModbusReadKind {
    Coil,
    DiscreteInput,
    HoldingRegister,
    InputRegister,
}

/// Modbus 写入类型。
/// 只包含可写的两种类型：线圈和保持寄存器。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModbusWriteKind {
    Coil,
    HoldingRegister,
}

/// 一次 Modbus 读取的值封装。
/// 
/// 根据 `data_type` 的不同，实际数据存放在不同字段中：
/// - `Coil` / `DiscreteInput` → `coils` 存布尔值列表，`registers` 为空
/// - `HoldingRegister` / `InputRegister` → `registers` 存 u16 值列表，`coils` 为空
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModbusValue {
    /// 起始寄存器/线圈地址
    pub register: u16,
    /// 数据类型
    pub data_type: ModbusDataType,
    /// 线圈/离散输入的读取结果
    pub coils: Vec<bool>,
    /// 寄存器的读取结果
    pub registers: Vec<u16>,
}

/// Modbus 寄存器映射配置。
/// 描述了"要读取哪个从站的哪个地址、读多少、属于哪个点位名、上报到哪个 MQTT topic"。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModbusRegisterMapping {
    /// 点位名称（业务标识）
    pub name: String,
    /// 从站地址（Modbus Slave ID）
    pub slave: u8,
    /// 起始地址
    pub address: u16,
    /// 读取长度（线圈个数或寄存器个数）
    pub len: u16,
    /// 读取类型（决定使用哪个功能码）
    pub data_type: ModbusReadKind,
    /// 上报时使用的 MQTT topic
    pub topic: String,
}

/// Modbus 写入请求。
/// 用于从 MQTT 或 WebSocket 下发写命令到 Modbus 从站。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModbusRequest {
    /// 目标从站地址
    pub slave: u8,
    /// 写入类型（线圈或保持寄存器）
    pub kind: ModbusWriteKind,
    /// 起始地址
    pub address: u16,
    /// 要写入的值
    pub value: ModbusValue,
}