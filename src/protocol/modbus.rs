use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModbusDataType {
    Coil,
    DiscreteInput,
    HoldingRegister,
    InputRegister,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModbusReadKind {
    Coil,
    DiscreteInput,
    HoldingRegister,
    InputRegister,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModbusWriteKind {
    Coil,
    HoldingRegister,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModbusValue {
    pub register: u16,
    pub data_type: ModbusDataType,
    pub coils: Vec<bool>,
    pub registers: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModbusRegisterMapping {
    pub name: String,
    pub slave: u8,
    pub address: u16,
    pub len: u16,
    pub data_type: ModbusReadKind,
    pub topic: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModbusRequest {
    pub slave: u8,
    pub kind: ModbusWriteKind,
    pub address: u16,
    pub value: ModbusValue,
}