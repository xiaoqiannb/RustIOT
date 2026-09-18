//! 协议与消息模型模块。
//! 
//! 本模块定义了整个 RustIOT 系统中流转的数据结构，包括：
//! - [`modbus`]：Modbus 寄存器映射、读取/写入类型、值封装等
//! - [`mqtt`]：MQTT 消息结构与客户端配置
//! - [`bridge`]：系统内部各组件（Modbus ↔ MQTT ↔ WebSocket）之间的桥接消息
//! 
//! 通过 `pub use` 重新导出所有类型，外部只需 `use crate::protocol::*` 即可使用。

pub mod bridge;
pub mod modbus;
pub mod mqtt;

pub use bridge::*;
pub use modbus::*;
pub use mqtt::*;