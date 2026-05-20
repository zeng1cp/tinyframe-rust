#![no_std]

//! `tinyframe`: a `no_std` TinyFrame-style protocol engine for embedded targets.
//!
//! Checksum coverage intentionally follows TinyFrame framing semantics:
//! - SOF is a framing sentinel and is **not** included in checksum calculation.
//! - LEN / ID / TYPE and payload bytes are included.
//! 
mod error;
mod peer;
mod listener;
mod frame;
mod checksum;
mod transport;
mod parser;
mod tx_core;
mod channel;
mod tinyframe;
mod observer_store;
mod rx_dispatch_core;
mod rx_parser_core;
mod utils;

// 重新导出最常用的类型
pub use error::{Error, ParseError};
pub use peer::Peer;
pub use listener::{ListenerAction, FrameCallback};
pub use frame::{Frame, ReceivedFrame};
pub use checksum::{Checksum, NoChecksum, XorChecksum, Crc8Maxim, Crc16, Crc32};
pub use transport::{Transport, BufferTransport};
pub use channel::FrameChannel;
pub use tinyframe::TinyFrame;


