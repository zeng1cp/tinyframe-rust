pub trait Checksum {
    type Value: Copy + Default + PartialEq;
    const WIDTH: usize;

    fn start(&self) -> Self::Value;
    fn add(&self, sum: Self::Value, byte: u8) -> Self::Value;
    fn finish(&self, sum: Self::Value) -> Self::Value;
    fn encode(&self, sum: Self::Value, out: &mut [u8]) -> usize;
    fn decode(&self, bytes: &[u8]) -> Self::Value;
}

/// Checksum strategy that disables checksum bytes completely.
///
/// `WIDTH = 0` means no checksum field is present in encoded frames.
#[derive(Clone, Copy, Default)]
pub struct NoChecksum;

impl Checksum for NoChecksum {
    type Value = u8;
    const WIDTH: usize = 0;

    fn start(&self) -> Self::Value {
        0
    }
    fn add(&self, sum: Self::Value, _byte: u8) -> Self::Value {
        sum
    }
    fn finish(&self, sum: Self::Value) -> Self::Value {
        sum
    }
    fn encode(&self, _sum: Self::Value, _out: &mut [u8]) -> usize {
        0
    }
    fn decode(&self, _bytes: &[u8]) -> Self::Value {
        0
    }
}

/// TinyFrame XOR checksum (`~(xor of bytes)`).
#[derive(Clone, Copy, Default)]
pub struct XorChecksum;

impl Checksum for XorChecksum {
    type Value = u8;
    const WIDTH: usize = 1;

    fn start(&self) -> Self::Value {
        0
    }
    fn add(&self, sum: Self::Value, byte: u8) -> Self::Value {
        sum ^ byte
    }
    fn finish(&self, sum: Self::Value) -> Self::Value {
        !sum
    }
    fn encode(&self, sum: Self::Value, out: &mut [u8]) -> usize {
        out[0] = sum;
        1
    }
    fn decode(&self, bytes: &[u8]) -> Self::Value {
        bytes[0]
    }
}

/// Dallas/Maxim CRC8 (poly 0x31 reflected = 0x8C).
#[derive(Clone, Copy, Default)]
pub struct Crc8Maxim;

impl Checksum for Crc8Maxim {
    type Value = u8;
    const WIDTH: usize = 1;

    fn start(&self) -> Self::Value {
        0
    }
    fn add(&self, sum: Self::Value, byte: u8) -> Self::Value {
        let mut crc = sum;
        let mut b = byte;
        for _ in 0..8 {
            let mix = (crc ^ b) & 0x01;
            crc >>= 1;
            if mix != 0 {
                crc ^= 0x8C;
            }
            b >>= 1;
        }
        crc
    }
    fn finish(&self, sum: Self::Value) -> Self::Value {
        sum
    }
    fn encode(&self, sum: Self::Value, out: &mut [u8]) -> usize {
        out[0] = sum;
        1
    }
    fn decode(&self, bytes: &[u8]) -> Self::Value {
        bytes[0]
    }
}

/// CRC16 (poly 0x8005 reflected implementation with 0xA001).
#[derive(Clone, Copy, Default)]
pub struct Crc16;

impl Checksum for Crc16 {
    type Value = u16;
    const WIDTH: usize = 2;

    fn start(&self) -> Self::Value {
        0
    }
    fn add(&self, sum: Self::Value, byte: u8) -> Self::Value {
        let mut crc = sum ^ (byte as u16);
        for _ in 0..8 {
            crc = if (crc & 1) != 0 {
                (crc >> 1) ^ 0xA001
            } else {
                crc >> 1
            };
        }
        crc
    }
    fn finish(&self, sum: Self::Value) -> Self::Value {
        sum
    }
    fn encode(&self, sum: Self::Value, out: &mut [u8]) -> usize {
        out[0] = (sum >> 8) as u8;
        out[1] = (sum & 0xFF) as u8;
        2
    }
    fn decode(&self, bytes: &[u8]) -> Self::Value {
        ((bytes[0] as u16) << 8) | (bytes[1] as u16)
    }
}

/// CRC32 (poly 0x04C11DB7 reflected implementation with 0xEDB88320).
#[derive(Clone, Copy, Default)]
pub struct Crc32;

impl Checksum for Crc32 {
    type Value = u32;
    const WIDTH: usize = 4;

    fn start(&self) -> Self::Value {
        0xFFFF_FFFF
    }
    fn add(&self, sum: Self::Value, byte: u8) -> Self::Value {
        let mut crc = sum ^ (byte as u32);
        for _ in 0..8 {
            crc = if (crc & 1) != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
        crc
    }
    fn finish(&self, sum: Self::Value) -> Self::Value {
        sum ^ 0xFFFF_FFFF
    }
    fn encode(&self, sum: Self::Value, out: &mut [u8]) -> usize {
        out[0] = (sum >> 24) as u8;
        out[1] = (sum >> 16) as u8;
        out[2] = (sum >> 8) as u8;
        out[3] = (sum & 0xFF) as u8;
        4
    }
    fn decode(&self, bytes: &[u8]) -> Self::Value {
        ((bytes[0] as u32) << 24)
            | ((bytes[1] as u32) << 16)
            | ((bytes[2] as u32) << 8)
            | (bytes[3] as u32)
    }
}