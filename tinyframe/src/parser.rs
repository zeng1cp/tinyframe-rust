use crate::Checksum;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParseStage {
    Sof,          // 等待帧起始标记 (SOF)
    Id,           // 解析 ID 字段
    Len,          // 解析 LEN 字段
    Type,         // 解析 TYPE 字段
    HeadChecksum, // 接收并验证头部校验和
    Data,         // 接收数据载荷
    DataChecksum, // 接收并验证数据校验和
}

/// Internal streaming parser state.
///
/// Parsing is incremental and byte-driven (`accept_byte`).
#[derive(Clone, Copy)]
pub(crate) struct Parser<K: Checksum> {
    pub stage: ParseStage,
    pub len: u32,
    pub id: u32,
    pub typ: u32,
    pub field_idx: usize,
    pub data_idx: usize,
    pub head_checksum: K::Value,
    pub data_checksum: K::Value,
    pub checksum_buf: [u8; 4],
    pub checksum_idx: usize,
    pub timeout: u16,
}

impl<K: Checksum> Default for Parser<K> {
    fn default() -> Self {
        Self {
            stage: ParseStage::Sof,
            len: 0,
            id: 0,
            typ: 0,
            field_idx: 0,
            data_idx: 0,
            head_checksum: K::Value::default(),
            data_checksum: K::Value::default(),
            checksum_buf: [0; 4],
            checksum_idx: 0,
            timeout: 0,
        }
    }
}
