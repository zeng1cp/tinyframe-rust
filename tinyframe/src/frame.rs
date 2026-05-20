#[derive(Clone, Copy, Debug)]
pub struct Frame {
    /// Frame ID (request ID, or response-correlated ID).
    pub id: u32,
    /// Application-defined message type.
    pub typ: u32,
    /// When true, `id` is used as-is and not auto-allocated.
    pub is_response: bool,
}

/// Borrowed decoded frame view.
///
/// `data` is zero-copy borrowed from internal RX buffer and is valid only
/// during listener callback execution.
#[derive(Clone, Copy)]
pub struct ReceivedFrame<'a> {
    pub id: u32,
    pub typ: u32,
    pub data: &'a [u8],
    pub timed_out: bool,
}