use core::cmp::min;

/// Byte output backend used by the protocol engine.
///
/// Typical embedded implementations wrap UART/SPI/USB CDC drivers.
pub trait Transport {
    type Error;
    fn begin_frame(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;
    fn end_frame(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn abort_frame(&mut self) {}
}

/// Fixed-size in-memory transport used by tests and host-side simulation.
pub struct BufferTransport<const N: usize> {
    tx: [u8; N],
    len: usize,
}

impl<const N: usize> BufferTransport<N> {
    /// Create empty transport buffer.
    pub fn new() -> Self {
        Self { tx: [0; N], len: 0 }
    }

    /// Return accumulated TX bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.tx[..self.len]
    }

    /// Clear accumulated TX bytes.
    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl<const N: usize> Default for BufferTransport<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Transport for BufferTransport<N> {
    type Error = ();

    fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        let take = min(N.saturating_sub(self.len), bytes.len());
        self.tx[self.len..self.len + take].copy_from_slice(&bytes[..take]);
        self.len += take;
        Ok(())
    }
}
