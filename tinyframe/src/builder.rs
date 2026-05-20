use crate::{
    Checksum, Error, Peer, TinyFrame, Transport,
    strategy::{DispatchPolicy, IdAllocator, IdThenTypeDispatch, SequentialIdAllocator},
};

pub struct TinyFrameBuilder<C, T, K> {
    ctx: C,
    transport: T,
    checksum: K,
    peer: Peer,
    sof: u8,
    parser_timeout_ticks: u16,
}

impl<C, T, K> TinyFrameBuilder<C, T, K>
where
    T: Transport,
    K: Checksum,
{
    /// Create a builder with sensible defaults.
    ///
    /// Defaults:
    /// - `peer`: `Peer::Master`
    /// - `sof`: `0x01`
    /// - `parser_timeout_ticks`: `10`
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{TinyFrameBuilder, BufferTransport, NoChecksum};
    ///
    /// // 最小可用构造：先创建 builder，后续再链式补充配置。
    /// let _b = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum);
    /// ```
    pub fn new(ctx: C, transport: T, checksum: K) -> Self {
        Self {
            ctx,
            transport,
            checksum,
            peer: Peer::Master,
            sof: 0x01,
            parser_timeout_ticks: 10,
        }
    }

    /// Set local peer role.
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{TinyFrameBuilder, BufferTransport, NoChecksum, Peer};
    ///
    /// // 当你希望本端是被动响应方时，通常设置为 Slave。
    /// let _b = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum)
    ///     .peer(Peer::Slave);
    /// ```
    pub fn peer(mut self, peer: Peer) -> Self {
        self.peer = peer;
        self
    }

    /// Set SOF (start-of-frame) byte used by encoder/decoder.
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{TinyFrameBuilder, BufferTransport, NoChecksum};
    ///
    /// // SOF 需与你对端协议约定一致，否则无法正确对帧。
    /// let _b = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum)
    ///     .sof(0xAA);
    /// ```
    pub fn sof(mut self, sof: u8) -> Self {
        self.sof = sof;
        self
    }

    /// Set parser timeout tick count.
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{TinyFrameBuilder, BufferTransport, NoChecksum};
    ///
    /// // 超时 tick 越大，解析器等待后续字节的容忍度越高。
    /// let _b = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum)
    ///     .parser_timeout_ticks(50);
    /// ```
    pub fn parser_timeout_ticks(mut self, ticks: u16) -> Self {
        self.parser_timeout_ticks = ticks;
        self
    }

    /// Build a `TinyFrame` with default strategies:
    /// - `SequentialIdAllocator`
    /// - `IdThenTypeDispatch`
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{TinyFrameBuilder, BufferTransport, NoChecksum, Peer};
    ///
    /// // 这个路径适合“默认策略就够用”的多数场景。
    /// let _tf = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum)
    ///     .peer(Peer::Master)
    ///     .sof(0x01)
    ///     .parser_timeout_ticks(10)
    ///     .build::<64, 4, 4, 1, 1, 1>()
    ///     .unwrap();
    /// ```
    ///
    /// # Example (UART + CRC16)
    /// ```no_run
    /// use tinyframe::{TinyFrameBuilder, Transport, Peer, Crc16};
    ///
    /// // 自定义 UART transport：只展示 TX 路径最小实现。
    /// struct UartTransport {
    ///     tx_log: [u8; 256],
    ///     len: usize,
    /// }
    ///
    /// impl UartTransport {
    ///     fn new() -> Self { Self { tx_log: [0; 256], len: 0 } }
    /// }
    ///
    /// impl Transport for UartTransport {
    ///     type Error = ();
    ///     fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
    ///         let n = core::cmp::min(bytes.len(), self.tx_log.len().saturating_sub(self.len));
    ///         self.tx_log[self.len..self.len + n].copy_from_slice(&bytes[..n]);
    ///         self.len += n;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// // 使用 Builder 配置 UART + CRC16，再用默认策略 build。
    /// let _tf = TinyFrameBuilder::new((), UartTransport::new(), Crc16)
    ///     .peer(Peer::Master)
    ///     .sof(0x55)
    ///     .parser_timeout_ticks(30)
    ///     .build::<128, 4, 4, 1, 1, 1>()
    ///     .unwrap();
    /// ```
    pub fn build<
        const RX: usize,
        const IDS: usize,
        const TYPES: usize,
        const ID: usize,
        const LEN: usize,
        const TY: usize,
    >(
        self,
    ) -> Result<
        TinyFrame<C, T, K, RX, IDS, TYPES, ID, LEN, TY, SequentialIdAllocator, IdThenTypeDispatch>,
        Error<T::Error>,
    > {
        TinyFrame::new(
            self.ctx,
            self.transport,
            self.checksum,
            self.peer,
            self.sof,
            self.parser_timeout_ticks,
        )
    }

    /// Build a `TinyFrame` with custom strategy types.
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{
    ///     TinyFrameBuilder, BufferTransport, NoChecksum,
    ///     SequentialIdAllocator, IdThenTypeDispatch
    /// };
    ///
    /// // 当你需要显式声明策略类型（例如后续替换为自定义策略）时使用该方法。
    /// let _tf = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum)
    ///     .build_with_strategies::<SequentialIdAllocator, IdThenTypeDispatch, 64, 4, 4, 1, 1, 1>()
    ///     .unwrap();
    /// ```
    ///
    /// # Example (UART + CRC32 + 显式策略)
    /// ```no_run
    /// use tinyframe::{
    ///     TinyFrameBuilder, Transport, Crc32, Peer,
    ///     SequentialIdAllocator, IdThenTypeDispatch
    /// };
    ///
    /// struct UartTransport {
    ///     tx_log: [u8; 512],
    ///     len: usize,
    /// }
    ///
    /// impl UartTransport {
    ///     fn new() -> Self { Self { tx_log: [0; 512], len: 0 } }
    /// }
    ///
    /// impl Transport for UartTransport {
    ///     type Error = ();
    ///     fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
    ///         let n = core::cmp::min(bytes.len(), self.tx_log.len().saturating_sub(self.len));
    ///         self.tx_log[self.len..self.len + n].copy_from_slice(&bytes[..n]);
    ///         self.len += n;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let _tf = TinyFrameBuilder::new((), UartTransport::new(), Crc32)
    ///     .peer(Peer::Slave)
    ///     .sof(0xA5)
    ///     .parser_timeout_ticks(40)
    ///     .build_with_strategies::<SequentialIdAllocator, IdThenTypeDispatch, 256, 8, 8, 1, 2, 1>()
    ///     .unwrap();
    /// ```
    pub fn build_with_strategies<
        A,
        D,
        const RX: usize,
        const IDS: usize,
        const TYPES: usize,
        const ID: usize,
        const LEN: usize,
        const TY: usize,
    >(
        self,
    ) -> Result<TinyFrame<C, T, K, RX, IDS, TYPES, ID, LEN, TY, A, D>, Error<T::Error>>
    where
        A: IdAllocator + Default,
        D: DispatchPolicy + Default,
    {
        TinyFrame::new(
            self.ctx,
            self.transport,
            self.checksum,
            self.peer,
            self.sof,
            self.parser_timeout_ticks,
        )
    }
}
