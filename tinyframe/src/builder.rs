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
    /// let _tf = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum)
    ///     .peer(Peer::Master)
    ///     .sof(0x01)
    ///     .parser_timeout_ticks(10)
    ///     .build::<64, 4, 4, 1, 1, 1>()
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
    /// let _tf = TinyFrameBuilder::new((), BufferTransport::<256>::new(), NoChecksum)
    ///     .build_with_strategies::<SequentialIdAllocator, IdThenTypeDispatch, 64, 4, 4, 1, 1, 1>()
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
