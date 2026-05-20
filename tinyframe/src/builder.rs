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

    pub fn peer(mut self, peer: Peer) -> Self {
        self.peer = peer;
        self
    }

    pub fn sof(mut self, sof: u8) -> Self {
        self.sof = sof;
        self
    }

    pub fn parser_timeout_ticks(mut self, ticks: u16) -> Self {
        self.parser_timeout_ticks = ticks;
        self
    }

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
