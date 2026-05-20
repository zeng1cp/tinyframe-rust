use crate::{Checksum, FrameChannel, ReceivedFrame, Transport, strategy::IdAllocator};

pub type ListenerId = usize;

pub type FrameCallback<C, T, K, A, const ID: usize, const LEN: usize, const TY: usize> =
    fn(&mut C, &mut FrameChannel<'_, T, K, ID, LEN, TY, A>, ReceivedFrame<'_>) -> ListenerAction;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerAction {
    /// Current listener does not consume the frame, continue dispatching.
    Next,
    /// Keep current listener active.
    Stay,
    /// Keep listener active and renew timeout (ID listener only).
    Renew,
    /// Remove current listener after callback returns.
    Close,
}

#[derive(Clone, Copy)]
pub(crate) struct IdListener<C, T, K, A, const ID: usize, const LEN: usize, const TY: usize>
where
    T: Transport,
    K: Checksum,
    A: IdAllocator,
{
    pub id: u32,
    pub timeout_left: u16,
    pub timeout_max: u16,
    pub on_frame: FrameCallback<C, T, K, A, ID, LEN, TY>,
}

#[derive(Clone, Copy)]
pub(crate) struct TypeListener<C, T, K, A, const ID: usize, const LEN: usize, const TY: usize>
where
    T: Transport,
    K: Checksum,
    A: IdAllocator,
{
    pub typ: u32,
    pub on_frame: FrameCallback<C, T, K, A, ID, LEN, TY>,
}
