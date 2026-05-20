use crate::{
    Checksum, FrameCallback, FrameChannel, ReceivedFrame, Transport,
    listener::{IdListener, TypeListener}, strategy::{DispatchPolicy, IdAllocator},
};

pub(crate) struct RxDispatchCore;
impl RxDispatchCore {
    pub fn dispatch<C, T, K, A, D, const IDS: usize, const TYPES: usize, const ID: usize, const LEN: usize, const TY: usize>(
        ctx: &mut C,
        tx: &mut crate::tx_core::TxCore<T, K, A, ID, LEN, TY>,
        policy: &D,
        id_listeners: &mut [Option<IdListener<C, T, K, A, ID, LEN, TY>>; IDS],
        type_listeners: &mut [Option<TypeListener<C, T, K, A, ID, LEN, TY>>; TYPES],
        generic_listener: Option<FrameCallback<C, T, K, A, ID, LEN, TY>>,
        frame: ReceivedFrame<'_>,
    ) where T: Transport, K: Checksum, A: IdAllocator, D: DispatchPolicy {
        let mut channel = FrameChannel { tx };
        policy.dispatch(ctx, &mut channel, id_listeners, type_listeners, generic_listener, frame);
    }
}
