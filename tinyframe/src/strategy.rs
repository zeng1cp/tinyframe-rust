use crate::{
    Checksum, FrameCallback, FrameChannel, ListenerAction, ReceivedFrame, Transport,
    listener::{IdListener, TypeListener},
};

pub trait IdAllocator {
    fn alloc_id<const ID: usize>(&mut self, next_id: &mut u32, peer_master: bool) -> u32;
}

#[derive(Clone, Copy, Default)]
pub struct SequentialIdAllocator;

impl IdAllocator for SequentialIdAllocator {
    fn alloc_id<const ID: usize>(&mut self, next_id: &mut u32, peer_master: bool) -> u32 {
        let value_mask = (1u32 << (ID * 8 - 1)) - 1;
        let peer_mask = 1u32 << (ID * 8 - 1);
        let raw = *next_id & value_mask;
        *next_id = next_id.wrapping_add(1) & value_mask;
        if peer_master { raw | peer_mask } else { raw }
    }
}

pub trait DispatchPolicy {
    fn dispatch<C, T, K, A, const IDS: usize, const TYPES: usize, const ID: usize, const LEN: usize, const TY: usize>(
        &self,
        ctx: &mut C,
        channel: &mut FrameChannel<'_, T, K, ID, LEN, TY, A>,
        id_listeners: &mut [Option<IdListener<C, T, K, A, ID, LEN, TY>>; IDS],
        type_listeners: &mut [Option<TypeListener<C, T, K, A, ID, LEN, TY>>; TYPES],
        generic_listener: Option<FrameCallback<C, T, K, A, ID, LEN, TY>>,
        frame: ReceivedFrame<'_>,
    ) where
        T: Transport,
        K: Checksum,
        A: IdAllocator;
}

#[derive(Clone, Copy, Default)]
pub struct IdThenTypeDispatch;

impl DispatchPolicy for IdThenTypeDispatch {
    fn dispatch<C, T, K, A, const IDS: usize, const TYPES: usize, const ID: usize, const LEN: usize, const TY: usize>(
        &self,
        ctx: &mut C,
        channel: &mut FrameChannel<'_, T, K, ID, LEN, TY, A>,
        id_listeners: &mut [Option<IdListener<C, T, K, A, ID, LEN, TY>>; IDS],
        type_listeners: &mut [Option<TypeListener<C, T, K, A, ID, LEN, TY>>; TYPES],
        generic_listener: Option<FrameCallback<C, T, K, A, ID, LEN, TY>>,
        frame: ReceivedFrame<'_>,
    ) where
        T: Transport,
        K: Checksum,
        A: IdAllocator,
    {
        let mut handled = false;
        for slot in id_listeners {
            if let Some(listener) = slot.as_mut() {
                if listener.id == frame.id {
                    handled = true;
                    match (listener.on_frame)(ctx, channel, frame) {
                        ListenerAction::Close => *slot = None,
                        ListenerAction::Renew => listener.timeout_left = listener.timeout_max,
                        ListenerAction::Stay | ListenerAction::Next => {}
                    }
                }
            }
        }
        for slot in type_listeners {
            if let Some(listener) = slot.as_mut() {
                if listener.typ == frame.typ {
                    handled = true;
                    if let ListenerAction::Close = (listener.on_frame)(ctx, channel, frame) {
                        *slot = None;
                    }
                }
            }
        }
        if !handled {
            if let Some(cb) = generic_listener {
                cb(ctx, channel, frame);
            }
        }
    }
}
