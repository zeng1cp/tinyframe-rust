use crate::{
    Checksum, FrameCallback, FrameChannel, ListenerAction, ReceivedFrame, Transport,
    listener::{IdListener, TypeListener},
};

pub(crate) struct RxDispatchCore;
impl RxDispatchCore {
    pub fn dispatch<C, T, K, const IDS: usize, const TYPES: usize, const ID: usize, const LEN: usize, const TY: usize>(
        ctx: &mut C,
        tx: &mut crate::tx_core::TxCore<T, K, ID, LEN, TY>,
        id_listeners: &mut [Option<IdListener<C, T, K, ID, LEN, TY>>; IDS],
        type_listeners: &mut [Option<TypeListener<C, T, K, ID, LEN, TY>>; TYPES],
        generic_listener: Option<FrameCallback<C, T, K, ID, LEN, TY>>,
        frame: ReceivedFrame<'_>,
    ) where T: Transport, K: Checksum {
        let mut channel = FrameChannel { tx };
        let mut handled = false;
        for slot in id_listeners {
            if let Some(listener) = slot.as_mut() {
                if listener.id == frame.id {
                    handled = true;
                    match (listener.on_frame)(ctx, &mut channel, frame) {
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
                    if let ListenerAction::Close = (listener.on_frame)(ctx, &mut channel, frame) {
                        *slot = None;
                    }
                }
            }
        }
        if !handled { if let Some(cb) = generic_listener { cb(ctx, &mut channel, frame); } }
    }
}
