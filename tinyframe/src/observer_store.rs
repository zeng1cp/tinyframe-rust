use crate::{
    Checksum, Error, FrameCallback, ReceivedFrame, Transport,
    listener::{IdListener, ListenerId, TypeListener},
    strategy::IdAllocator,
};

pub(crate) struct ObserverStore<
    C,
    T,
    K,
    A,
    const IDS: usize,
    const TYPES: usize,
    const ID: usize,
    const LEN: usize,
    const TY: usize,
> where
    T: Transport,
    K: Checksum,
    A: IdAllocator,
{
    pub id_listeners: [Option<IdListener<C, T, K, A, ID, LEN, TY>>; IDS],
    pub type_listeners: [Option<TypeListener<C, T, K, A, ID, LEN, TY>>; TYPES],
    pub generic_listener: Option<FrameCallback<C, T, K, A, ID, LEN, TY>>,
}

impl<C, T, K, A, const IDS: usize, const TYPES: usize, const ID: usize, const LEN: usize, const TY: usize>
    ObserverStore<C, T, K, A, IDS, TYPES, ID, LEN, TY>
where
    T: Transport,
    K: Checksum,
    A: IdAllocator,
{
    pub fn new() -> Self {
        Self {
            id_listeners: core::array::from_fn(|_| None),
            type_listeners: core::array::from_fn(|_| None),
            generic_listener: None,
        }
    }

    pub fn add_id_listener(
        &mut self,
        id: u32,
        timeout_ticks: u16,
        on_frame: FrameCallback<C, T, K, A, ID, LEN, TY>,
    ) -> Result<ListenerId, Error<T::Error>> {
        let slot = self.id_listeners.iter().position(|x| x.is_none()).ok_or(Error::NoListenerSlot)?;
        self.id_listeners[slot] = Some(IdListener { id, timeout_left: timeout_ticks, timeout_max: timeout_ticks, on_frame });
        Ok(slot)
    }
    pub fn add_type_listener(&mut self, typ: u32, on_frame: FrameCallback<C, T, K, A, ID, LEN, TY>) -> Result<ListenerId, Error<T::Error>> {
        let slot = self.type_listeners.iter().position(|x| x.is_none()).ok_or(Error::NoListenerSlot)?;
        self.type_listeners[slot] = Some(TypeListener { typ, on_frame });
        Ok(slot)
    }
    pub fn remove_id_listener(&mut self, listener_id: ListenerId) -> bool { listener_id < IDS && self.id_listeners[listener_id].take().is_some() }
    pub fn remove_type_listener(&mut self, listener_id: ListenerId) -> bool { listener_id < TYPES && self.type_listeners[listener_id].take().is_some() }
    pub fn remove_id_listener_by_frame_id(&mut self, frame_id: u32) -> bool {
        for slot in &mut self.id_listeners { if slot.as_ref().map(|x| x.id == frame_id).unwrap_or(false) { *slot = None; return true; } }
        false
    }
    pub fn remove_type_listener_by_type(&mut self, typ: u32) -> bool {
        for slot in &mut self.type_listeners { if slot.as_ref().map(|x| x.typ == typ).unwrap_or(false) { *slot = None; return true; } }
        false
    }
    pub fn renew_id_listener(&mut self, frame_id: u32) -> bool {
        for slot in &mut self.id_listeners { if let Some(listener)=slot { if listener.id == frame_id { listener.timeout_left = listener.timeout_max; return true; } } }
        false
    }
    pub fn tick_timeouts<F: FnMut(ReceivedFrame<'_>, FrameCallback<C, T, K, A, ID, LEN, TY>)>(&mut self, mut on_timeout: F) {
        for i in 0..IDS {
            let mut entry = match self.id_listeners[i].take() { Some(e) => e, None => continue };
            if entry.timeout_left > 0 {
                entry.timeout_left -= 1;
                if entry.timeout_left == 0 {
                    on_timeout(ReceivedFrame { id: entry.id, typ: 0, data: &[], timed_out: true }, entry.on_frame);
                    continue;
                }
            }
            self.id_listeners[i] = Some(entry);
        }
    }
}
