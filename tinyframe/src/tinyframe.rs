use crate::{
    Checksum, Error, Frame, FrameCallback, FrameChannel, ListenerAction, ParseError, Peer,
    ReceivedFrame, Transport,
    listener::{IdListener, ListenerId, TypeListener},
    parser::{ParseStage, Parser},
    tx_core::TxCore,
    utils::{FieldKind, is_valid_width},
};

pub struct TinyFrame<
    C,
    T,
    K,
    const RX: usize,
    const IDS: usize,
    const TYPES: usize,
    const ID: usize,
    const LEN: usize,
    const TY: usize,
> where
    T: Transport,
    K: Checksum,
{
    ctx: C,
    tx: TxCore<T, K, ID, LEN, TY>,
    parser_timeout_ticks: u16,
    parser: Parser<K>,
    rx_buf: [u8; RX],
    id_listeners: [Option<IdListener<C, T, K, ID, LEN, TY>>; IDS],
    type_listeners: [Option<TypeListener<C, T, K, ID, LEN, TY>>; TYPES],
    generic_listener: Option<FrameCallback<C, T, K, ID, LEN, TY>>,
    last_parse_error: Option<ParseError>,
}

impl<
    C,
    T,
    K,
    const RX: usize,
    const IDS: usize,
    const TYPES: usize,
    const ID: usize,
    const LEN: usize,
    const TY: usize,
> TinyFrame<C, T, K, RX, IDS, TYPES, ID, LEN, TY>
where
    T: Transport,
    K: Checksum,
{
    /// Create a TinyFrame engine instance.
    ///
    /// # Type Parameters
    /// - `RX`: max accepted payload length in bytes.
    /// - `IDS`: ID listener slot count.
    /// - `TYPES`: type listener slot count.
    /// - `ID`/`LEN`/`TY`: encoded field widths in bytes (1..=4).
    pub fn new(
        ctx: C,
        transport: T,
        checksum: K,
        peer: Peer,
        sof: u8,
        parser_timeout_ticks: u16,
    ) -> Result<Self, Error<T::Error>> {
        if !is_valid_width(ID) || !is_valid_width(LEN) || !is_valid_width(TY) {
            return Err(Error::InvalidFieldWidth);
        }
        if K::WIDTH > 4 {
            return Err(Error::InvalidFieldWidth);
        }

        Ok(Self {
            ctx,
            tx: TxCore {
                transport,
                checksum,
                sof,
                peer_master: matches!(peer, Peer::Master),
                tx_busy: false,
                multipart: None,
                next_id: 0,
            },
            parser_timeout_ticks,
            parser: Parser::default(),
            rx_buf: [0; RX],
            id_listeners: core::array::from_fn(|_| None),
            type_listeners: core::array::from_fn(|_| None),
            generic_listener: None,
            last_parse_error: None,
        })
    }

    /// Immutable access to user context.
    pub fn context(&self) -> &C {
        &self.ctx
    }
    /// Mutable access to user context.
    pub fn context_mut(&mut self) -> &mut C {
        &mut self.ctx
    }
    /// Mutable access to transport backend.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.tx.transport
    }
    /// Last parse error observed during `accept`/`accept_byte`.
    pub fn last_parse_error(&self) -> Option<ParseError> {
        self.last_parse_error
    }

    /// Send an outbound frame.
    pub fn send(&mut self, frame: Frame, data: &[u8]) -> Result<(), Error<T::Error>> {
        self.tx.send(frame, data)
    }

    /// Send a response frame reusing request ID.
    pub fn respond(
        &mut self,
        request: ReceivedFrame<'_>,
        typ: u32,
        data: &[u8],
    ) -> Result<(), Error<T::Error>> {
        self.send(
            Frame {
                id: request.id,
                typ,
                is_response: true,
            },
            data,
        )
    }

    pub fn send_multipart(&mut self, frame: Frame, len: u32) -> Result<u32, Error<T::Error>> {
        self.tx.begin_multipart(frame, len)
    }

    pub fn multipart_payload(&mut self, data: &[u8]) -> Result<(), Error<T::Error>> {
        self.tx.multipart_payload(data)
    }

    pub fn multipart_close(&mut self) -> Result<(), Error<T::Error>> {
        self.tx.multipart_close()
    }

    pub fn add_id_listener(
        &mut self,
        id: u32,
        timeout_ticks: u16,
        on_frame: FrameCallback<C, T, K, ID, LEN, TY>,
    ) -> Result<ListenerId, Error<T::Error>> {
        let slot = self
            .id_listeners
            .iter()
            .position(|x| x.is_none())
            .ok_or(Error::NoListenerSlot)?;
        self.id_listeners[slot] = Some(IdListener {
            id,
            timeout_left: timeout_ticks,
            timeout_max: timeout_ticks,
            on_frame,
        });
        Ok(slot)
    }

    /// Register a type-based listener.
    pub fn add_type_listener(
        &mut self,
        typ: u32,
        on_frame: FrameCallback<C, T, K, ID, LEN, TY>,
    ) -> Result<ListenerId, Error<T::Error>> {
        let slot = self
            .type_listeners
            .iter()
            .position(|x| x.is_none())
            .ok_or(Error::NoListenerSlot)?;
        self.type_listeners[slot] = Some(TypeListener { typ, on_frame });
        Ok(slot)
    }

    /// Register a catch-all listener when no ID/type listener handled frame.
    pub fn set_generic_listener(&mut self, cb: FrameCallback<C, T, K, ID, LEN, TY>) {
        self.generic_listener = Some(cb);
    }

    pub fn clear_generic_listener(&mut self) {
        self.generic_listener = None;
    }

    /// Remove an ID listener by slot index.
    pub fn remove_id_listener(&mut self, listener_id: ListenerId) -> bool {
        if listener_id >= IDS {
            return false;
        }
        self.id_listeners[listener_id].take().is_some()
    }

    /// Remove an ID listener by frame ID.
    pub fn remove_id_listener_by_frame_id(&mut self, frame_id: u32) -> bool {
        for slot in &mut self.id_listeners {
            if slot.as_ref().map(|x| x.id == frame_id).unwrap_or(false) {
                *slot = None;
                return true;
            }
        }
        false
    }

    /// Remove a type listener by slot index.
    pub fn remove_type_listener(&mut self, listener_id: ListenerId) -> bool {
        if listener_id >= TYPES {
            return false;
        }
        self.type_listeners[listener_id].take().is_some()
    }

    /// Remove a type listener by message type.
    pub fn remove_type_listener_by_type(&mut self, typ: u32) -> bool {
        for slot in &mut self.type_listeners {
            if slot.as_ref().map(|x| x.typ == typ).unwrap_or(false) {
                *slot = None;
                return true;
            }
        }
        false
    }

    /// Renew timeout for an ID listener matched by frame ID.
    pub fn renew_id_listener(&mut self, frame_id: u32) -> bool {
        for slot in &mut self.id_listeners {
            if let Some(listener) = slot {
                if listener.id == frame_id {
                    listener.timeout_left = listener.timeout_max;
                    return true;
                }
            }
        }
        false
    }

    /// Tick parser and listener timeout state machine.
    pub fn tick(&mut self) {
        if self.parser.timeout > 0 {
            self.parser.timeout -= 1;
            if self.parser.timeout == 0 {
                self.reset_parser();
            }
        }

        for i in 0..IDS {
            let mut entry = match self.id_listeners[i].take() {
                Some(e) => e,
                None => continue,
            };

            if entry.timeout_left > 0 {
                entry.timeout_left -= 1;
                if entry.timeout_left == 0 {
                    let frame = ReceivedFrame {
                        id: entry.id,
                        typ: 0,
                        data: &[],
                        timed_out: true,
                    };
                    let mut channel = FrameChannel { tx: &mut self.tx };
                    let _ = (entry.on_frame)(&mut self.ctx, &mut channel, frame);
                    continue;
                }
            }

            self.id_listeners[i] = Some(entry);
        }
    }

    /// Reset incremental parser state.
    pub fn reset_parser(&mut self) {
        self.parser = Parser::default();
    }

    /// Feed a byte slice into streaming parser.
    pub fn accept(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.accept_byte(b);
        }
    }

    /// Feed one byte into streaming parser.
    pub fn accept_byte(&mut self, byte: u8) {
        if self.parser.stage != ParseStage::Sof {
            self.parser.timeout = self.parser_timeout_ticks;
        }

        match self.parser.stage {
            ParseStage::Sof => {
                if byte == self.tx.sof {
                    self.parser = Parser {
                        stage: ParseStage::Id,
                        head_checksum: self.tx.checksum.start(),
                        data_checksum: self.tx.checksum.start(),
                        timeout: self.parser_timeout_ticks,
                        ..Parser::default()
                    };
                }
            }
            ParseStage::Id => {
                self.parse_field_byte(byte, ID, FieldKind::Id);
            }
            ParseStage::Len => {
                self.parse_field_byte(byte, LEN, FieldKind::Len);
            }
            ParseStage::Type => {
                self.parse_field_byte(byte, TY, FieldKind::Type);
            }
            ParseStage::Data => {
                if self.parser.data_idx < RX {
                    self.rx_buf[self.parser.data_idx] = byte;
                }
                self.parser.data_checksum = self.tx.checksum.add(self.parser.data_checksum, byte);
                self.parser.data_idx += 1;
                if self.parser.data_idx >= self.parser.len as usize {
                    if K::WIDTH == 0 {
                        self.dispatch_zero_copy();
                        self.reset_parser();
                    } else {
                        self.parser.checksum_idx = 0;
                        self.parser.stage = ParseStage::DataChecksum;
                    }
                }
            }
            ParseStage::HeadChecksum => {
                if K::WIDTH == 0 {
                    self.dispatch_zero_copy();
                    self.reset_parser();
                    return;
                }
                if self.parser.checksum_idx < self.parser.checksum_buf.len() {
                    self.parser.checksum_buf[self.parser.checksum_idx] = byte;
                }
                self.parser.checksum_idx += 1;
                if self.parser.checksum_idx >= K::WIDTH {
                    let calc = self.tx.checksum.finish(self.parser.head_checksum);
                    let recv = self
                        .tx
                        .checksum
                        .decode(&self.parser.checksum_buf[..K::WIDTH]);
                    if calc != recv {
                        self.last_parse_error = Some(ParseError::ChecksumMismatch);
                    } else {
                        if self.parser.len == 0 {
                            self.dispatch_zero_copy();
                            self.reset_parser();
                        } else {
                            self.parser.stage = ParseStage::Data;
                            self.parser.data_idx = 0;
                            self.parser.data_checksum = self.tx.checksum.start();
                        }
                    }
                }
            }
            ParseStage::DataChecksum => {
                if self.parser.checksum_idx < self.parser.checksum_buf.len() {
                    self.parser.checksum_buf[self.parser.checksum_idx] = byte;
                }
                self.parser.checksum_idx += 1;
                if self.parser.checksum_idx >= K::WIDTH {
                    let calc = self.tx.checksum.finish(self.parser.data_checksum);
                    let recv = self
                        .tx
                        .checksum
                        .decode(&self.parser.checksum_buf[..K::WIDTH]);
                    if calc != recv {
                        self.last_parse_error = Some(ParseError::ChecksumMismatch);
                    } else {
                        self.dispatch_zero_copy();
                    }
                    self.reset_parser();
                }
            }
        }
    }

    fn parse_field_byte(&mut self, byte: u8, width: usize, target: FieldKind) {
        // 1. 将当前字节纳入头部校验和计算
        self.parser.head_checksum = self.tx.checksum.add(self.parser.head_checksum, byte);

        // 2. 按大端序拼接字段值
        let shift = ((width - 1 - self.parser.field_idx) * 8) as u32;
        match target {
            FieldKind::Len => self.parser.len |= (byte as u32) << shift,
            FieldKind::Id => self.parser.id |= (byte as u32) << shift,
            FieldKind::Type => self.parser.typ |= (byte as u32) << shift,
        }
        self.parser.field_idx += 1;

        // 3. 若字段已完整接收，切换到下一阶段
        if self.parser.field_idx >= width {
            self.parser.field_idx = 0;
            self.parser.stage = match target {
                FieldKind::Id => ParseStage::Len,
                FieldKind::Len => ParseStage::Type,
                FieldKind::Type => {
                    // TYPE 字段完成后，根据 LEN 决定走向
                    if self.parser.len as usize > RX {
                        // 载荷过大，记录错误并重置解析器
                        self.last_parse_error = Some(ParseError::PayloadTooLarge);
                        self.reset_parser();
                        return;
                    }
                    if self.parser.len == 0 {
                        // 无载荷：若校验和宽度为 0 则直接分发，否则进入头部校验和阶段
                        if K::WIDTH == 0 {
                            self.dispatch_zero_copy();
                            self.reset_parser();
                            return;
                        }
                        self.parser.checksum_idx = 0;
                        ParseStage::HeadChecksum
                    } else {
                        // 有载荷：同样根据校验和宽度决定是否先进行头部校验
                        if K::WIDTH == 0 {
                            self.parser.data_idx = 0;
                            self.parser.data_checksum = self.tx.checksum.start();
                            ParseStage::Data
                        } else {
                            self.parser.checksum_idx = 0;
                            ParseStage::HeadChecksum
                        }
                    }
                }
            };
        }
    }

    fn dispatch_zero_copy(&mut self) {
        let frame = ReceivedFrame {
            id: self.parser.id,
            typ: self.parser.typ,
            data: &self.rx_buf[..self.parser.len as usize],
            timed_out: false,
        };

        let mut channel = FrameChannel { tx: &mut self.tx };
        let mut handled = false;

        // 1. 优先匹配 ID 监听器（点对点请求/响应）
        for slot in &mut self.id_listeners {
            if let Some(listener) = slot.as_mut() {
                if listener.id == frame.id {
                    handled = true;
                    match (listener.on_frame)(&mut self.ctx, &mut channel, frame) {
                        ListenerAction::Close => *slot = None,
                        ListenerAction::Renew => listener.timeout_left = listener.timeout_max,
                        ListenerAction::Stay | ListenerAction::Next => {}
                    }
                }
            }
        }

        // 2. 其次匹配类型监听器（按消息类型处理）
        for slot in &mut self.type_listeners {
            if let Some(listener) = slot.as_mut() {
                if listener.typ == frame.typ {
                    handled = true;
                    if let ListenerAction::Close =
                        (listener.on_frame)(&mut self.ctx, &mut channel, frame)
                    {
                        *slot = None;
                    }
                }
            }
        }

        // 3. 若未被处理，则调用通用监听器（catch-all）
        if !handled {
            if let Some(cb) = self.generic_listener {
                cb(&mut self.ctx, &mut channel, frame);
            }
        }
    }
}
