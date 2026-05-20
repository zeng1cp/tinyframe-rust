use crate::{
    Checksum, Error, Frame, FrameCallback, FrameChannel, ParseError, Peer,
    ReceivedFrame, Transport,
    listener::{IdListener, ListenerId, TypeListener},
    rx_dispatch_core::RxDispatchCore,
    rx_parser_core::{ParsedFrameMeta, RxParserCore},
    strategy::{DispatchPolicy, IdThenTypeDispatch, SequentialIdAllocator},
    tx_core::TxCore,
    utils::is_valid_width,
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
    A = SequentialIdAllocator,
    D = IdThenTypeDispatch,
> where
    T: Transport,
    K: Checksum,
    A: crate::strategy::IdAllocator,
    D: DispatchPolicy,
{
    ctx: C,
    tx: TxCore<T, K, A, ID, LEN, TY>,
    rx: RxParserCore<K, RX>,
    id_listeners: [Option<IdListener<C, T, K, A, ID, LEN, TY>>; IDS],
    type_listeners: [Option<TypeListener<C, T, K, A, ID, LEN, TY>>; TYPES],
    generic_listener: Option<FrameCallback<C, T, K, A, ID, LEN, TY>>,
    dispatch_policy: D,
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
    A,
    D,
> TinyFrame<C, T, K, RX, IDS, TYPES, ID, LEN, TY, A, D>
where
    T: Transport,
    K: Checksum,
    A: crate::strategy::IdAllocator + Default,
    D: DispatchPolicy + Default,
{
    /// Create a `TinyFrame` with default façade parameters.
    ///
    /// Defaults:
    /// - `peer`: `Peer::Master`
    /// - `sof`: `0x01`
    /// - `parser_timeout_ticks`: `10`
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{TinyFrame, BufferTransport, NoChecksum};
    ///
    /// // 1) 给出一个常用类型别名，隐藏复杂泛型参数。
    /// type Tf = TinyFrame<(), BufferTransport<256>, NoChecksum, 64, 4, 4, 1, 1, 1>;
    /// // 2) 使用 new_simple 快速创建：默认 Master、SOF=0x01、超时 tick=10。
    /// let _tf = Tf::new_simple((), BufferTransport::new(), NoChecksum).unwrap();
    /// ```
    ///
    /// # UART + CRC 示例（更接近嵌入式真实场景）
    /// ```no_run
    /// use tinyframe::{TinyFrame, Transport, Crc16};
    ///
    /// // 模拟一个 UART 发送后端（真实项目里可包一层串口驱动 HAL）。
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
    ///     // 这里用 () 简化错误类型；真实项目可替换为串口驱动自己的错误枚举。
    ///     type Error = ();
    ///     fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
    ///         let n = core::cmp::min(bytes.len(), self.tx_log.len().saturating_sub(self.len));
    ///         self.tx_log[self.len..self.len + n].copy_from_slice(&bytes[..n]);
    ///         self.len += n;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// // 使用 CRC16 作为校验算法；字段宽度示例中 ID/LEN/TYPE 都是 1 字节。
    /// type TfUart = TinyFrame<(), UartTransport, Crc16, 128, 4, 4, 1, 1, 1>;
    /// let _tf = TfUart::new_simple((), UartTransport::new(), Crc16).unwrap();
    /// ```
    pub fn new_simple(
        ctx: C,
        transport: T,
        checksum: K,
    ) -> Result<Self, Error<T::Error>> {
        Self::new(ctx, transport, checksum, Peer::Master, 0x01, 10)
    }

    /// Create a TinyFrame engine instance.
    ///
    /// # Type Parameters
    /// - `RX`: max accepted payload length in bytes.
    /// - `IDS`: ID listener slot count.
    /// - `TYPES`: type listener slot count.
    /// - `ID`/`LEN`/`TY`: encoded field widths in bytes (1..=4).
    ///
    /// # Example
    /// ```no_run
    /// use tinyframe::{TinyFrame, BufferTransport, NoChecksum, Peer};
    ///
    /// // 明确指定全部构造参数，适合需要精细控制场景。
    /// type Tf = TinyFrame<(), BufferTransport<256>, NoChecksum, 64, 4, 4, 1, 1, 1>;
    /// // 参数解释：
    /// // - Peer::Master: 本端角色
    /// // - 0x01: 帧起始字节 SOF
    /// // - 10: 解析超时 tick
    /// let _tf = Tf::new((), BufferTransport::new(), NoChecksum, Peer::Master, 0x01, 10).unwrap();
    /// ```
    ///
    /// # 结合 UART 传输与 CRC32 校验
    /// ```no_run
    /// use tinyframe::{TinyFrame, Transport, Peer, Crc32};
    ///
    /// // 该示例展示如何把自定义传输层（UART）与 CRC 校验一起接入。
    /// struct UartTransport {
    ///     tx_log: [u8; 1024],
    ///     len: usize,
    /// }
    ///
    /// impl UartTransport {
    ///     fn new() -> Self { Self { tx_log: [0; 1024], len: 0 } }
    /// }
    ///
    /// impl Transport for UartTransport {
    ///     type Error = ();
    ///     fn begin_frame(&mut self) -> Result<(), Self::Error> {
    ///         // 可选：例如拉高片选、打时间戳等。
    ///         Ok(())
    ///     }
    ///     fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
    ///         let n = core::cmp::min(bytes.len(), self.tx_log.len().saturating_sub(self.len));
    ///         self.tx_log[self.len..self.len + n].copy_from_slice(&bytes[..n]);
    ///         self.len += n;
    ///         Ok(())
    ///     }
    ///     fn end_frame(&mut self) -> Result<(), Self::Error> {
    ///         // 可选：例如 flush UART DMA。
    ///         Ok(())
    ///     }
    /// }
    ///
    /// // 这里选择 Crc32，接收缓冲区更大一些（RX=256）。
    /// type TfUartCrc = TinyFrame<(), UartTransport, Crc32, 256, 8, 8, 1, 2, 1>;
    ///
    /// let _tf = TfUartCrc::new(
    ///     (),                  // 用户上下文
    ///     UartTransport::new(),// 自定义 UART 传输
    ///     Crc32,               // CRC 校验策略
    ///     Peer::Slave,         // 本端角色
    ///     0xA5,                // SOF
    ///     20,                  // parser 超时 tick
    /// ).unwrap();
    /// ```
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
                id_allocator: A::default(),
            },
            rx: RxParserCore::new(parser_timeout_ticks),
            id_listeners: core::array::from_fn(|_| None),
            type_listeners: core::array::from_fn(|_| None),
            generic_listener: None,
            dispatch_policy: D::default(),
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
        self.rx.last_parse_error
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
        on_frame: FrameCallback<C, T, K, A, ID, LEN, TY>,
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
        on_frame: FrameCallback<C, T, K, A, ID, LEN, TY>,
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
    pub fn set_generic_listener(&mut self, cb: FrameCallback<C, T, K, A, ID, LEN, TY>) {
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
        self.rx.tick();

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
        self.rx.reset_parser();
    }

    /// Feed a byte slice into streaming parser.
    pub fn accept(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.accept_byte(b);
        }
    }

    /// Feed one byte into streaming parser.
    pub fn accept_byte(&mut self, byte: u8) {
        let frame = self
            .rx
            .accept_byte(byte, self.tx.sof, &self.tx.checksum, (ID, LEN, TY));
        if let Some(ParsedFrameMeta { id, typ, len }) = frame {
            let frame = ReceivedFrame {
                id,
                typ,
                data: &self.rx.rx_buf[..len],
                timed_out: false,
            };
            RxDispatchCore::dispatch(
                &mut self.ctx,
                &mut self.tx,
                &self.dispatch_policy,
                &mut self.id_listeners,
                &mut self.type_listeners,
                self.generic_listener,
                frame,
            );
            self.rx.reset_parser();
        }
    }
}
