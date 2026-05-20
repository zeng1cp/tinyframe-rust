
#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests {
    use super::*;

    fn calc<K: Checksum>(ck: K, bytes: &[u8]) -> K::Value {
        let mut acc = ck.start();
        for &b in bytes {
            acc = ck.add(acc, b);
        }
        ck.finish(acc)
    }

    type Tf = TinyFrame<Ctx, BufferTransport<256>, NoChecksum, 64, 4, 4, 1, 1, 1>;

    #[derive(Default)]
    struct Ctx {
        seen_len: usize,
        seen_type: u32,
        timed_out: bool,
    }

    fn type_listener(
        ctx: &mut Ctx,
        tx: &mut FrameChannel<'_, BufferTransport<256>, NoChecksum, 1, 1, 1>,
        frame: ReceivedFrame<'_>,
    ) -> ListenerAction {
        if frame.timed_out {
            ctx.timed_out = true;
            return ListenerAction::Close;
        }
        ctx.seen_len = frame.data.len();
        ctx.seen_type = frame.typ;
        tx.send(
            Frame {
                id: frame.id,
                typ: 0x21,
                is_response: true,
            },
            &[9, 8],
        )
        .unwrap();
        ListenerAction::Stay
    }

    #[test]
    fn zero_copy_parse_and_listener_can_send() {
        let mut tf = Tf::new(
            Ctx::default(),
            BufferTransport::new(),
            NoChecksum,
            Peer::Slave,
            0x01,
            10,
        )
        .unwrap();
        tf.add_type_listener(0x07, type_listener).unwrap();

        // NoChecksum => frame has no trailing checksum byte.
        tf.accept(&[0x01, 0x11, 0x03, 0x07, 1, 2, 3]);

        assert_eq!(tf.context().seen_len, 3);
        assert_eq!(tf.context().seen_type, 0x07);
        assert!(!tf.transport_mut().bytes().is_empty());
    }

    #[test]
    fn remove_listener_apis_work() {
        let mut tf = Tf::new(
            Ctx::default(),
            BufferTransport::new(),
            NoChecksum,
            Peer::Slave,
            0x01,
            10,
        )
        .unwrap();
        let id_slot = tf.add_id_listener(0x44, 0, type_listener).unwrap();
        let type_slot = tf.add_type_listener(0x12, type_listener).unwrap();

        assert!(tf.remove_id_listener(id_slot));
        assert!(tf.remove_type_listener(type_slot));
        assert!(!tf.remove_id_listener(99));
        assert!(!tf.remove_type_listener(99));
    }

    #[test]
    fn listener_timeout_and_renew() {
        let mut tf = Tf::new(
            Ctx::default(),
            BufferTransport::new(),
            NoChecksum,
            Peer::Slave,
            0x01,
            10,
        )
        .unwrap();
        tf.add_id_listener(0x55, 2, type_listener).unwrap();
        assert!(tf.renew_id_listener(0x55));
        tf.tick();
        tf.tick();
        assert!(tf.context().timed_out);
    }

    #[test]
    fn configurable_field_width_send() {
        type TfWide = TinyFrame<Ctx, BufferTransport<256>, NoChecksum, 64, 2, 2, 2, 2, 2>;
        let mut tf = TfWide::new(
            Ctx::default(),
            BufferTransport::new(),
            NoChecksum,
            Peer::Master,
            0x01,
            10,
        )
        .unwrap();
        tf.send(
            Frame {
                id: 0x1234,
                typ: 0x00AA,
                is_response: true,
            },
            &[1, 2],
        )
        .unwrap();

        let bytes = tf.transport_mut().bytes();
        assert_eq!(bytes[0], 0x01);
        assert_eq!(&bytes[1..3], &[0x12, 0x34]);
        assert_eq!(&bytes[3..5], &[0x00, 0x02]);
        assert_eq!(&bytes[5..7], &[0x00, 0xAA]);
    }

    #[test]
    fn no_checksum_send_has_no_trailing_checksum_byte() {
        let mut tf = Tf::new(
            Ctx::default(),
            BufferTransport::new(),
            NoChecksum,
            Peer::Master,
            0x01,
            10,
        )
        .unwrap();
        tf.send(
            Frame {
                id: 0x11,
                typ: 0x22,
                is_response: true,
            },
            &[0xAA, 0xBB],
        )
        .unwrap();

        let bytes = tf.transport_mut().bytes();
        // SOF + ID + LEN + TYPE + DATA(2)
        assert_eq!(bytes.len(), 1 + 1 + 1 + 1 + 2);
        assert_eq!(bytes, &[0x01, 0x11, 0x02, 0x22, 0xAA, 0xBB]);
    }

    #[derive(Clone, Copy, Default)]
    struct Sum8;

    impl Checksum for Sum8 {
        type Value = u8;
        const WIDTH: usize = 1;

        fn start(&self) -> Self::Value {
            0
        }
        fn add(&self, sum: Self::Value, byte: u8) -> Self::Value {
            sum.wrapping_add(byte)
        }
        fn finish(&self, sum: Self::Value) -> Self::Value {
            sum
        }
        fn encode(&self, sum: Self::Value, out: &mut [u8]) -> usize {
            out[0] = sum;
            1
        }
        fn decode(&self, bytes: &[u8]) -> Self::Value {
            bytes[0]
        }
    }

    #[test]
    fn checksum_does_not_include_sof() {
        type TfCk = TinyFrame<Ctx, BufferTransport<256>, Sum8, 64, 2, 2, 1, 1, 1>;
        let mut tf = TfCk::new(
            Ctx::default(),
            BufferTransport::new(),
            Sum8,
            Peer::Slave,
            0x01,
            10,
        )
        .unwrap();

        // header checksum = ID+LEN+TYPE = 2+1+3 = 6, data checksum = 4
        tf.accept(&[0x01, 0x02, 0x01, 0x03, 0x06, 0x04, 0x04]);
        assert_eq!(tf.last_parse_error(), None);

        // if SOF were included header checksum would be 7, so this frame must fail
        tf.accept(&[0x01, 0x02, 0x01, 0x03, 0x07, 0x04, 0x04]);
        assert_eq!(tf.last_parse_error(), Some(ParseError::ChecksumMismatch));
    }

    #[test]
    fn builtin_checksum_vectors() {
        let v = b"123456789";
        assert_eq!(calc(Crc8Maxim, v), 0xA1);
        assert_eq!(calc(Crc16, v), 0xBB3D);
        assert_eq!(calc(Crc32, v), 0xCBF4_3926);
        assert_eq!(calc(XorChecksum, &[1, 2, 3]), !(1 ^ 2 ^ 3));
    }

    #[derive(Clone, Copy, Default)]
    struct Custom1;

    impl Checksum for Custom1 {
        type Value = u8;
        const WIDTH: usize = 1;

        fn start(&self) -> Self::Value {
            0
        }
        fn add(&self, sum: Self::Value, byte: u8) -> Self::Value {
            sum.wrapping_add(byte)
        }
        fn finish(&self, sum: Self::Value) -> Self::Value {
            sum
        }
        fn encode(&self, sum: Self::Value, out: &mut [u8]) -> usize {
            out[0] = sum;
            1
        }
        fn decode(&self, bytes: &[u8]) -> Self::Value {
            bytes[0]
        }
    }

    #[test]
    fn custom_checksum_support_via_trait_impl() {
        type TfCustom = TinyFrame<Ctx, BufferTransport<256>, Custom1, 64, 2, 2, 1, 1, 1>;
        let mut tf = TfCustom::new(
            Ctx::default(),
            BufferTransport::new(),
            Custom1,
            Peer::Master,
            0x01,
            10,
        )
        .unwrap();
        tf.send(
            Frame {
                id: 0x04,
                typ: 0x01,
                is_response: true,
            },
            &[0x02, 0x03],
        )
        .unwrap();
        // Rust port now follows TinyFrame-style split checksums: HEAD_CKSUM then DATA_CKSUM.
        assert_eq!(
            tf.transport_mut().bytes(),
            &[0x01, 0x04, 0x02, 0x01, 0x07, 0x02, 0x03, 0x05]
        );
    }

    #[test]
    fn multipart_send_header_payload_and_close() {
        type TfCustom = TinyFrame<Ctx, BufferTransport<256>, Custom1, 64, 2, 2, 1, 1, 1>;
        let mut tf = TfCustom::new(
            Ctx::default(),
            BufferTransport::new(),
            Custom1,
            Peer::Master,
            0x01,
            10,
        )
        .unwrap();
        let frame_id = tf
            .send_multipart(
                Frame {
                    id: 0x22,
                    typ: 0x10,
                    is_response: true,
                },
                3,
            )
            .unwrap();
        assert_eq!(frame_id, 0x22);
        tf.multipart_payload(&[1, 2]).unwrap();
        tf.multipart_payload(&[3]).unwrap();
        tf.multipart_close().unwrap();

        // SOF ID LEN TYPE HEAD_CKSUM DATA DATA_CKSUM
        assert_eq!(
            tf.transport_mut().bytes(),
            &[0x01, 0x22, 0x03, 0x10, 0x35, 1, 2, 3, 0x06]
        );
    }

    #[derive(Default)]
    struct HookTransport {
        buf: [u8; 32],
        len: usize,
        begin_count: usize,
        end_count: usize,
        abort_count: usize,
    }

    impl HookTransport {
        fn bytes(&self) -> &[u8] {
            &self.buf[..self.len]
        }
    }

    impl Transport for HookTransport {
        type Error = ();

        fn begin_frame(&mut self) -> Result<(), Self::Error> {
            self.begin_count += 1;
            self.len = 0;
            Ok(())
        }

        fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
            let next = self.len + bytes.len();
            self.buf[self.len..next].copy_from_slice(bytes);
            self.len = next;
            Ok(())
        }

        fn end_frame(&mut self) -> Result<(), Self::Error> {
            self.end_count += 1;
            Ok(())
        }

        fn abort_frame(&mut self) {
            self.abort_count += 1;
            self.len = 0;
        }
    }

    #[test]
    fn transport_frame_hooks_wrap_send_once() {
        type TfHooks = TinyFrame<Ctx, HookTransport, NoChecksum, 64, 2, 2, 1, 1, 1>;
        let mut tf = TfHooks::new(
            Ctx::default(),
            HookTransport::default(),
            NoChecksum,
            Peer::Slave,
            0x01,
            10,
        )
        .unwrap();

        tf.send(
            Frame {
                id: 0x10,
                typ: 0x02,
                is_response: true,
            },
            &[0xAA],
        )
        .unwrap();

        let transport = tf.transport_mut();
        assert_eq!(transport.begin_count, 1);
        assert_eq!(transport.end_count, 1);
        assert_eq!(transport.abort_count, 0);
        assert_eq!(transport.bytes(), &[0x01, 0x10, 0x01, 0x02, 0xAA]);
    }
}
