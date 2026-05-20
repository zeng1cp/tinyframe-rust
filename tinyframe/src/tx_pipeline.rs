use crate::{Checksum, Error, Transport, utils::encode_be};

pub(crate) struct TxPipeline;

impl TxPipeline {
    pub fn write_header<T, K, const ID: usize, const LEN: usize, const TY: usize>(
        transport: &mut T,
        checksum: &K,
        sof: u8,
        frame_id: u32,
        len: u32,
        typ: u32,
    ) -> Result<K::Value, Error<T::Error>>
    where
        T: Transport,
        K: Checksum,
    {
        let mut head_csum = checksum.start();
        transport.begin_frame().map_err(Error::Transport)?;
        transport.write(&[sof]).map_err(Error::Transport)?;

        let mut field_buf = [0u8; 4];
        encode_be(frame_id, ID, &mut field_buf);
        transport.write(&field_buf[..ID]).map_err(Error::Transport)?;
        for &b in &field_buf[..ID] {
            head_csum = checksum.add(head_csum, b);
        }

        encode_be(len, LEN, &mut field_buf);
        transport.write(&field_buf[..LEN]).map_err(Error::Transport)?;
        for &b in &field_buf[..LEN] {
            head_csum = checksum.add(head_csum, b);
        }

        encode_be(typ, TY, &mut field_buf);
        transport.write(&field_buf[..TY]).map_err(Error::Transport)?;
        for &b in &field_buf[..TY] {
            head_csum = checksum.add(head_csum, b);
        }
        Ok(head_csum)
    }

    pub fn write_checksum<T, K>(
        transport: &mut T,
        checksum: &K,
        state: K::Value,
    ) -> Result<(), Error<T::Error>>
    where
        T: Transport,
        K: Checksum,
    {
        if K::WIDTH > 0 {
            let mut cksum_buf = [0u8; 4];
            let used = checksum.encode(checksum.finish(state), &mut cksum_buf);
            transport.write(&cksum_buf[..used]).map_err(Error::Transport)?;
        }
        Ok(())
    }
}
