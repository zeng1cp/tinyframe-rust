use crate::{
    Checksum, Error, Frame, Transport,
    strategy::IdAllocator,
    tx_pipeline::TxPipeline,
    utils::{fits_in_bytes, is_valid_width},
};

#[derive(Clone, Copy)]
pub(crate) struct MultipartTx<S: Copy> {
    pub expected_len: u32,
    pub sent: u32,
    pub data_checksum: S,
}

pub(crate) struct TxCore<T, K, A, const ID: usize, const LEN: usize, const TY: usize>
where
    T: Transport,
    K: Checksum,
    A: IdAllocator,
{
    pub transport: T,
    pub checksum: K,
    pub sof: u8,
    pub peer_master: bool,
    pub tx_busy: bool,
    pub multipart: Option<MultipartTx<K::Value>>,
    pub next_id: u32,
    pub id_allocator: A,
}

impl<T, K, A, const ID: usize, const LEN: usize, const TY: usize> TxCore<T, K, A, ID, LEN, TY>
where
    T: Transport,
    K: Checksum,
    A: IdAllocator,
{
    pub fn send(&mut self, mut frame: Frame, data: &[u8]) -> Result<(), Error<T::Error>> {
        if !is_valid_width(ID) || !is_valid_width(LEN) || !is_valid_width(TY) {
            return Err(Error::InvalidFieldWidth);
        }
        if K::WIDTH > 4 {
            return Err(Error::InvalidFieldWidth);
        }
        if self.tx_busy || self.multipart.is_some() {
            return Err(Error::TxBusy);
        }

        let len_u32 = u32::try_from(data.len()).map_err(|_| Error::LengthOverflow)?;
        if !fits_in_bytes(len_u32, LEN)
            || !fits_in_bytes(frame.id, ID)
            || !fits_in_bytes(frame.typ, TY)
        {
            return Err(Error::LengthOverflow);
        }

        if !frame.is_response {
            frame.id = if frame.id == 0 {
                self.alloc_id()
            } else {
                frame.id
            };
        }

        self.tx_busy = true;
        let result = (|| {
            let head_csum = TxPipeline::write_header::<T, K, ID, LEN, TY>(
                &mut self.transport,
                &self.checksum,
                self.sof,
                frame.id,
                len_u32,
                frame.typ,
            )?;
            TxPipeline::write_checksum(&mut self.transport, &self.checksum, head_csum)?;

            self.transport.write(data).map_err(Error::Transport)?;
            if K::WIDTH > 0 && !data.is_empty() {
                let mut data_csum = self.checksum.start();
                for &b in data {
                    data_csum = self.checksum.add(data_csum, b);
                }
                let sum = self.checksum.finish(data_csum);
                let mut cksum_buf = [0u8; 4];
                let used = self.checksum.encode(sum, &mut cksum_buf);
                self.transport
                    .write(&cksum_buf[..used])
                    .map_err(Error::Transport)?;
            }

            self.transport.end_frame().map_err(Error::Transport)?;
            Ok(())
        })();
        self.tx_busy = false;
        if result.is_err() {
            self.transport.abort_frame();
        }
        result
    }

    fn alloc_id(&mut self) -> u32 {
        self.id_allocator
            .alloc_id::<ID>(&mut self.next_id, self.peer_master)
    }

    pub fn begin_multipart(&mut self, mut frame: Frame, len: u32) -> Result<u32, Error<T::Error>> {
        if self.tx_busy || self.multipart.is_some() {
            return Err(Error::TxBusy);
        }
        if !fits_in_bytes(len, LEN) || !fits_in_bytes(frame.id, ID) || !fits_in_bytes(frame.typ, TY)
        {
            return Err(Error::LengthOverflow);
        }
        if !frame.is_response {
            frame.id = if frame.id == 0 {
                self.alloc_id()
            } else {
                frame.id
            };
        }

        self.tx_busy = true;
        let result = (|| {
            self.transport.begin_frame().map_err(Error::Transport)?;

            let head_csum = TxPipeline::write_header::<T, K, ID, LEN, TY>(
                &mut self.transport,
                &self.checksum,
                self.sof,
                frame.id,
                len,
                frame.typ,
            )?;
            TxPipeline::write_checksum(&mut self.transport, &self.checksum, head_csum)?;

            self.multipart = Some(MultipartTx {
                expected_len: len,
                sent: 0,
                data_checksum: self.checksum.start(),
            });
            Ok(frame.id)
        })();
        self.tx_busy = false;
        if result.is_err() {
            self.transport.abort_frame();
        }
        result
    }

    pub fn multipart_payload(&mut self, data: &[u8]) -> Result<(), Error<T::Error>> {
        let part = self.multipart.as_mut().ok_or(Error::MultipartNotStarted)?;
        if let Err(err) = self.transport.write(data) {
            self.multipart = None;
            self.transport.abort_frame();
            return Err(Error::Transport(err));
        }
        for &b in data {
            part.data_checksum = self.checksum.add(part.data_checksum, b);
        }
        part.sent = part.sent.saturating_add(data.len() as u32);
        Ok(())
    }

    pub fn multipart_close(&mut self) -> Result<(), Error<T::Error>> {
        let part = self.multipart.take().ok_or(Error::MultipartNotStarted)?;
        if part.sent != part.expected_len {
            self.transport.abort_frame();
            return Err(Error::MultipartLengthMismatch);
        }
        if K::WIDTH > 0 && part.expected_len > 0 {
            let mut cksum_buf = [0u8; 4];
            let used = self
                .checksum
                .encode(self.checksum.finish(part.data_checksum), &mut cksum_buf);
            if let Err(err) = self.transport.write(&cksum_buf[..used]) {
                self.transport.abort_frame();
                return Err(Error::Transport(err));
            }
        }
        if let Err(err) = self.transport.end_frame() {
            self.transport.abort_frame();
            return Err(Error::Transport(err));
        }
        Ok(())
    }
}
