use crate::{Checksum, Error, Frame, ReceivedFrame, Transport, tx_core::TxCore};

pub struct FrameChannel<'a, T, K, const ID: usize, const LEN: usize, const TY: usize>
where
    T: Transport,
    K: Checksum,
{
    pub(crate) tx: &'a mut TxCore<T, K, ID, LEN, TY>,
}

impl<'a, T, K, const ID: usize, const LEN: usize, const TY: usize>
    FrameChannel<'a, T, K, ID, LEN, TY>
where
    T: Transport,
    K: Checksum,
{
    /// Send an arbitrary frame from within listener callback context.
    pub fn send(&mut self, frame: Frame, data: &[u8]) -> Result<(), Error<T::Error>> {
        self.tx.send(frame, data)
    }

    /// Convenience API to reply to a received request frame.
    pub fn respond(
        &mut self,
        request: ReceivedFrame<'_>,
        typ: u32,
        data: &[u8],
    ) -> Result<(), Error<T::Error>> {
        self.tx.send(
            Frame {
                id: request.id,
                typ,
                is_response: true,
            },
            data,
        )
    }

    pub fn begin_multipart(&mut self, frame: Frame, len: u32) -> Result<u32, Error<T::Error>> {
        self.tx.begin_multipart(frame, len)
    }

    pub fn multipart_payload(&mut self, data: &[u8]) -> Result<(), Error<T::Error>> {
        self.tx.multipart_payload(data)
    }

    pub fn multipart_close(&mut self) -> Result<(), Error<T::Error>> {
        self.tx.multipart_close()
    }
}
