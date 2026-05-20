/// Parse-time protocol errors recorded by [`TinyFrame::last_parse_error`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidLength,
    PayloadTooLarge,
    ChecksumMismatch,
    InvalidChecksumWidth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error<E> {
    /// Underlying transport write failure.
    Transport(E),
    /// Invalid compile-time field-width configuration.
    InvalidFieldWidth,
    /// Runtime value cannot fit configured field width.
    LengthOverflow,
    /// TX core is busy (re-entrant send during active TX transaction).
    TxBusy,
    /// Multipart TX is not active.
    MultipartNotStarted,
    /// Multipart TX closed with mismatched payload length.
    MultipartLengthMismatch,
    /// Listener table has no free slot.
    NoListenerSlot,
}