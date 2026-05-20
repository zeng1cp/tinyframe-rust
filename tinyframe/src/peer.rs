/// TinyFrame peer role.
///
/// The highest ID bit inside configured ID-width is reserved as peer-bit.
/// `Master` sets it to `1`, `Slave` keeps it `0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    Master,
    Slave,
}