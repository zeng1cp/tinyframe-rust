// 解析辅助函数（如 parse_field_byte）可以作为 Parser 的方法实现

#[derive(Clone, Copy)]
pub(crate) enum FieldKind {
    Len,
    Id,
    Type,
}

pub(crate) fn is_valid_width(width: usize) -> bool {
    (1..=4).contains(&width)
}

/// Returns whether value `v` is representable in a big-endian `width`-byte field.
pub(crate) fn fits_in_bytes(v: u32, width: usize) -> bool {
    if width == 4 {
        true
    } else {
        v <= ((1u32 << (width * 8)) - 1)
    }
}

/// Encode a `u32` value into the first `width` bytes of `out` in big-endian order.
pub(crate) fn encode_be(v: u32, width: usize, out: &mut [u8; 4]) {
    for (i, b) in out[..width].iter_mut().enumerate() {
        let shift = ((width - 1 - i) * 8) as u32;
        *b = ((v >> shift) & 0xFF) as u8;
    }
}
