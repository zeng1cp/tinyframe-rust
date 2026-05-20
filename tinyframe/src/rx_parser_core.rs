use crate::{Checksum, ParseError, parser::{ParseStage, Parser}, utils::FieldKind};

#[derive(Clone, Copy)]
pub(crate) struct ParsedFrameMeta {
    pub id: u32,
    pub typ: u32,
    pub len: usize,
}

pub(crate) struct RxParserCore<K: Checksum, const RX: usize> {
    pub parser_timeout_ticks: u16,
    pub parser: Parser<K>,
    pub rx_buf: [u8; RX],
    pub last_parse_error: Option<ParseError>,
}

impl<K: Checksum, const RX: usize> RxParserCore<K, RX> {
    pub fn new(parser_timeout_ticks: u16) -> Self {
        Self {
            parser_timeout_ticks,
            parser: Parser::default(),
            rx_buf: [0; RX],
            last_parse_error: None,
        }
    }

    pub fn reset_parser(&mut self) {
        self.parser = Parser::default();
    }

    pub fn tick(&mut self) {
        if self.parser.timeout > 0 {
            self.parser.timeout -= 1;
            if self.parser.timeout == 0 {
                self.reset_parser();
            }
        }
    }

    pub fn accept_byte(
        &mut self,
        byte: u8,
        sof: u8,
        checksum: &K,
        field_widths: (usize, usize, usize),
    ) -> Option<ParsedFrameMeta> {
        let (id_w, len_w, ty_w) = field_widths;
        if self.parser.stage != ParseStage::Sof {
            self.parser.timeout = self.parser_timeout_ticks;
        }

        match self.parser.stage {
            ParseStage::Sof => {
                if byte == sof {
                    self.parser = Parser {
                        stage: ParseStage::Id,
                        head_checksum: checksum.start(),
                        data_checksum: checksum.start(),
                        timeout: self.parser_timeout_ticks,
                        ..Parser::default()
                    };
                }
            }
            ParseStage::Id => self.parse_field_byte(byte, id_w, FieldKind::Id, checksum),
            ParseStage::Len => self.parse_field_byte(byte, len_w, FieldKind::Len, checksum),
            ParseStage::Type => self.parse_field_byte(byte, ty_w, FieldKind::Type, checksum),
            ParseStage::Data => {
                if self.parser.data_idx < RX {
                    self.rx_buf[self.parser.data_idx] = byte;
                }
                self.parser.data_checksum = checksum.add(self.parser.data_checksum, byte);
                self.parser.data_idx += 1;
                if self.parser.data_idx >= self.parser.len as usize {
                    if K::WIDTH == 0 {
                        return Some(self.decoded_meta());
                    }
                    self.parser.checksum_idx = 0;
                    self.parser.stage = ParseStage::DataChecksum;
                }
            }
            ParseStage::HeadChecksum => {
                if self.parser.checksum_idx < self.parser.checksum_buf.len() {
                    self.parser.checksum_buf[self.parser.checksum_idx] = byte;
                }
                self.parser.checksum_idx += 1;
                if self.parser.checksum_idx >= K::WIDTH {
                    let calc = checksum.finish(self.parser.head_checksum);
                    let recv = checksum.decode(&self.parser.checksum_buf[..K::WIDTH]);
                    if calc != recv {
                        self.last_parse_error = Some(ParseError::ChecksumMismatch);
                        self.reset_parser();
                    } else if self.parser.len == 0 {
                        return Some(self.decoded_meta());
                    } else {
                        self.parser.stage = ParseStage::Data;
                        self.parser.data_idx = 0;
                        self.parser.data_checksum = checksum.start();
                    }
                }
            }
            ParseStage::DataChecksum => {
                if self.parser.checksum_idx < self.parser.checksum_buf.len() {
                    self.parser.checksum_buf[self.parser.checksum_idx] = byte;
                }
                self.parser.checksum_idx += 1;
                if self.parser.checksum_idx >= K::WIDTH {
                    let calc = checksum.finish(self.parser.data_checksum);
                    let recv = checksum.decode(&self.parser.checksum_buf[..K::WIDTH]);
                    if calc != recv {
                        self.last_parse_error = Some(ParseError::ChecksumMismatch);
                        self.reset_parser();
                    } else {
                        return Some(self.decoded_meta());
                    }
                }
            }
        }
        None
    }

    fn parse_field_byte(&mut self, byte: u8, width: usize, target: FieldKind, checksum: &K) {
        self.parser.head_checksum = checksum.add(self.parser.head_checksum, byte);
        let shift = ((width - 1 - self.parser.field_idx) * 8) as u32;
        match target {
            FieldKind::Len => self.parser.len |= (byte as u32) << shift,
            FieldKind::Id => self.parser.id |= (byte as u32) << shift,
            FieldKind::Type => self.parser.typ |= (byte as u32) << shift,
        }
        self.parser.field_idx += 1;
        if self.parser.field_idx >= width {
            self.parser.field_idx = 0;
            self.parser.stage = match target {
                FieldKind::Id => ParseStage::Len,
                FieldKind::Len => ParseStage::Type,
                FieldKind::Type => {
                    if self.parser.len as usize > RX {
                        self.last_parse_error = Some(ParseError::PayloadTooLarge);
                        self.reset_parser();
                        return;
                    }
                    if self.parser.len == 0 {
                        if K::WIDTH == 0 {
                            return;
                        }
                        self.parser.checksum_idx = 0;
                        ParseStage::HeadChecksum
                    } else if K::WIDTH == 0 {
                        self.parser.data_idx = 0;
                        self.parser.data_checksum = checksum.start();
                        ParseStage::Data
                    } else {
                        self.parser.checksum_idx = 0;
                        ParseStage::HeadChecksum
                    }
                }
            };
        }
    }

    fn decoded_meta(&self) -> ParsedFrameMeta {
        ParsedFrameMeta { id: self.parser.id, typ: self.parser.typ, len: self.parser.len as usize }
    }
}
