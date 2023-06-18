use crate::error::Error;

pub trait RowBinary: hyper::body::Buf {
    fn done(&self) -> bool {
        !self.has_remaining()
    }
    fn read_u8(&mut self) -> Result<u8, Error> {
        if self.has_remaining() {
            Ok(self.get_u8())
        } else {
            Err(Error::NotEnoughData)
        }
    }
    fn read_bytes(&mut self, len: usize) -> Result<hyper::body::Bytes, Error> {
        if self.remaining() < len {
            Err(Error::NotEnoughData)
        } else {
            Ok(self.copy_to_bytes(len))
        }
    }
    fn read_leb128(&mut self) -> Result<u64, Error> {
        let mut result = 0;
        let mut shift = 0;
        loop {
            let byte = self.read_u8()?;
            result |= ((byte & 127) as u64) << shift;
            if byte & 128 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }
}

impl<B: hyper::body::Buf> RowBinary for B {}

pub trait Column: Sized {
    const TYPE: &'static str;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error>;
}

pub trait Row: Sized {
    const TYPES: &'static [&'static str];
    fn read(buf: &mut impl RowBinary) -> Result<Self, Error>;
}
impl<C: Column> Row for C {
    const TYPES: &'static [&'static str] = &[Self::TYPE];
    fn read(buf: &mut impl RowBinary) -> Result<Self, Error> {
        Self::read_value(buf)
    }
}

impl Column for String {
    const TYPE: &'static str = "String";
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let l = buf.read_leb128()?;
        let raw = buf.read_bytes(l as usize)?;
        Ok(String::from_utf8(raw.to_vec())?)
    }
}

impl Column for u8 {
    const TYPE: &'static str = "UInt8";
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        buf.read_u8()
    }
}
