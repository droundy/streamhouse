use hyper::body::Buf;

use crate::error::Error;

pub trait WriteRowBinary {
    fn write_u8(&mut self, value: u8) -> Result<(), Error>;
    fn write_leb128(&mut self, mut value: u64) -> Result<(), Error> {
        loop {
            if value < 128 {
                self.write_u8(value as u8)?;
                return Ok(());
            } else {
                self.write_u8(value as u8)?;
                value = value >> 7;
            }
        }
    }
}
impl WriteRowBinary for Vec<u8> {
    fn write_u8(&mut self, value: u8) -> Result<(), Error> {
        self.push(value);
        Ok(())
    }
}
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
    const TYPE: ColumnType;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error>;
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error>;
}

pub trait Row: Sized {
    const TYPES: &'static [ColumnType];
    fn read(buf: &mut impl RowBinary) -> Result<Self, Error>;
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error>;
}
impl<C: Column> Row for C {
    const TYPES: &'static [ColumnType] = &[Self::TYPE];
    fn read(buf: &mut impl RowBinary) -> Result<Self, Error> {
        Self::read_value(buf)
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.write_value(buf)
    }
}

impl Column for String {
    const TYPE: ColumnType = ColumnType::String;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let l = buf.read_leb128()?;
        let raw = buf.read_bytes(l as usize)?;
        Ok(String::from_utf8(raw.to_vec())?)
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_leb128(self.len() as u64)?;
        for b in self.as_bytes() {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

impl Column for Vec<u8> {
    const TYPE: ColumnType = ColumnType::String;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let l = buf.read_leb128()?;
        Ok(buf.read_bytes(l as usize)?.to_vec())
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_leb128(self.len() as u64)?;
        for b in self {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

impl<const N: usize> Column for [u8; N] {
    const TYPE: ColumnType = ColumnType::FixedString(N);
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let bytes = buf.read_bytes(N)?;
        Ok((&*bytes)[0..N].try_into().unwrap())
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        for b in self {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

impl Column for u8 {
    const TYPE: ColumnType = ColumnType::UInt8;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        buf.read_u8()
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_u8(*self)
    }
}

impl Column for u16 {
    const TYPE: ColumnType = ColumnType::UInt16;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let x = <[u8; 2]>::read(buf)?;
        Ok(u16::from_le_bytes(x))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

impl Column for u32 {
    const TYPE: ColumnType = ColumnType::UInt32;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let x = <[u8; 4]>::read(buf)?;
        Ok(Self::from_le_bytes(x))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

impl Column for u64 {
    const TYPE: ColumnType = ColumnType::UInt64;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let x = <[u8; 8]>::read(buf)?;
        Ok(Self::from_le_bytes(x))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

impl Column for u128 {
    const TYPE: ColumnType = ColumnType::UInt128;
    fn read_value(buf: &mut impl RowBinary) -> Result<Self, Error> {
        let x = <[u8; 16]>::read(buf)?;
        Ok(Self::from_le_bytes(x))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ColumnType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    String,
    FixedString(usize),
}

impl ColumnType {
    pub fn read(bytes: &[u8]) -> Result<Self, Error> {
        match bytes {
            b"UInt8" => Ok(Self::UInt8),
            b"UInt16" => Ok(Self::UInt16),
            b"UInt32" => Ok(Self::UInt32),
            b"UInt64" => Ok(Self::UInt64),
            b"UInt128" => Ok(Self::UInt128),
            b"String" => Ok(Self::String),
            _ => {
                if bytes.starts_with(b"FixedString(") && bytes.last() == Some(&(')' as u8)) {
                    let mut len = 0;
                    for b in &bytes[b"FixedString(".len()..bytes.len() - 1] {
                        len += (*b - '0' as u8) as usize;
                    }
                    Ok(Self::FixedString(len))
                } else {
                    Err(Error::UnsupportedColumn(
                        String::from_utf8_lossy(bytes).into_owned(),
                    ))
                }
            }
        }
    }
}
