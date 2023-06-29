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

fn read_u8(buf: &[u8]) -> Result<(u8, &[u8]), Error> {
    if let Some((f, rest)) = buf.split_first() {
        Ok((*f, rest))
    } else {
        Err(Error::NotEnoughData)
    }
}

fn read_bytes(buf: &[u8], len: usize) -> Result<(&[u8], &[u8]), Error> {
    if buf.len() < len {
        Err(Error::NotEnoughData)
    } else {
        Ok(buf.split_at(len))
    }
}

fn read_leb128(mut buf: &[u8]) -> Result<(u64, &[u8]), Error> {
    let mut result = 0;
    let mut shift = 0;
    let mut byte;
    loop {
        (byte, buf) = read_u8(buf)?;
        result |= ((byte & 127) as u64) << shift;
        if byte & 128 == 0 {
            return Ok((result, buf));
        }
        shift += 7;
    }
}

pub trait Column: Sized {
    const TYPE: ColumnType;
    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), Error>;
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error>;
}

/// FIXME rename this to `Column`.
pub struct AColumn {
    pub(crate) name: &'static str,
    pub(crate) column_type: &'static ColumnType,
}

pub trait Row: Sized {
    /// The set of columns in this row.
    ///
    /// The parent is the name of this row, if it has a name, which is useful
    /// for rows that are "primitive" column types.
    fn columns(parent: &'static str) -> Vec<AColumn>;

    fn read(buf: &[u8]) -> Result<(Self, &[u8]), Error>;
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error>;
}
impl<C: Column> Row for C {
    fn columns(_parent: &'static str) -> Vec<AColumn> {
        unimplemented!()
    }
    fn read(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        Self::read_value(buf)
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.write_value(buf)
    }
}

impl Row for String {
    fn columns(name: &'static str) -> Vec<AColumn> {
        vec![AColumn {
            name,
            column_type: &ColumnType::String,
        }]
    }
    fn read(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (l, buf) = read_leb128(buf)?;
        let (bytes, buf) = read_bytes(buf, l as usize)?;
        Ok((String::from_utf8(bytes.to_vec())?, buf))
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_leb128(self.len() as u64)?;
        for b in self.as_bytes() {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

impl Row for Vec<u8> {
    fn columns(name: &'static str) -> Vec<AColumn> {
        vec![AColumn {
            name,
            column_type: &ColumnType::String,
        }]
    }
    fn read(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (l, buf) = read_leb128(buf)?;
        let (bytes, buf) = read_bytes(buf, l as usize)?;
        Ok((bytes.to_vec(), buf))
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_leb128(self.len() as u64)?;
        for b in self {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

impl<const N: usize> Row for [u8; N] {
    fn columns(name: &'static str) -> Vec<AColumn> {
        vec![AColumn {
            name,
            column_type: &ColumnType::FixedString(N),
        }]
    }
    fn read(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (bytes, buf) = read_bytes(buf, N)?;
        Ok((bytes.try_into().unwrap(), buf))
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        for b in self {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

impl Column for u8 {
    const TYPE: ColumnType = ColumnType::UInt8;
    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        read_u8(buf)
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_u8(*self)
    }
}

impl Column for u16 {
    const TYPE: ColumnType = ColumnType::UInt16;
    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (x, buf) = <[u8; 2]>::read(buf)?;
        Ok((Self::from_le_bytes(x), buf))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

impl Column for u32 {
    const TYPE: ColumnType = ColumnType::UInt32;
    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (x, buf) = <[u8; 4]>::read(buf)?;
        Ok((Self::from_le_bytes(x), buf))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

impl Column for u64 {
    const TYPE: ColumnType = ColumnType::UInt64;
    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (x, buf) = <[u8; 8]>::read(buf)?;
        Ok((Self::from_le_bytes(x), buf))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

impl Column for u128 {
    const TYPE: ColumnType = ColumnType::UInt128;
    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (x, buf) = <[u8; 16]>::read(buf)?;
        Ok((Self::from_le_bytes(x), buf))
    }
    fn write_value(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_le_bytes().write(buf)
    }
}

impl<T: Row> Row for Box<[T]> {
    fn columns(name: &'static str) -> Vec<AColumn> {
        let c = T::columns(name);
        if c.len() != 1 {
            panic!("Arrays must have a primitive type, should enforce at compile time with sealed trait FIXME");
        }
        let column_type = match c[0].column_type {
            ColumnType::String => &ColumnType::Array(&ColumnType::String),
            _ => panic!("Arrays must have a primitive type, should enforce at compile time with sealed trait FIXME"),
        };
        vec![AColumn { name, column_type }]
    }
    fn read(buf: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (l, mut buf) = read_leb128(buf)?;
        let mut out = Vec::with_capacity(l as usize);
        for _ in 0..l {
            let (v, rest) = T::read(buf)?;
            buf = rest;
            out.push(v);
        }
        Ok((out.into_boxed_slice(), buf))
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_leb128(self.len() as u64)?;
        for v in self.iter() {
            v.write(buf)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ColumnType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    String,
    FixedString(usize),
    Array(&'static ColumnType),
    DateTime,
}

impl ColumnType {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        match bytes {
            b"UInt8" => Ok(Self::UInt8),
            b"UInt16" => Ok(Self::UInt16),
            b"UInt32" => Ok(Self::UInt32),
            b"UInt64" => Ok(Self::UInt64),
            b"UInt128" => Ok(Self::UInt128),
            b"String" => Ok(Self::String),
            b"DateTime" => Ok(Self::DateTime),
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
