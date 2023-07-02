//! Wrappers for clickhouse column types

use crate::Column;

use crate::{Error, Row};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ColumnType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Float32,
    Float64,
    String,
    IPv4,
    IPv6,
    FixedString(usize),
    Array(&'static ColumnType),
    DateTime,
    UUID,
}

impl ColumnType {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        match bytes {
            b"UInt8" => Ok(Self::UInt8),
            b"UInt16" => Ok(Self::UInt16),
            b"UInt32" => Ok(Self::UInt32),
            b"UInt64" => Ok(Self::UInt64),
            b"UInt128" => Ok(Self::UInt128),

            b"Int8" => Ok(Self::Int8),
            b"Int16" => Ok(Self::Int16),
            b"Int32" => Ok(Self::Int32),
            b"Int64" => Ok(Self::Int64),
            b"Int128" => Ok(Self::Int128),

            b"Float32" => Ok(Self::Float32),
            b"Float64" => Ok(Self::Float64),

            b"IPv4" => Ok(Self::IPv4),
            b"IPv6" => Ok(Self::IPv6),

            b"String" => Ok(Self::String),
            b"DateTime" => Ok(Self::DateTime),
            b"UUID" => Ok(Self::UUID),
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
pub struct DateTime(u32);

impl DateTime {
    pub fn now() -> Self {
        DateTime(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
        )
    }
}

impl Row for DateTime {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::DateTime,
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        Ok(DateTime(u32::read(buf)?))
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.0.write(buf)
    }
}

impl Row for std::net::Ipv4Addr {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::IPv4,
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        let bytes: [u8; 4] = buf.read()?;
        Ok(std::net::Ipv4Addr::from(bytes))
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.octets().write(buf)
    }
}

impl Row for std::net::Ipv6Addr {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::IPv6,
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        let bytes: [u8; 16] = buf.read()?;
        Ok(std::net::Ipv6Addr::from(bytes))
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.octets().write(buf)
    }
}

/// A newtype that enables using clickhouse UUID without a uuid crate dependency.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
pub struct Uuid([u8; 16]);

impl From<[u8; 16]> for Uuid {
    fn from(value: [u8; 16]) -> Self {
        Uuid(value)
    }
}
impl From<Uuid> for [u8; 16] {
    fn from(value: Uuid) -> Self {
        value.0
    }
}

impl Row for Uuid {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::UUID,
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        Ok(Uuid(buf.read()?))
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.0.write(buf)
    }
}
