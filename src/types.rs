//! Wrappers for clickhouse column types

use std::collections::BTreeMap;

use crate::Column;

use crate::row::PrimitiveRow;
use crate::{Error, Row};

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, PartialOrd, Ord)]
pub enum ColumnType {
    Bool,
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
    Map(&'static ColumnType, &'static ColumnType),
    LowCardinality(&'static ColumnType),
    DateTime,
    UUID,
    Nullable(&'static ColumnType),
    Enum8(&'static [(&'static str, u8)]),
}

impl ColumnType {
    pub fn parse(bytes: &[u8]) -> Result<&'static Self, Error> {
        match bytes {
            b"Bool" => Ok(&Self::Bool),

            b"UInt8" => Ok(&Self::UInt8),
            b"UInt16" => Ok(&Self::UInt16),
            b"UInt32" => Ok(&Self::UInt32),
            b"UInt64" => Ok(&Self::UInt64),
            b"UInt128" => Ok(&Self::UInt128),

            b"Int8" => Ok(&Self::Int8),
            b"Int16" => Ok(&Self::Int16),
            b"Int32" => Ok(&Self::Int32),
            b"Int64" => Ok(&Self::Int64),
            b"Int128" => Ok(&Self::Int128),

            b"Float32" => Ok(&Self::Float32),
            b"Float64" => Ok(&Self::Float64),

            b"IPv4" => Ok(&Self::IPv4),
            b"IPv6" => Ok(&Self::IPv6),

            b"String" => Ok(&Self::String),
            b"DateTime" => Ok(&Self::DateTime),
            b"UUID" => Ok(&Self::UUID),
            _ => {
                if bytes.starts_with(b"FixedString(") && bytes.last() == Some(&b')') {
                    let mut len = 0;
                    for b in &bytes[b"FixedString(".len()..bytes.len() - 1] {
                        len += (*b - b'0') as usize;
                    }
                    static FIXED_STRINGS: std::sync::Mutex<Vec<&'static ColumnType>> =
                        std::sync::Mutex::new(Vec::new());
                    let mut fixed = FIXED_STRINGS.lock().unwrap();
                    let mut fixed_len = fixed.len();
                    while len >= fixed_len {
                        fixed.push(Box::leak(Box::new(Self::FixedString(fixed_len))));
                        fixed_len += 1;
                    }
                    Ok(fixed[len])
                } else if bytes.starts_with(b"LowCardinality(") && bytes.last() == Some(&b')') {
                    let sub_bytes = &bytes[b"LowCardinality(".len()..bytes.len() - 1];
                    static MAP: std::sync::Mutex<BTreeMap<ColumnType, &'static ColumnType>> =
                        std::sync::Mutex::new(BTreeMap::new());
                    let mut map = MAP.lock().unwrap();
                    let sub_column = Self::parse(sub_bytes)?;
                    if let Some(v) = map.get(sub_column) {
                        Ok(v)
                    } else {
                        map.insert(
                            *sub_column,
                            Box::leak(Box::new(Self::LowCardinality(sub_column))),
                        );
                        Ok(map[sub_column])
                    }
                } else if bytes.starts_with(b"Array(") && bytes.last() == Some(&b')') {
                    let sub_bytes = &bytes[b"Array(".len()..bytes.len() - 1];
                    static MAP: std::sync::Mutex<BTreeMap<ColumnType, &'static ColumnType>> =
                        std::sync::Mutex::new(BTreeMap::new());
                    let mut map = MAP.lock().unwrap();
                    let sub_column = Self::parse(sub_bytes)?;
                    if let Some(v) = map.get(sub_column) {
                        Ok(v)
                    } else {
                        map.insert(*sub_column, Box::leak(Box::new(Self::Array(sub_column))));
                        Ok(map[sub_column])
                    }
                } else if bytes.starts_with(b"Enum8(") && bytes.last() == Some(&b')') {
                    let sub_bytes = &bytes[b"Enum8(".len()..bytes.len() - 1];
                    static MAP: std::sync::Mutex<BTreeMap<ColumnType, &'static ColumnType>> =
                        std::sync::Mutex::new(BTreeMap::new());
                    let mut map = MAP.lock().unwrap();
                    let sub_column = Self::parse(sub_bytes)?;
                    if let Some(v) = map.get(sub_column) {
                        Ok(v)
                    } else {
                        map.insert(*sub_column, Box::leak(Box::new(Self::Enum8(sub_column))));
                        Ok(map[sub_column])
                    }
                } else if bytes.starts_with(b"Map(") && bytes.last() == Some(&b')') {
                    let sub_bytes = &bytes[b"Map(".len()..bytes.len() - 1];
                    let Some(i) = sub_bytes.iter().position(|&b| b == b',') else {
                        return Err(Error::UnsupportedColumn(format!("Map should have a comma: {}", String::from_utf8_lossy(sub_bytes))))
                    };
                    let key_bytes = &sub_bytes[..i];
                    let mut value_bytes = &sub_bytes[i + 1..];
                    if value_bytes.first() == Some(&b' ') {
                        value_bytes = &value_bytes[1..];
                    }
                    static MAP: std::sync::Mutex<
                        BTreeMap<(ColumnType, ColumnType), &'static ColumnType>,
                    > = std::sync::Mutex::new(BTreeMap::new());
                    let mut map = MAP.lock().unwrap();
                    let key_column = Self::parse(key_bytes)?;
                    let value_column = Self::parse(value_bytes)?;
                    let kv = (*key_column, *value_column);
                    if let Some(v) = map.get(&kv) {
                        Ok(v)
                    } else {
                        map.insert(kv, Box::leak(Box::new(Self::Map(key_column, value_column))));
                        Ok(map[&kv])
                    }
                } else if bytes.starts_with(b"Nullable(") && bytes.last() == Some(&b')') {
                    let sub_bytes = &bytes[b"Nullable(".len()..bytes.len() - 1];
                    static MAP: std::sync::Mutex<BTreeMap<ColumnType, &'static ColumnType>> =
                        std::sync::Mutex::new(BTreeMap::new());
                    let mut map = MAP.lock().unwrap();
                    let sub_column = Self::parse(sub_bytes)?;
                    if let Some(v) = map.get(sub_column) {
                        Ok(v)
                    } else {
                        map.insert(*sub_column, Box::leak(Box::new(Self::Nullable(sub_column))));
                        Ok(map[sub_column])
                    }
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

/// Represents a `LowCardinality` version of a type
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct LowCardinality<T>(pub T);

impl<T> From<T> for LowCardinality<T> {
    fn from(value: T) -> Self {
        LowCardinality(value)
    }
}
impl<T> std::ops::Deref for LowCardinality<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: PrimitiveRow> Row for LowCardinality<T> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::LowCardinality(T::COLUMN_TYPE),
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        Ok(LowCardinality(buf.read()?))
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.0.write(buf)
    }
}

impl<K: PrimitiveRow + std::hash::Hash + Eq, V: PrimitiveRow> Row
    for std::collections::HashMap<K, V>
{
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::Map(K::COLUMN_TYPE, V::COLUMN_TYPE),
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        let length = buf.read_leb128()?;
        let mut h = std::collections::HashMap::with_capacity(length);
        for _ in 0..length {
            h.insert(buf.read()?, buf.read()?);
        }
        Ok(h)
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        buf.write_leb128(self.len() as u64)?;
        for (k, v) in self.iter() {
            k.write(buf)?;
            v.write(buf)?;
        }
        Ok(())
    }
}

impl<K: PrimitiveRow + Ord + Eq, V: PrimitiveRow> Row for std::collections::BTreeMap<K, V> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::Map(K::COLUMN_TYPE, V::COLUMN_TYPE),
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        let length = buf.read_leb128()?;
        let mut h = std::collections::BTreeMap::new();
        for _ in 0..length {
            h.insert(buf.read()?, buf.read()?);
        }
        Ok(h)
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        buf.write_leb128(self.len() as u64)?;
        for (k, v) in self.iter() {
            k.write(buf)?;
            v.write(buf)?;
        }
        Ok(())
    }
}
