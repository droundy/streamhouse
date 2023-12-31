//! Wrappers for clickhouse column types

use crate::Column;

use crate::row::single_column;
use crate::Row;

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
            column_type: "DateTime".to_string(),
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
            column_type: "IPv4".to_string(),
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
            column_type: "IPv6".to_string(),
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
            column_type: "UUID".to_string(),
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

impl<T: Row> Row for LowCardinality<T> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: format!("LowCardinality({})", single_column::<T>()),
        }]
    }
    fn read(buf: &mut crate::row::Bytes) -> Result<Self, crate::Error> {
        Ok(LowCardinality(buf.read()?))
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.0.write(buf)
    }
}

impl<K: Row + std::hash::Hash + Eq, V: Row> Row for std::collections::HashMap<K, V> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: format!("Map({}, {})", single_column::<K>(), single_column::<V>()),
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

impl<K: Row + Ord + Eq, V: Row> Row for std::collections::BTreeMap<K, V> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: format!("Map({}, {})", single_column::<K>(), single_column::<V>()),
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
