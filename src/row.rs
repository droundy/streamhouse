use crate::error::Error;
use crate::ColumnType;

pub trait WriteRowBinary {
    fn write_u8(&mut self, value: u8) -> Result<(), Error>;
    fn write_leb128(&mut self, mut value: u64) -> Result<(), Error> {
        loop {
            if value < 128 {
                self.write_u8(value as u8)?;
                return Ok(());
            } else {
                self.write_u8(value as u8)?;
                value >>= 7;
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

pub struct Bytes<'a> {
    pub(crate) buf: &'a [u8],
}

impl<'a> Bytes<'a> {
    pub fn read<T: Row>(&mut self) -> Result<T, Error> {
        Row::read(self)
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        if let Some((&f, rest)) = self.buf.split_first() {
            self.buf = rest;
            Ok(f)
        } else {
            Err(Error::NotEnoughData)
        }
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], Error> {
        if self.buf.len() < len {
            Err(Error::NotEnoughData)
        } else {
            let (v, rest) = self.buf.split_at(len);
            self.buf = rest;
            Ok(v)
        }
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        if self.buf.len() < N {
            Err(Error::NotEnoughData)
        } else {
            let (v, rest) = self.buf.split_at(N);
            self.buf = rest;
            Ok(v.try_into().unwrap())
        }
    }

    pub(crate) fn read_leb128(&mut self) -> Result<usize, Error> {
        let mut result = 0;
        let mut shift = 0;
        loop {
            let byte = self.read_u8()?;
            result |= ((byte & 127) as usize) << shift;
            if byte & 128 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }
}

/// The definition of a column within a table.
///
/// This consists of a column name and a column type.
#[derive(Debug)]
pub struct Column {
    pub(crate) name: &'static str,
    pub(crate) column_type: &'static ColumnType,
}

/// A type that is *either* a column type *or* a full clickhouse row.
///
/// Row types are composable, so a row is typically composed of a sequence of
/// rows.
pub trait Row: Sized {
    /// The set of columns in this row.
    ///
    /// The `parent` is the name of this row, if it has a name, which is used in
    /// the derive macro.  If there is no name, then `parent` should be the
    /// empty string.
    fn columns(parent: &'static str) -> Vec<Column>;

    /// Read this row from a buffer.
    fn read(buf: &mut Bytes) -> Result<Self, Error>;
    /// Write this row (for insertion into clickhouse).
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error>;
}

impl Row for String {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::String,
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        let l = buf.read_leb128()?;
        let bytes = buf.read_bytes(l)?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_leb128(self.len() as u64)?;
        for b in self.as_bytes() {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

pub trait PrimitiveRow: Row {
    const COLUMN_TYPE: &'static ColumnType;
}

impl PrimitiveRow for Vec<u8> {
    const COLUMN_TYPE: &'static ColumnType = &ColumnType::String;
}
impl Row for Vec<u8> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::String,
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        let l = buf.read_leb128()?;
        let bytes = buf.read_bytes(l)?;
        Ok(bytes.to_vec())
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
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::FixedString(N),
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        buf.read_array()
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        for b in self {
            buf.write_u8(*b)?;
        }
        Ok(())
    }
}

impl Row for u8 {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::UInt8,
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        buf.read_u8()
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_u8(*self)
    }
}

impl Row for bool {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::Bool,
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        Ok(buf.read_u8()? != 0)
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_u8(*self as u8)
    }
}

macro_rules! row_via_array {
    ($t:ty, $clickhouse_type:expr) => {
        impl Row for $t {
            fn columns(name: &'static str) -> Vec<Column> {
                vec![Column {
                    name,
                    column_type: &$clickhouse_type,
                }]
            }
            fn read(buf: &mut Bytes) -> Result<Self, Error> {
                Ok(Self::from_le_bytes(buf.read_array()?))
            }
            fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
                self.to_le_bytes().write(buf)
            }
        }
        impl PrimitiveRow for $t {
            const COLUMN_TYPE: &'static ColumnType = &$clickhouse_type;
        }
    };
}

row_via_array!(u16, ColumnType::UInt16);
row_via_array!(u32, ColumnType::UInt32);
row_via_array!(u64, ColumnType::UInt64);
row_via_array!(u128, ColumnType::UInt128);

row_via_array!(i8, ColumnType::Int8);
row_via_array!(i16, ColumnType::Int16);
row_via_array!(i32, ColumnType::Int32);
row_via_array!(i64, ColumnType::Int64);
row_via_array!(i128, ColumnType::Int128);

row_via_array!(f32, ColumnType::Float32);
row_via_array!(f64, ColumnType::Float64);

macro_rules! primitive_row_type {
    ($t:ty, $clickhouse_type:expr) => {
        impl PrimitiveRow for $t {
            const COLUMN_TYPE: &'static ColumnType = &$clickhouse_type;
        }
    };
}
primitive_row_type!(String, ColumnType::String);
primitive_row_type!(crate::types::Uuid, ColumnType::UUID);
primitive_row_type!(u8, ColumnType::UInt8);

impl<T: PrimitiveRow> Row for Box<[T]> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::Array(T::COLUMN_TYPE),
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        let l = buf.read_leb128()?;
        let mut out = Vec::with_capacity(l);
        for _ in 0..l {
            out.push(buf.read()?);
        }
        Ok(out.into_boxed_slice())
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        buf.write_leb128(self.len() as u64)?;
        for v in self.iter() {
            v.write(buf)?;
        }
        Ok(())
    }
}

impl<T: PrimitiveRow> Row for Option<T> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: &ColumnType::Nullable(T::COLUMN_TYPE),
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        let b = buf.read_u8()?;
        if b == 1 {
            Ok(None)
        } else {
            Ok(Some(buf.read()?))
        }
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        if let Some(v) = self {
            buf.write_u8(0)?;
            v.write(buf)
        } else {
            buf.write_u8(1)
        }
    }
}

/// Trait for types that can be represented in clickhouse as another type.
///
/// # Example
/// ```
/// struct DateTimeWithNanos(f64);
///
/// #[derive(streamhouse::Row)]
/// struct DateInClickhouse {
///     /// The first column is called datetime
///     seconds: u64,
///     /// There is another nanos colum for the nanoseconds
///     nanos: u32,
/// }
///
/// impl streamhouse::RowAs for DateTimeWithNanos {
///     type InternalRow = DateInClickhouse;
///     fn from_internal(internal: DateInClickhouse) -> Self {
///         Self(internal.seconds as f64 + internal.nanos as f64 * 1e-9)
///     }
///     fn to_internal(&self) -> Self::InternalRow {
///         DateInClickhouse { seconds: self.0 as u64, nanos: ((self.0 - self.0.floor())*1e9) as u32}
///     }
/// }
/// ```
pub trait RowAs {
    type InternalRow: Row;
    fn from_internal(internal: Self::InternalRow) -> Self;
    fn to_internal(&self) -> Self::InternalRow;
}
impl<R: RowAs> Row for R {
    fn columns(parent: &'static str) -> Vec<Column> {
        <Self as RowAs>::InternalRow::columns(parent)
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        let internal = <Self as RowAs>::InternalRow::read(buf)?;
        Ok(<Self as RowAs>::from_internal(internal))
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.to_internal().write(buf)
    }
}
