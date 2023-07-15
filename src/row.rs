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
    pub(crate) column_type: String,
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
            column_type: "String".to_string(),
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

impl Row for Vec<u8> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: "String".to_string(),
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
            column_type: format!("FixedString({N})"),
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

#[test]
fn u8_type_name() {
    assert_eq!("UInt8", u8::columns("")[0].column_type);
}

impl Row for u8 {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: "UInt8".to_string(),
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
            column_type: "Bool".to_string(),
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
                    column_type: $clickhouse_type.to_string(),
                }]
            }
            fn read(buf: &mut Bytes) -> Result<Self, Error> {
                Ok(Self::from_le_bytes(buf.read_array()?))
            }
            fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
                self.to_le_bytes().write(buf)
            }
        }
    };
}

row_via_array!(u16, "UInt16");
row_via_array!(u32, "UInt32");
row_via_array!(u64, "UInt64");
row_via_array!(u128, "UInt128");

row_via_array!(i8, "Int8");
row_via_array!(i16, "Int16");
row_via_array!(i32, "Int32");
row_via_array!(i64, "Int64");
row_via_array!(i128, "Int128");

row_via_array!(f32, "Float32");
row_via_array!(f64, "Float64");

pub(crate) fn single_column<R: Row>() -> String {
    let c = R::columns("");
    if c.len() == 1 {
        c.into_iter().map(|c| c.column_type).next().unwrap()
    } else {
        let types = c
            .into_iter()
            .map(|c| c.column_type)
            .collect::<Vec<_>>()
            .join(", ");
        format!("Tuple({types})")
    }
}

impl<T: Row> Row for Box<[T]> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: format!("Array({})", single_column::<T>()),
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

impl<T: Row> Row for Option<T> {
    fn columns(name: &'static str) -> Vec<Column> {
        vec![Column {
            name,
            column_type: format!("Nullable({})", single_column::<T>()),
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

impl<T1: Row, T2: Row> Row for (T1, T2) {
    fn columns(name: &'static str) -> Vec<Column> {
        let c1 = T1::columns(name);
        let c2 = T2::columns(name);
        let types = c1
            .into_iter()
            .map(|c| c.column_type)
            .chain(c2.into_iter().map(|c| c.column_type))
            .collect::<Vec<_>>()
            .join(", ");
        vec![Column {
            name,
            column_type: format!("Tuple({})", types),
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        let v1 = T1::read(buf)?;
        let v2 = T2::read(buf)?;
        Ok((v1, v2))
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.0.write(buf)?;
        self.1.write(buf)
    }
}

impl<T1: Row, T2: Row, T3: Row> Row for (T1, T2, T3) {
    fn columns(name: &'static str) -> Vec<Column> {
        let c1 = T1::columns(name);
        let c2 = T2::columns(name);
        let c3 = T3::columns(name);
        let types = c1
            .into_iter()
            .map(|c| c.column_type)
            .chain(c2.into_iter().map(|c| c.column_type))
            .chain(c3.into_iter().map(|c| c.column_type))
            .collect::<Vec<_>>()
            .join(", ");
        vec![Column {
            name,
            column_type: format!("Tuple({})", types),
        }]
    }
    fn read(buf: &mut Bytes) -> Result<Self, Error> {
        let v1 = T1::read(buf)?;
        let v2 = T2::read(buf)?;
        let v3 = T3::read(buf)?;
        Ok((v1, v2, v3))
    }
    fn write(&self, buf: &mut impl WriteRowBinary) -> Result<(), Error> {
        self.0.write(buf)?;
        self.1.write(buf)?;
        self.2.write(buf)
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
