use crate::AColumn;
use crate::ColumnType;

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
    fn columns(name: &'static str) -> Vec<AColumn> {
        vec![AColumn {
            name,
            column_type: &ColumnType::DateTime,
        }]
    }
    fn read(buf: &[u8]) -> Result<(Self, &[u8]), crate::Error> {
        let (v, buf) = u32::read(buf)?;
        Ok((DateTime(v), buf))
    }
    fn write(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.0.write(buf)
    }
}
