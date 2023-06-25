use crate::ColumnType;

use crate::Column;

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

impl Column for DateTime {
    const TYPE: ColumnType = ColumnType::DateTime;
    fn read_value(buf: &[u8]) -> Result<(Self, &[u8]), crate::Error> {
        let (v, buf) = u32::read_value(buf)?;
        Ok((DateTime(v), buf))
    }
    fn write_value(&self, buf: &mut impl crate::WriteRowBinary) -> Result<(), crate::Error> {
        self.0.write_value(buf)
    }
}
