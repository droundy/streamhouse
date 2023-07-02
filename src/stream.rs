use crate::{row::Bytes, ColumnType, Error, Row};
use futures_util::stream::TryStreamExt;

pub(crate) struct Stream<R: Row> {
    body: hyper::Body,
    bytes: Vec<u8>,
    cursor: usize,
    all_done: bool,
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Row> Stream<R> {
    pub async fn new(body: hyper::Body) -> Result<Self, Error> {
        let mut s = Self {
            body,
            bytes: Vec::new(),
            cursor: 0,
            all_done: false,
            _phantom: std::marker::PhantomData,
        };
        s.check_header().await?;
        Ok(s)
    }

    fn am_done(&mut self) -> bool {
        self.all_done && self.cursor == self.bytes.len()
    }

    pub async fn read<V: Row>(&mut self) -> Result<V, Error> {
        loop {
            let mut buf = Bytes {
                buf: &self.bytes[self.cursor..],
            };
            match V::read(&mut buf) {
                Ok(v) => {
                    self.cursor = self.bytes.len() - buf.buf.len();
                    return Ok(v);
                }
                Err(Error::NotEnoughData) => {
                    if let Some(more_bytes) = self.body.try_next().await? {
                        let buf = &self.bytes[self.cursor..];
                        self.bytes = if buf.is_empty() {
                            more_bytes.to_vec()
                        } else {
                            let mut b = Vec::with_capacity(buf.len() + more_bytes.len());
                            b.extend(buf);
                            b.extend(more_bytes);
                            b
                        };
                        self.cursor = 0;
                    } else {
                        self.all_done = true;
                        return Err(Error::NotEnoughData);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn get_next(&mut self) -> Option<Result<R, Error>> {
        if self.am_done() {
            None
        } else {
            match self.read().await {
                Ok(r) => Some(Ok(r)),
                Err(Error::NotEnoughData) => {
                    if self.am_done() {
                        None
                    } else {
                        Some(Err(Error::NotEnoughData))
                    }
                }
                Err(e) => Some(Err(e)),
            }
        }
    }
    async fn next_and_self(mut self) -> Option<(Result<R, Error>, Self)> {
        let v = self.get_next().await;
        v.map(|r| (r, self))
    }
    pub fn into_stream(self) -> impl futures_util::stream::Stream<Item = Result<R, Error>> {
        Box::pin(futures_util::stream::unfold(self, Self::next_and_self))
    }

    async fn check_header(&mut self) -> Result<(), Error> {
        let column_names: Box<[String]> = self.read().await?;
        let correct_column_names = R::columns("").iter().map(|c| c.name).collect::<Vec<_>>();
        let single_column_query =
            correct_column_names.len() == 1 && correct_column_names[0].is_empty();
        if column_names.len() != correct_column_names.len()
            || (!single_column_query
                && correct_column_names
                    .iter()
                    .zip(column_names.iter())
                    .any(|(a, b)| a != b))
        {
            return Err(Error::WrongColumnNames {
                row: column_names,
                schema: correct_column_names,
            });
        }

        let mut column_types: Vec<ColumnType> = Vec::new();
        for _ in 0..column_names.len() {
            let s: Vec<u8> = self.read().await?;
            column_types.push(ColumnType::parse(&s)?);
        }
        let types = R::columns("")
            .iter()
            .map(|c| *c.column_type)
            .collect::<Vec<_>>();
        if &types != &column_types {
            return Err(Error::WrongColumnTypes {
                row: types,
                schema: column_types,
            });
        }
        Ok(())
    }
}
