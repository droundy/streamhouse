use crate::{ColumnType, Error, Row};
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
            let buf = &self.bytes[self.cursor..];
            match V::read(buf) {
                Ok((v, buf)) => {
                    self.cursor = self.bytes.len() - buf.len();
                    return Ok(v);
                }
                Err(Error::NotEnoughData) => {
                    if let Some(more_bytes) = self.body.try_next().await? {
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

        let mut column_types: Vec<ColumnType> = Vec::new();
        for _ in 0..column_names.len() {
            let s: Vec<u8> = self.read().await?;
            column_types.push(ColumnType::parse(&s)?);
        }
        if R::TYPES != &column_types {
            return Err(Error::WrongColumnTypes {
                row: R::TYPES,
                schema: column_types,
            });
        }
        Ok(())
    }
}
