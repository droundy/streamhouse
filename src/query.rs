use std::borrow::Borrow;
use std::pin::Pin;

use crate::row::WriteRowBinary;
use crate::stream::Stream;
use crate::{Client, Error, Row};
use futures_util::stream::try_unfold;
use futures_util::{StreamExt, TryStreamExt};
use hyper::header::CONTENT_LENGTH;

impl Client {
    pub async fn query_fetch_all<R: Row>(&self, query: &str) -> Result<Vec<R>, Error> {
        self.query(query).await?.try_collect::<Vec<_>>().await
    }

    pub async fn query<R: Row>(
        &self,
        query: &str,
    ) -> Result<impl futures_util::Stream<Item = Result<R, Error>>, Error> {
        let mut builder = self.request_builder();

        let query = format!("{query} FORMAT RowBinaryWithNamesAndTypes");
        builder = builder.header(CONTENT_LENGTH, query.len().to_string());
        let request = builder
            .body(hyper::Body::from(query.to_string()))
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let response = self.client.request(request).await.map_err(Error::from)?;
        if response.status() != hyper::StatusCode::OK {
            return Err(Error::from_bad_response(response).await);
        }
        let body = response.into_body();
        Ok(Stream::new(body).await?.into_stream())
    }

    pub async fn execute(&self, query: &str) -> Result<(), Error> {
        let mut builder = self.request_builder();
        builder = builder.header(CONTENT_LENGTH, query.len().to_string());
        let request = builder
            .body(hyper::Body::from(query.to_string()))
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let response = self.client.request(request).await.map_err(Error::from)?;
        if response.status() != hyper::StatusCode::OK {
            return Err(Error::from_bad_response(response).await);
        }
        Ok(())
    }

    pub async fn insert<R, I>(&self, table: &str, rows: I) -> Result<(), Error>
    where
        R: Row,
        I: IntoIterator,
        I::Item: Borrow<R>,
    {
        let builder = self.request_builder();
        let mut body_bytes =
            format!("INSERT INTO {table} FORMAT RowBinaryWithNamesAndTypes\n").into_bytes();
        let columns = R::columns("");
        body_bytes.write_leb128(columns.len() as u64)?;
        for n in columns.iter().map(|c| c.name) {
            if n.is_empty() {
                return Err(Error::MissingColumnName {
                    row: columns.into_iter().map(|c| c.name).collect(),
                });
            }
            n.to_string().write(&mut body_bytes)?;
        }
        for t in columns.iter().map(|c| c.column_type) {
            format!("{t:?}").write(&mut body_bytes)?;
        }
        for r in rows {
            r.borrow().write(&mut body_bytes)?;
        }

        let request = builder
            .body(hyper::Body::from(body_bytes))
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let response = self.client.request(request).await.map_err(Error::from)?;
        if response.status() != hyper::StatusCode::OK {
            return Err(Error::from_bad_response(response).await);
        }
        Ok(())
    }

    pub async fn insert_stream<R: Row + Send + 'static>(
        &self,
        table: &str,
        rows: impl futures_util::Stream<Item = Result<R, Error>> + Send + 'static,
    ) -> Result<(), Error> {
        let rows: Pin<Box<dyn futures_util::Stream<Item = Result<R, Error>> + Send>> =
            Box::pin(rows);
        let builder = self.request_builder();
        let request = builder
            .body(row_stream_to_body(table.to_string(), rows))
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let response = self.client.request(request).await.map_err(Error::from)?;
        if response.status() != hyper::StatusCode::OK {
            return Err(Error::from_bad_response(response).await);
        }
        Ok(())
    }

    fn request_builder(&self) -> hyper::http::request::Builder {
        let mut builder = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(self.url.as_str());

        if let Some(database) = &self.database {
            builder = builder.header("X-ClickHouse-Database", database);
        }
        if let Some(user) = &self.user {
            builder = builder.header("X-ClickHouse-User", user);
        }
        if let Some(password) = &self.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }
        builder
    }
}

fn row_stream_to_body<R: Row + 'static + Send>(
    table: String,
    rows: Pin<Box<dyn futures_util::Stream<Item = Result<R, Error>> + Send>>,
) -> hyper::Body {
    let s: Box<
        (dyn futures_util::Stream<
            Item = Result<hyper::body::Bytes, Box<(dyn std::error::Error + Send + Sync + 'static)>>,
        > + Send
             + 'static),
    > = Box::new(try_unfold(
        RowReader::new(table, rows),
        RowReader::next_and_self,
    ));
    hyper::Body::from(s)
}

struct RowReader<R> {
    table: String,
    rows: Pin<Box<dyn futures_util::Stream<Item = Vec<Result<R, Error>>> + Send>>,
    have_started: bool,
}

const MAX_ROWS: usize = 10_000;

impl<R: Row + 'static> RowReader<R> {
    fn new(
        table: String,
        rows: Pin<Box<dyn futures_util::Stream<Item = Result<R, Error>> + Send>>,
    ) -> Self {
        Self {
            table,
            rows: Box::pin(rows.ready_chunks(MAX_ROWS)),
            have_started: false,
        }
    }
    async fn next_and_self(
        mut self,
    ) -> Result<Option<(hyper::body::Bytes, Self)>, Box<dyn std::error::Error + Send + Sync>> {
        let mut bytes = Vec::new();
        if !self.have_started {
            bytes = format!(
                "INSERT INTO {} FORMAT RowBinaryWithNamesAndTypes\n",
                self.table
            )
            .into_bytes();
            let columns = R::columns("");
            bytes.write_leb128(columns.len() as u64)?;
            for n in columns.iter().map(|c| c.name) {
                if n.is_empty() {
                    return Err(Box::new(Error::MissingColumnName {
                        row: columns.into_iter().map(|c| c.name).collect(),
                    }));
                }
                n.to_string().write(&mut bytes)?;
            }
            for t in columns.iter().map(|c| c.column_type) {
                format!("{t:?}").write(&mut bytes)?;
            }
            self.have_started = true;
        }
        if let Some(chunk) = self.rows.next().await {
            for row in chunk {
                let row = row?;
                row.write(&mut bytes)?;
            }
        }
        if bytes.is_empty() {
            Ok(None)
        } else {
            Ok(Some((hyper::body::Bytes::from(bytes), self)))
        }
    }
}
