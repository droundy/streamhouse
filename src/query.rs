use crate::column::{ColumnType, RowBinary};
use crate::{Client, Error, Row};
use hyper::body::Buf;
use hyper::header::CONTENT_LENGTH;

impl Client {
    pub async fn query_fetch_all<R: Row>(&self, query: &str) -> Result<Vec<R>, Error> {
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
        let mut bytes = hyper::body::aggregate(body).await?;
        let num_columns = bytes.read_leb128()?;
        let mut column_names = Vec::new();
        for _ in 0..num_columns {
            column_names.push(String::read(&mut bytes)?);
        }

        let mut column_types = Vec::new();
        for _ in 0..num_columns {
            column_types.push(ColumnType::read(&Vec::<u8>::read(&mut bytes)?)?);
        }
        if R::TYPES != &column_types {
            return Err(Error::WrongColumnTypes {
                row: R::TYPES,
                schema: column_types,
            });
        }
        println!("We have {num_columns} columns: {column_names:?} of types {column_types:?}");
        let mut rows = Vec::new();
        while !bytes.done() {
            rows.push(R::read(&mut bytes)?);
        }

        Ok(rows)
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
        let body = response.into_body();
        let mut bytes = hyper::body::aggregate(body).await?;
        let bytes = bytes.read_bytes(bytes.remaining())?.to_vec();
        let msg = String::from_utf8_lossy(&bytes);
        println!("got msg {msg}");
        Ok(())
    }

    fn request_builder(&self) -> hyper::http::request::Builder {
        let mut url = self.url.clone();
        if let Some(database) = &self.database {
            url = format!("{url}?database={database}");
        }

        let mut builder = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(url);

        if let Some(user) = &self.user {
            builder = builder.header("X-ClickHouse-User", user);
        }
        if let Some(password) = &self.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }
        builder
    }
}
