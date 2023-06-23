use crate::column::{ColumnType, RowBinary, WriteRowBinary};
use crate::{Client, Error, Row};
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
        let column_names = <Box<[String]>>::read(&mut bytes)?;

        let mut column_types = Vec::new();
        for _ in 0..column_names.len() {
            column_types.push(ColumnType::read(&Vec::<u8>::read(&mut bytes)?)?);
        }
        if R::TYPES != &column_types {
            return Err(Error::WrongColumnTypes {
                row: R::TYPES,
                schema: column_types,
            });
        }
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
        Ok(())
    }

    pub async fn insert<R: Row>(
        &self,
        table: &str,
        rows: impl IntoIterator<Item = R>,
    ) -> Result<(), Error> {
        let builder = self.request_builder();
        let mut body_bytes =
            format!("INSERT INTO {table} FORMAT RowBinaryWithNamesAndTypes\n").into_bytes();
        body_bytes.write_leb128(R::TYPES.len() as u64)?;
        for n in R::NAMES {
            n.to_string().write(&mut body_bytes)?;
        }
        for t in R::TYPES {
            format!("{t:?}").write(&mut body_bytes)?;
        }
        for r in rows {
            r.write(&mut body_bytes)?;
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
