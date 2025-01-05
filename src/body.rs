use crate::Error;
use futures::{Stream, TryStream, TryStreamExt};
use http_body_util::{combinators::BoxBody, Collected, Empty, Full, StreamBody};
use hyper::{
    body::{Body as HttpBody, Bytes, Frame, Incoming, SizeHint},
    Request, Response,
};
use std::{pin::Pin, task::{Context, Poll}};

/// Enum that holds different types of data for the response body
#[derive(Debug)]
enum Internal {
    BoxBody(BoxBody<Bytes, Error>),
    Collected(Collected<Bytes>),
    Empty(Empty<Bytes>),
    Full(Full<Bytes>),
    Incoming(Incoming),
    String(String),
}

/// Concrete implementation of [`Body`](HttpBody).
#[derive(Debug)]
pub struct Body {
    inner: Internal,
}

impl Body {
    // Builds an empty body
    pub fn empty() -> Self {
        Self::from(Empty::new())
    }

    // Builds a body from a Stream
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: TryStream + Send + Sync + 'static,
        S::Ok: Into<Bytes>,
        S::Error: Into<Error>,
    {
        Self {
            inner: Internal::BoxBody(BoxBody::new(StreamBody::new(
                stream
                    .map_ok(Into::into) // Converts items into Bytes
                    .map_ok(Frame::data) // Converts items into data Frames
                    .map_err(Into::into), // Handles errors
            ))),
        }
    }
}

// Implementing Stream trait for Body
impl Stream for Body {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().inner {
            Internal::BoxBody(body) => Pin::new(body).poll_next(cx),
            Internal::Collected(body) => Pin::new(body).poll_next(cx).map_err(Into::into),
            Internal::Empty(body) => Pin::new(body).poll_next(cx).map_err(Into::into),
            Internal::Full(body) => Pin::new(body).poll_next(cx).map_err(Into::into),
            Internal::Incoming(body) => Pin::new(body).poll_next(cx).map_err(Into::into),
            Internal::String(body) => Poll::Ready(Some(Ok(Bytes::from(body.clone())))),
        }
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = Error;

    // Polling to get the next Frame
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.inner {
            Internal::BoxBody(body) => Pin::new(body).poll_frame(cx),
            Internal::Collected(body) => Pin::new(body).poll_frame(cx).map_err(|e| match e {}),
            Internal::Empty(body) => Pin::new(body).poll_frame(cx).map_err(|e| match e {}),
            Internal::Full(body) => Pin::new(body).poll_frame(cx).map_err(|e| match e {}),
            Internal::Incoming(body) => Pin::new(body).poll_frame(cx).map_err(Error::from),
            Internal::String(body) => Poll::Ready(Some(Ok(Frame::data(Bytes::from(body.clone()))))),
        }
    }

    // Check if the stream has ended
    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Internal::BoxBody(body) => body.is_end_stream(),
            Internal::Collected(body) => body.is_end_stream(),
            Internal::Empty(body) => body.is_end_stream(),
            Internal::Full(body) => body.is_end_stream(),
            Internal::Incoming(body) => body.is_end_stream(),
            Internal::String(_) => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Internal::BoxBody(body) => body.size_hint(),
            Internal::Collected(body) => body.size_hint(),
            Internal::Empty(body) => body.size_hint(),
            Internal::Full(body) => body.size_hint(),
            Internal::Incoming(body) => body.size_hint(),
            Internal::String(_) => SizeHint::default(),
        }
    }
}