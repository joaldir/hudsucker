use crate::Error;
use futures::{Stream, TryStream, TryStreamExt};
use http_body_util::{combinators::BoxBody, Collected, Empty, Full, StreamBody};
use hyper::{
    body::{Body as HttpBody, Bytes, Frame, Incoming, SizeHint},
    Request, Response,
};
use std::{pin::Pin, task::{Context, Poll}};

/// Enum que contém diferentes tipos de dados para o corpo da resposta
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
    // Constrói um corpo vazio
    pub fn empty() -> Self {
        Self::from(Empty::new())
    }

    // Constrói um corpo a partir de um Stream
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: TryStream + Send + Sync + 'static,
        S::Ok: Into<Bytes>,
        S::Error: Into<Error>,
    {
        Self {
            inner: Internal::BoxBody(BoxBody::new(StreamBody::new(
                stream
                    .map_ok(Into::into) // converte os itens em Bytes
                    .map_ok(Frame::data) // converte os itens em Frames de dados
                    .map_err(Into::into), // trata os erros
            ))),
        }
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = Error;

    // Polling para obter o próximo Frame
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
            Internal::String(body) => Pin::new(body).poll_frame(cx).map_err(|e| match e {}),
        }
    }

    // Verifica se o stream chegou ao fim
    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Internal::BoxBody(body) => body.is_end_stream(),
            Internal::Collected(_) => true,
            Internal::Empty(_) => true,
            Internal::Full(_) => true,
            Internal::Incoming(_) => false,
            Internal::String(_) => true,
        }
    }

    // Retorna o tamanho estimado do stream
    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Internal::BoxBody(body) => body.size_hint(),
            Internal::Collected(_) => SizeHint::default(),
            Internal::Empty(_) => SizeHint::default(),
            Internal::Full(_) => SizeHint::default(),
            Internal::Incoming(_) => SizeHint::default(),
            Internal::String(_) => SizeHint::default(),
        }
    }
}

impl From<Incoming> for Body {
    fn from(inner: Incoming) -> Self {
        Body {
            inner: Internal::Incoming(inner),
        }
    }
}

impl From<Full<Bytes>> for Body {
    fn from(inner: Full<Bytes>) -> Self {
        Body {
            inner: Internal::Full(inner),
        }
    }
}

impl From<Empty<Bytes>> for Body {
    fn from(inner: Empty<Bytes>) -> Self {
        Body {
            inner: Internal::Empty(inner),
        }
    }
}

impl From<Collected<Bytes>> for Body {
    fn from(inner: Collected<Bytes>) -> Self {
        Body {
            inner: Internal::Collected(inner),
        }
    }
}

impl From<String> for Body {
    fn from(inner: String) -> Self {
        Body {
            inner: Internal::String(inner),
        }
    }
}

impl From<Bytes> for Body {
    fn from(inner: Bytes) -> Self {
        Body {
            inner: Internal::Full(Full::new(inner)),
        }
    }
}