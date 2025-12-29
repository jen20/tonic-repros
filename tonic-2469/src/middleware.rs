use bytes::Buf;
use http_body::{Body, Frame};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Debug)]
enum ParseState {
    Header { buf: [u8; 5], filled: usize },
    Payload { remaining: u32 },
}

impl Default for ParseState {
    fn default() -> Self {
        ParseState::Header {
            buf: [0; 5],
            filled: 0,
        }
    }
}

impl ParseState {
    fn parse(&mut self, message_sizes: &Arc<Mutex<Vec<u32>>>, data: &[u8]) {
        let mut pos = 0;

        while pos < data.len() {
            match self {
                ParseState::Header { buf, filled } => {
                    let needed = 5 - *filled;
                    let available = data.len() - pos;
                    let consume = needed.min(available);

                    buf[*filled..*filled + consume].copy_from_slice(&data[pos..pos + consume]);
                    *filled += consume;
                    pos += consume;

                    if *filled == 5 {
                        let msg_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
                        message_sizes.lock().unwrap().push(msg_len);

                        *self = ParseState::Payload { remaining: msg_len };
                    }
                }
                ParseState::Payload { remaining } => {
                    let available = data.len() - pos;
                    let consume = (*remaining as usize).min(available);

                    pos += consume;
                    *remaining -= consume as u32;

                    if *remaining == 0 {
                        *self = ParseState::default();
                    }
                }
            }
        }
    }
}

pin_project! {
    pub struct PayloadSizeExtractor<B> {
        #[pin]
        inner: B,
        message_sizes: Arc<Mutex<Vec<u32>>>,
        state: ParseState,
    }
}

impl<B> PayloadSizeExtractor<B> {
    pub fn new(inner: B, message_sizes: Arc<Mutex<Vec<u32>>>) -> Self {
        Self {
            inner,
            message_sizes,
            state: ParseState::default(),
        }
    }
}

impl<B> Body for PayloadSizeExtractor<B>
where
    B: Body,
    B::Data: Buf,
    B::Error: std::error::Error + 'static,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let result = this.inner.poll_frame(cx);

        if let Poll::Ready(Some(Ok(frame))) = &result {
            if let Some(data) = frame.data_ref() {
                this.state.parse(this.message_sizes, data.chunk());
            }
        }

        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Clone)]
pub struct PayloadSizeLayer;

impl<S> Layer<S> for PayloadSizeLayer {
    type Service = PayloadSizeService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PayloadSizeService { inner: service }
    }
}

pin_project! {
    pub struct PayloadSizeFuture<F> {
        #[pin]
        inner: F,
        message_sizes: Arc<Mutex<Vec<u32>>>,
    }
}

impl<F, T, E> Future for PayloadSizeFuture<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.inner.poll(cx);

        if let Poll::Ready(Ok(_)) = &result {
            let sizes = this.message_sizes.lock().unwrap();
            if !sizes.is_empty() {
                println!("[Sizing] Request message sizes: {:?} bytes", &*sizes);
            }
        }

        result
    }
}

#[derive(Clone)]
pub struct PayloadSizeService<S> {
    inner: S,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for PayloadSizeService<S>
where
    S: Service<http::Request<PayloadSizeExtractor<ReqBody>>>,
    ReqBody: Body,
    ReqBody::Data: Buf,
    ReqBody::Error: std::error::Error + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = PayloadSizeFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let message_sizes = Arc::new(Mutex::new(Vec::new()));
        let (parts, body) = req.into_parts();
        let parser_body = PayloadSizeExtractor::new(body, message_sizes.clone());
        let req = http::Request::from_parts(parts, parser_body);

        PayloadSizeFuture {
            inner: self.inner.call(req),
            message_sizes,
        }
    }
}
