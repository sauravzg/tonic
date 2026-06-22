/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

//! Server-side deadline enforcement via the `grpc-timeout` header.
//!
//! The `grpc-timeout` header is sent by the client to indicate the maximum
//! time allowed for an RPC. This module provides a [`DeadlineInterceptor`]
//! that parses this header and enforces it by racing the handler against a
//! sleep timer.

use std::time::Duration;

use crate::client::CallOptions;
use crate::core::{RequestHeaders, Trailers};
use crate::rt::GrpcRuntime;
use crate::server::interceptor::Intercept;
use crate::server::{Handle, RecvStream, SendStream};
use crate::status::{StatusCodeError, StatusError};

/// A server interceptor that enforces deadlines sent by the client via the
/// `grpc-timeout` header.
///
/// When the `grpc-timeout` header is present, this interceptor races the
/// inner handler against a sleep timer. If the timer fires first, the
/// handler's future is dropped (cooperative cancellation) and a
/// `DEADLINE_EXCEEDED` status is returned.
///
/// When no `grpc-timeout` header is present, the request is passed through
/// unchanged.
///
/// # Example
///
/// ```ignore
/// use grpc::server::interceptor::deadline::DeadlineInterceptor;
///
/// let deadline = DeadlineInterceptor::new(runtime.clone());
/// let server = Server::builder_with(deadline)
///     .add_service(my_service)
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct DeadlineInterceptor {
    runtime: GrpcRuntime,
}

impl DeadlineInterceptor {
    /// Creates a new `DeadlineInterceptor` using the given runtime for
    /// sleep timers.
    pub fn new(runtime: GrpcRuntime) -> Self {
        Self { runtime }
    }
}

impl Intercept for DeadlineInterceptor {
    async fn intercept(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
        tx: &mut impl SendStream,
        rx: impl RecvStream + 'static,
        next: &impl Handle,
    ) -> Trailers {
        let timeout = headers
            .metadata()
            .get("grpc-timeout")
            .map(|val| val.to_str())
            .and_then(parse_grpc_timeout);

        if let Some(duration) = timeout {
            let sleep = self.runtime.sleep(duration);

            // Race the handler against the timeout. If the timeout wins,
            // the handler future is dropped — cooperative cancellation.
            tokio::select! {
                trailers = next.handle(headers, options, tx, rx) => {
                    trailers
                }
                _ = sleep => {
                    Trailers::new(Err(StatusError::new(
                        StatusCodeError::DeadlineExceeded,
                        "context deadline exceeded",
                    )))
                }
            }
        } else {
            // No timeout header — pass through.
            next.handle(headers, options, tx, rx).await
        }
    }
}

/// Parses the gRPC timeout format into a [`Duration`].
///
/// The format is `<value><unit>` where unit is one of:
/// - `H` — hours
/// - `M` — minutes
/// - `S` — seconds
/// - `m` — milliseconds
/// - `u` — microseconds
/// - `n` — nanoseconds
///
/// Examples: `"100m"` (100ms), `"2S"` (2 seconds), `"1H"` (1 hour).
pub(crate) fn parse_grpc_timeout(s: &str) -> Option<Duration> {
    if s.is_empty() {
        return None;
    }
    let (val_str, unit) = s.split_at(s.len() - 1);
    let val: u64 = val_str.parse().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(val * 3600)),
        "M" => Some(Duration::from_secs(val * 60)),
        "S" => Some(Duration::from_secs(val)),
        "m" => Some(Duration::from_millis(val)),
        "u" => Some(Duration::from_micros(val)),
        "n" => Some(Duration::from_nanos(val)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{RecvMessage, ServerResponseStreamItem};
    use crate::server::SendOptions;

    // --- parse_grpc_timeout tests ---

    #[test]
    fn parse_hours() {
        assert_eq!(parse_grpc_timeout("1H"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7200)));
    }

    #[test]
    fn parse_minutes() {
        assert_eq!(parse_grpc_timeout("5M"), Some(Duration::from_secs(300)));
    }

    #[test]
    fn parse_seconds() {
        assert_eq!(parse_grpc_timeout("30S"), Some(Duration::from_secs(30)));
    }

    #[test]
    fn parse_milliseconds() {
        assert_eq!(parse_grpc_timeout("100m"), Some(Duration::from_millis(100)));
        assert_eq!(parse_grpc_timeout("0m"), Some(Duration::from_millis(0)));
    }

    #[test]
    fn parse_microseconds() {
        assert_eq!(parse_grpc_timeout("500u"), Some(Duration::from_micros(500)));
    }

    #[test]
    fn parse_nanoseconds() {
        assert_eq!(
            parse_grpc_timeout("1000000n"),
            Some(Duration::from_nanos(1_000_000))
        );
    }

    #[test]
    fn parse_empty_returns_none() {
        assert_eq!(parse_grpc_timeout(""), None);
    }

    #[test]
    fn parse_invalid_unit_returns_none() {
        assert_eq!(parse_grpc_timeout("100x"), None);
    }

    #[test]
    fn parse_invalid_value_returns_none() {
        assert_eq!(parse_grpc_timeout("abcS"), None);
    }

    #[test]
    fn parse_single_digit() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
    }

    // --- DeadlineInterceptor integration tests ---

    struct MockSendStream;
    impl SendStream for MockSendStream {
        async fn send<'a>(
            &mut self,
            _item: ServerResponseStreamItem<'a>,
            _options: SendOptions,
        ) -> Result<(), ()> {
            Ok(())
        }
    }

    struct MockRecvStream;
    impl RecvStream for MockRecvStream {
        async fn next(&mut self, _msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
            None
        }
    }

    /// A handler that completes immediately.
    struct InstantHandler;
    impl Handle for InstantHandler {
        async fn handle(
            &self,
            _headers: RequestHeaders,
            _options: CallOptions,
            _tx: &mut impl SendStream,
            _rx: impl RecvStream + 'static,
        ) -> Trailers {
            Trailers::new(Ok(()))
        }
    }

    /// A handler that sleeps for a given duration before completing.
    struct SlowHandler {
        delay: Duration,
    }
    impl Handle for SlowHandler {
        async fn handle(
            &self,
            _headers: RequestHeaders,
            _options: CallOptions,
            _tx: &mut impl SendStream,
            _rx: impl RecvStream + 'static,
        ) -> Trailers {
            tokio::time::sleep(self.delay).await;
            Trailers::new(Ok(()))
        }
    }

    #[tokio::test]
    async fn no_timeout_header_passes_through() {
        let runtime = crate::rt::default_runtime();
        let interceptor = DeadlineInterceptor::new(runtime);
        let handler = InstantHandler;

        let headers = RequestHeaders::new().with_method_name("/test/Method");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = interceptor
            .intercept(headers, CallOptions::default(), &mut tx, rx, &handler)
            .await;

        assert!(trailers.status().is_ok());
    }

    #[tokio::test]
    async fn handler_completes_before_timeout() {
        let runtime = crate::rt::default_runtime();
        let interceptor = DeadlineInterceptor::new(runtime);
        let handler = InstantHandler;

        let headers = RequestHeaders::new()
            .with_method_name("/test/Method")
            .with_metadata({
                let mut m = crate::metadata::MetadataMap::new();
                m.insert("grpc-timeout", "10S".parse().unwrap());
                m
            });
        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = interceptor
            .intercept(headers, CallOptions::default(), &mut tx, rx, &handler)
            .await;

        // Handler completes instantly — well within 10s timeout.
        assert!(trailers.status().is_ok());
    }

    #[tokio::test]
    async fn handler_exceeds_timeout_returns_deadline_exceeded() {
        let runtime = crate::rt::default_runtime();
        let interceptor = DeadlineInterceptor::new(runtime);
        // Handler sleeps for 1 second.
        let handler = SlowHandler {
            delay: Duration::from_secs(1),
        };

        let headers = RequestHeaders::new()
            .with_method_name("/test/Method")
            .with_metadata({
                let mut m = crate::metadata::MetadataMap::new();
                // Timeout of 10ms — handler will exceed this.
                m.insert("grpc-timeout", "10m".parse().unwrap());
                m
            });
        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = interceptor
            .intercept(headers, CallOptions::default(), &mut tx, rx, &handler)
            .await;

        let err = trailers.status().as_ref().unwrap_err();
        assert_eq!(err.code(), StatusCodeError::DeadlineExceeded);
        assert!(err.message().contains("deadline exceeded"));
    }
}
