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

use std::collections::HashMap;
use std::sync::Arc;

use crate::StatusCodeError;
use crate::StatusError;
use crate::client::CallOptions;
use crate::core::RequestHeaders;
use crate::core::Trailers;
use crate::server::BoxedRecvStream;
use crate::server::DynHandle;
use crate::server::DynRecvStream;
use crate::server::DynSendStream;
use crate::server::Handle;
use crate::server::interceptor::HandleExt;
use crate::server::RecvStream;
use crate::server::SendStream;
use crate::server::interceptor::Identity;
use crate::server::interceptor::Intercept;
use crate::server::descriptor::{MethodDescriptor, ServiceDescriptor};
use crate::server::service::Service;
use crate::server::service::ServiceRegistrar;

/// A builder for constructing an immutable [`Router`].
///
/// Methods can be added individually via [`add_method`](RouterBuilder::add_method)
/// or in bulk via [`add_service`](RouterBuilder::add_service).
///
/// The type parameter `I` represents the interceptor stack applied to all
/// handlers registered through this builder. It is set at construction time
/// and cannot be changed.
///
/// This type is `pub(crate)` — users interact with [`ServerBuilder`] instead.
pub(crate) struct RouterBuilder<I = Identity> {
    handlers: HashMap<String, Arc<dyn DynHandle>>,
    descriptors: Vec<ServiceDescriptor>,
    interceptor: I,
}

impl RouterBuilder {
    /// Creates a new, empty `RouterBuilder` with no interceptors.
    pub(crate) fn new() -> RouterBuilder<Identity> {
        RouterBuilder {
            handlers: HashMap::new(),
            descriptors: Vec::new(),
            interceptor: Identity,
        }
    }
}

impl<I> RouterBuilder<I>
where
    I: Intercept + Clone + Send + Sync + 'static,
{
    /// Creates a new `RouterBuilder` with the given interceptor chain.
    ///
    /// All handlers registered through this builder will be wrapped with
    /// `interceptor` before type erasure.
    pub(crate) fn new_with_interceptor(interceptor: I) -> Self {
        RouterBuilder {
            handlers: HashMap::new(),
            descriptors: Vec::new(),
            interceptor,
        }
    }

    /// Registers `handler` for the given method.
    ///
    /// If a handler was already registered for the same path, it is replaced
    /// (last-one-wins).
    ///
    /// The handler is wrapped with the builder's interceptor stack before
    /// type-erasure.
    pub(crate) fn add_method<H>(mut self, descriptor: MethodDescriptor, handler: H) -> Self
    where
        H: Handle + Send + Sync + 'static,
    {
        let wrapped = handler.with_interceptor(self.interceptor.clone());
        self.handlers.insert(descriptor.full_path.into_owned(), Arc::new(wrapped));
        self
    }

    /// Registers all methods from a [`Service`] using the visitor pattern.
    ///
    /// The service's [`register_methods`](Service::register_methods) is called
    /// with `self` as the [`ServiceRegistrar`]. Each method handler is wrapped
    /// with the builder's interceptor stack.
    pub(crate) fn add_service(mut self, service: impl Service) -> Self {
        self.descriptors.push(service.descriptor());
        service.register_methods(&mut self);
        self
    }

    /// Consumes this builder and produces an immutable [`Router`].
    pub(crate) fn build(self) -> Router {
        Router {
            handlers: self.handlers,
        }
    }
}

impl Default for RouterBuilder<Identity> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> ServiceRegistrar for RouterBuilder<I>
where
    I: Intercept + Clone + Send + Sync + 'static,
{
    fn register_method<H>(&mut self, descriptor: MethodDescriptor, handler: H)
    where
        H: Handle + Send + Sync + 'static,
    {
        let wrapped = handler.with_interceptor(self.interceptor.clone());
        self.handlers.insert(descriptor.full_path.into_owned(), Arc::new(wrapped));
    }
}

/// Routes incoming gRPC RPCs to the correct handler based on the request's
/// method name.
///
/// `Router` implements [`Handle`], so it can be passed directly to
/// [`Server::set_handler`](crate::server::Server::set_handler).
///
/// A `Router` is immutable once built; use [`RouterBuilder`] to construct one.
///
/// # Example
///
/// ```ignore
/// let router = RouterBuilder::new()
///     .add_method("/mypackage.Echo/UnaryEcho", echo_handler)
///     .add_method("/mypackage.Echo/ServerStreamingEcho", stream_handler)
///     .build();
///
/// let mut server = grpc::server::Server::new();
/// server.set_handler(router);
/// ```
pub struct Router {
    handlers: HashMap<String, Arc<dyn DynHandle>>,
}

impl Handle for Router {
    async fn handle(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
        tx: &mut impl SendStream,
        rx: impl RecvStream + 'static,
    ) -> Trailers {
        if let Some(handler) = self.handlers.get(headers.method_name()) {
            // Bridge from `impl SendStream` → `&mut dyn DynSendStream` and
            // `impl RecvStream` → `BoxedRecvStream`, matching the blanket
            // `DynHandle for T: Handle` pattern.
            let mut dyn_tx: &mut dyn DynSendStream = tx;
            let boxed_rx = BoxedRecvStream(Box::new(rx) as Box<dyn DynRecvStream + 'static>);
            handler
                .dyn_handle(headers, options, &mut dyn_tx, boxed_rx)
                .await
        } else {
            Trailers::new(Err(StatusError::new(
                StatusCodeError::Unimplemented,
                format!("unknown method: {}", headers.method_name()),
            )))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use super::*;
    use crate::client::CallOptions;
    use crate::core::RecvMessage;
    use crate::core::RequestHeaders;
    use crate::core::ServerResponseStreamItem;
    use crate::server::SendOptions;
    use crate::server::descriptor::{MethodDescriptor, MethodType, ServiceDescriptor};

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

    /// A handler that records its method name when called and returns OK.
    struct RecordingHandler {
        called_with: Arc<Mutex<Option<String>>>,
    }

    impl Handle for RecordingHandler {
        async fn handle(
            &self,
            headers: RequestHeaders,
            _options: CallOptions,
            _tx: &mut impl SendStream,
            _rx: impl RecvStream + 'static,
        ) -> Trailers {
            let mut called = self.called_with.lock().await;
            *called = Some(headers.method_name().clone());
            Trailers::new(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_registered_method_dispatches() {
        let called_with = Arc::new(Mutex::new(None));
        let handler = RecordingHandler {
            called_with: called_with.clone(),
        };

        let router = RouterBuilder::new()
            .add_method(MethodDescriptor::new("/pkg.Svc/Method", MethodType::Unary), handler)
            .build();

        let headers = RequestHeaders::new().with_method_name("/pkg.Svc/Method");

        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        assert!(trailers.status().is_ok());
        assert_eq!(
            *called_with.lock().await,
            Some("/pkg.Svc/Method".to_string())
        );
    }

    #[tokio::test]
    async fn test_unregistered_method_returns_unimplemented() {
        let router = RouterBuilder::new().build();

        let headers = RequestHeaders::new().with_method_name("/pkg.Svc/NoSuchMethod");

        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        let err = trailers.status().as_ref().unwrap_err();
        assert_eq!(err.code(), StatusCodeError::Unimplemented);
        assert!(
            err.message().contains("/pkg.Svc/NoSuchMethod"),
            "error message should contain the method name, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_last_one_wins_for_duplicate_paths() {
        let first_called = Arc::new(Mutex::new(None));
        let second_called = Arc::new(Mutex::new(None));

        let first_handler = RecordingHandler {
            called_with: first_called.clone(),
        };
        let second_handler = RecordingHandler {
            called_with: second_called.clone(),
        };

        let router = RouterBuilder::new()
            .add_method(MethodDescriptor::new("/pkg.Svc/Method", MethodType::Unary), first_handler)
            .add_method(MethodDescriptor::new("/pkg.Svc/Method", MethodType::Unary), second_handler)
            .build();

        let headers = RequestHeaders::new().with_method_name("/pkg.Svc/Method");

        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        assert!(trailers.status().is_ok());
        // The second handler should have been called (last-one-wins).
        assert_eq!(
            *second_called.lock().await,
            Some("/pkg.Svc/Method".to_string())
        );
        // The first handler should NOT have been called.
        assert!(first_called.lock().await.is_none());
    }

    #[tokio::test]
    async fn test_add_service_visitor_pattern() {
        let method_a_called = Arc::new(Mutex::new(None));
        let method_b_called = Arc::new(Mutex::new(None));

        let handler_a = RecordingHandler {
            called_with: method_a_called.clone(),
        };
        let handler_b = RecordingHandler {
            called_with: method_b_called.clone(),
        };

        struct MockService {
            handler_a: RecordingHandler,
            handler_b: RecordingHandler,
        }

        impl Service for MockService {
            fn descriptor(&self) -> ServiceDescriptor {
                ServiceDescriptor::new("mock.MockService", vec![
                    MethodDescriptor::new("/mock.MockService/MethodA", MethodType::Unary),
                    MethodDescriptor::new("/mock.MockService/MethodB", MethodType::Unary),
                ])
            }

            fn register_methods(self, registrar: &mut impl ServiceRegistrar) {
                registrar.register_method(
                    MethodDescriptor::new("/mock.MockService/MethodA", MethodType::Unary),
                    self.handler_a,
                );
                registrar.register_method(
                    MethodDescriptor::new("/mock.MockService/MethodB", MethodType::Unary),
                    self.handler_b,
                );
            }
        }

        let service = MockService {
            handler_a,
            handler_b,
        };

        let router = RouterBuilder::new().add_service(service).build();

        // Dispatch to MethodA.
        let headers_a = RequestHeaders::new().with_method_name("/mock.MockService/MethodA");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;
        let trailers = router
            .handle(headers_a, CallOptions::default(), &mut tx, rx)
            .await;
        assert!(trailers.status().is_ok());
        assert_eq!(
            *method_a_called.lock().await,
            Some("/mock.MockService/MethodA".to_string())
        );

        // Dispatch to MethodB.
        let headers_b = RequestHeaders::new().with_method_name("/mock.MockService/MethodB");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;
        let trailers = router
            .handle(headers_b, CallOptions::default(), &mut tx, rx)
            .await;
        assert!(trailers.status().is_ok());
        assert_eq!(
            *method_b_called.lock().await,
            Some("/mock.MockService/MethodB".to_string())
        );
    }

    #[tokio::test]
    async fn test_router_builder_default() {
        let router = RouterBuilder::default().build();

        let headers = RequestHeaders::new().with_method_name("/any.Service/AnyMethod");

        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        let err = trailers.status().as_ref().unwrap_err();
        assert_eq!(err.code(), StatusCodeError::Unimplemented);
    }

    // --- Interceptor-related test helpers ---

    use crate::server::interceptor::Intercept;

    /// An interceptor that pushes its `id` into a shared order vec, then
    /// delegates to `next`.
    #[derive(Clone)]
    struct OrderInterceptor {
        id: usize,
        order: Arc<Mutex<Vec<usize>>>,
    }

    impl Intercept for OrderInterceptor {
        async fn intercept(
            &self,
            headers: RequestHeaders,
            options: CallOptions,
            tx: &mut impl SendStream,
            rx: impl RecvStream + 'static,
            next: &impl Handle,
        ) -> Trailers {
            self.order.lock().await.push(self.id);
            next.handle(headers, options, tx, rx).await
        }
    }

    /// A handler that pushes `0` into the shared order vec.
    struct OrderHandler {
        order: Arc<Mutex<Vec<usize>>>,
    }

    impl Handle for OrderHandler {
        async fn handle(
            &self,
            _headers: RequestHeaders,
            _options: CallOptions,
            _tx: &mut impl SendStream,
            _rx: impl RecvStream + 'static,
        ) -> Trailers {
            self.order.lock().await.push(0);
            Trailers::new(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_router_builder_with_single_interceptor() {
        let order = Arc::new(Mutex::new(Vec::new()));

        let interceptor = OrderInterceptor {
            id: 1,
            order: order.clone(),
        };
        let handler = OrderHandler {
            order: order.clone(),
        };

        let router = RouterBuilder::new_with_interceptor(interceptor)
            .add_method(MethodDescriptor::new("/pkg.Svc/Method", MethodType::Unary), handler)
            .build();

        let headers = RequestHeaders::new().with_method_name("/pkg.Svc/Method");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        assert!(trailers.status().is_ok());
        // Interceptor (1) should run before handler (0).
        assert_eq!(*order.lock().await, vec![1, 0]);
    }

    #[tokio::test]
    async fn test_router_builder_with_chained_interceptors() {
        let order = Arc::new(Mutex::new(Vec::new()));

        let auth = OrderInterceptor {
            id: 1,
            order: order.clone(),
        };
        let logging = OrderInterceptor {
            id: 2,
            order: order.clone(),
        };
        let handler = OrderHandler {
            order: order.clone(),
        };

        // First-added-first: auth (added first) should run before logging.
        use crate::server::interceptor::ComposedIntercept;
        let chain = ComposedIntercept::new(auth, logging);
        let router = RouterBuilder::new_with_interceptor(chain)
            .add_method(MethodDescriptor::new("/pkg.Svc/Method", MethodType::Unary), handler)
            .build();

        let headers = RequestHeaders::new().with_method_name("/pkg.Svc/Method");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        assert!(trailers.status().is_ok());
        // Execution order: auth(1) → logging(2) → handler(0).
        assert_eq!(*order.lock().await, vec![1, 2, 0]);
    }

    #[tokio::test]
    async fn test_router_builder_interceptor_with_add_service() {
        let order = Arc::new(Mutex::new(Vec::new()));

        let interceptor = OrderInterceptor {
            id: 1,
            order: order.clone(),
        };

        struct InterceptedService {
            handler: OrderHandler,
        }

        impl Service for InterceptedService {
            fn descriptor(&self) -> ServiceDescriptor {
                ServiceDescriptor::new("test.InterceptedService", vec![
                    MethodDescriptor::new("/test.InterceptedService/Method", MethodType::Unary),
                ])
            }

            fn register_methods(self, registrar: &mut impl ServiceRegistrar) {
                registrar.register_method(
                    MethodDescriptor::new("/test.InterceptedService/Method", MethodType::Unary),
                    self.handler,
                );
            }
        }

        let service = InterceptedService {
            handler: OrderHandler {
                order: order.clone(),
            },
        };

        let router = RouterBuilder::new_with_interceptor(interceptor)
            .add_service(service)
            .build();

        let headers = RequestHeaders::new().with_method_name("/test.InterceptedService/Method");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;

        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        assert!(trailers.status().is_ok());
        // Interceptor (1) should run before handler (0) even via add_service.
        assert_eq!(*order.lock().await, vec![1, 0]);
    }
}
