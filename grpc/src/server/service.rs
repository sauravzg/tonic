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

use crate::server::Handle;
use crate::server::descriptor::{MethodDescriptor, ServiceDescriptor};
use crate::server::interceptor::{HandleExt, Intercept};

/// A visitor that accepts generic handlers before type-erasure.
///
/// Unlike registering with `Arc<dyn DynHandle>` directly, this trait takes
/// a generic `H: Handle`, which allows callers to wrap interceptors around
/// the handler before it is erased.
pub trait ServiceRegistrar {
    /// Registers `handler` for the given method described by `descriptor`.
    ///
    /// The descriptor carries the full method path (e.g.
    /// `"/package.Service/Method"`) and method type.
    fn register_method<H>(&mut self, descriptor: MethodDescriptor, handler: H)
    where
        H: Handle + Send + Sync + 'static;
}

/// A gRPC service that can register its methods with a [`ServiceRegistrar`].
///
/// Implementations use the visitor pattern: the service consumes itself and
/// pushes each method handler into the provided registrar.
///
/// # Example
///
/// ```ignore
/// use grpc::server::descriptor::*;
///
/// struct EchoService { /* ... */ }
///
/// impl Service for EchoService {
///     fn descriptor(&self) -> ServiceDescriptor {
///         ServiceDescriptor::new_static("mypackage.Echo", &[
///             MethodDescriptor::new_static("/mypackage.Echo/UnaryEcho", MethodType::Unary),
///         ])
///     }
///
///     fn register_methods(self, registrar: &mut impl ServiceRegistrar) {
///         registrar.register_method(
///             MethodDescriptor::new_static("/mypackage.Echo/UnaryEcho", MethodType::Unary),
///             self.unary_handler(),
///         );
///     }
/// }
/// ```
pub trait Service {
    /// Returns the service descriptor (pure metadata).
    ///
    /// This provides service and method metadata without registering handlers,
    /// enabling use cases like server reflection and service listing.
    fn descriptor(&self) -> ServiceDescriptor;

    /// Registers all method handlers for this service with the given registrar.
    fn register_methods(self, registrar: &mut impl ServiceRegistrar);
}

/// A service wrapped with an interceptor that applies to all its methods.
///
/// Created by [`ServiceExt::with_interceptor()`]. The interceptor is applied
/// to each method handler at registration time (before type erasure), not at
/// call time.
pub struct InterceptedService<S, I> {
    service: S,
    interceptor: I,
}

impl<S, I> Service for InterceptedService<S, I>
where
    S: Service,
    I: Intercept + Clone + Send + Sync + 'static,
{
    fn descriptor(&self) -> ServiceDescriptor {
        self.service.descriptor()
    }

    fn register_methods(self, registrar: &mut impl ServiceRegistrar) {
        let mut wrapping = InterceptingRegistrar {
            inner: registrar,
            interceptor: self.interceptor,
        };
        self.service.register_methods(&mut wrapping);
    }
}

/// A registrar wrapper that applies an interceptor to each registered method.
///
/// This sits between the `Service` and the actual `ServiceRegistrar` (e.g.,
/// `RouterBuilder`), wrapping each handler with the interceptor while the
/// handler is still generic (before type erasure).
pub(crate) struct InterceptingRegistrar<'a, R, I> {
    inner: &'a mut R,
    interceptor: I,
}

impl<R, I> ServiceRegistrar for InterceptingRegistrar<'_, R, I>
where
    R: ServiceRegistrar,
    I: Intercept + Clone + Send + Sync + 'static,
{
    fn register_method<H>(&mut self, descriptor: MethodDescriptor, handler: H)
    where
        H: Handle + Send + Sync + 'static,
    {
        self.inner.register_method(
            descriptor,
            handler.with_interceptor(self.interceptor.clone()),
        );
    }
}

/// Extension trait for composing interceptors on services.
pub trait ServiceExt: Service + Sized {
    /// Wraps this service with an interceptor that applies to all its methods.
    ///
    /// This is a pre-registration transformation: the interceptor is applied
    /// when the service registers its methods, not at call time. Each method
    /// handler is wrapped while still generic (before type erasure).
    ///
    /// Equivalent to Java's `ServerInterceptors.intercept(service, interceptor)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let rate_limited_greeter = greeter_service.with_interceptor(rate_limiter);
    /// ```
    fn with_interceptor<I: Intercept>(self, interceptor: I) -> InterceptedService<Self, I> {
        InterceptedService {
            service: self,
            interceptor,
        }
    }
}

impl<T: Service> ServiceExt for T {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use super::*;
    use crate::client::CallOptions;
    use crate::core::RecvMessage;
    use crate::core::RequestHeaders;
    use crate::core::ServerResponseStreamItem;
    use crate::core::Trailers;
    use crate::server::SendOptions;
    use crate::server::descriptor::{MethodType, ServiceDescriptor};
    use crate::server::interceptor::Intercept;
    use crate::server::router::RouterBuilder;
    use crate::server::{Handle, RecvStream, SendStream};

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

    /// An interceptor that records that it ran by pushing its id.
    #[derive(Clone)]
    struct TrackingInterceptor {
        id: usize,
        order: Arc<Mutex<Vec<usize>>>,
    }

    impl Intercept for TrackingInterceptor {
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

    /// A handler that pushes 0 to confirm it was called.
    struct TrackingHandler {
        order: Arc<Mutex<Vec<usize>>>,
    }

    impl Handle for TrackingHandler {
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

    /// A mock service that registers one method with a tracking handler.
    struct MockService {
        order: Arc<Mutex<Vec<usize>>>,
    }

    impl Service for MockService {
        fn descriptor(&self) -> ServiceDescriptor {
            ServiceDescriptor::new(
                "test.MockService",
                vec![MethodDescriptor::new(
                    "/test.MockService/Method",
                    MethodType::Unary,
                )],
            )
        }

        fn register_methods(self, registrar: &mut impl ServiceRegistrar) {
            registrar.register_method(
                MethodDescriptor::new("/test.MockService/Method", MethodType::Unary),
                TrackingHandler {
                    order: self.order,
                },
            );
        }
    }

    #[test]
    fn intercepted_service_preserves_descriptor() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let svc = MockService {
            order: order.clone(),
        };
        let interceptor = TrackingInterceptor {
            id: 1,
            order: order.clone(),
        };

        let intercepted = svc.with_interceptor(interceptor);
        let desc = intercepted.descriptor();
        assert_eq!(desc.name, "test.MockService");
        assert_eq!(desc.methods.len(), 1);
        assert_eq!(desc.methods[0].full_path, "/test.MockService/Method");
    }

    #[tokio::test]
    async fn intercepted_service_applies_interceptor_to_handler() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let svc = MockService {
            order: order.clone(),
        };
        let interceptor = TrackingInterceptor {
            id: 1,
            order: order.clone(),
        };

        // Wrap service with interceptor and register via RouterBuilder.
        let intercepted = svc.with_interceptor(interceptor);
        let router = RouterBuilder::new()
            .add_service(intercepted)
            .build();

        // Invoke the handler.
        let headers = RequestHeaders::new().with_method_name("/test.MockService/Method");
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
    async fn intercepted_service_chains_multiple_interceptors() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let svc = MockService {
            order: order.clone(),
        };
        let int_a = TrackingInterceptor {
            id: 1,
            order: order.clone(),
        };
        let int_b = TrackingInterceptor {
            id: 2,
            order: order.clone(),
        };

        // Chain: service -> int_a -> int_b
        // Execution: int_b runs first (outermost), then int_a, then handler.
        let intercepted = svc.with_interceptor(int_a).with_interceptor(int_b);
        let router = RouterBuilder::new()
            .add_service(intercepted)
            .build();

        let headers = RequestHeaders::new().with_method_name("/test.MockService/Method");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;
        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        assert!(trailers.status().is_ok());
        // Outermost interceptor (2) runs first, then (1), then handler (0).
        assert_eq!(*order.lock().await, vec![2, 1, 0]);
    }

    #[tokio::test]
    async fn service_without_interceptor_handler_runs_directly() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let svc = MockService {
            order: order.clone(),
        };

        let router = RouterBuilder::new()
            .add_service(svc)
            .build();

        let headers = RequestHeaders::new().with_method_name("/test.MockService/Method");
        let mut tx = MockSendStream;
        let rx = MockRecvStream;
        let trailers = router
            .handle(headers, CallOptions::default(), &mut tx, rx)
            .await;

        assert!(trailers.status().is_ok());
        // Only handler (0), no interceptors.
        assert_eq!(*order.lock().await, vec![0]);
    }
}
