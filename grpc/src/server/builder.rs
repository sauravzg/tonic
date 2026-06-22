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

use crate::server::Server;
use crate::server::interceptor::Identity;
use crate::server::interceptor::Intercept;
use crate::server::router::RouterBuilder;
use crate::server::service::Service;

/// A fluent builder for constructing a [`Server`].
///
/// The interceptor chain is set at construction time and applies to all
/// services registered through this builder. Use [`Server::builder()`] for
/// no interceptors, or [`Server::builder_with()`] to provide an interceptor
/// chain.
///
/// # Examples
///
/// ```ignore
/// // No interceptors
/// let server = Server::builder()
///     .add_service(greeter_service)
///     .build();
///
/// // With interceptors
/// use grpc::server::interceptor::ComposedIntercept;
/// let chain = ComposedIntercept::new(logging, auth);
/// let server = Server::builder_with(chain)
///     .add_service(greeter_service)
///     .build();
/// ```
pub struct ServerBuilder<I = Identity> {
    router: RouterBuilder<I>,
}

impl ServerBuilder<Identity> {
    /// Creates a new `ServerBuilder` with no interceptors.
    pub(crate) fn new() -> Self {
        ServerBuilder {
            router: RouterBuilder::new(),
        }
    }
}

impl<I> ServerBuilder<I>
where
    I: Intercept + Clone + Send + Sync + 'static,
{
    /// Creates a new `ServerBuilder` with the given interceptor chain.
    ///
    /// All services registered through this builder will have their method
    /// handlers wrapped with `interceptor` before type erasure.
    pub(crate) fn new_with_interceptor(interceptor: I) -> Self {
        ServerBuilder {
            router: RouterBuilder::new_with_interceptor(interceptor),
        }
    }

    /// Registers all methods from a [`Service`].
    ///
    /// The service's method handlers are wrapped with the builder's interceptor
    /// chain before type erasure.
    pub fn add_service(mut self, service: impl Service) -> Self {
        self.router = self.router.add_service(service);
        self
    }

    /// Builds the [`Server`] with all registered services.
    pub fn build(self) -> Server {
        let router = self.router.build();
        let mut server = Server::new();
        server.set_handler(router);
        server
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use crate::client::CallOptions;
    use crate::core::RecvMessage;
    use crate::core::RequestHeaders;
    use crate::core::ServerResponseStreamItem;
    use crate::core::Trailers;
    use crate::server::Handle;
    use crate::server::RecvStream;
    use crate::server::SendOptions;
    use crate::server::SendStream;
    use crate::server::Server;
    use crate::server::descriptor::{MethodDescriptor, MethodType, ServiceDescriptor};
    use crate::server::interceptor::{ComposedIntercept, Intercept};
    use crate::server::service::{Service, ServiceRegistrar};

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

    struct TestService {
        order: Arc<Mutex<Vec<usize>>>,
    }

    impl Service for TestService {
        fn descriptor(&self) -> ServiceDescriptor {
            ServiceDescriptor::new(
                "test.Svc",
                vec![MethodDescriptor::new("/test.Svc/Method", MethodType::Unary)],
            )
        }

        fn register_methods(self, registrar: &mut impl ServiceRegistrar) {
            registrar.register_method(
                MethodDescriptor::new("/test.Svc/Method", MethodType::Unary),
                TrackingHandler { order: self.order },
            );
        }
    }

    #[test]
    fn server_builder_builds_without_services() {
        // Should not panic — creates a server with empty router.
        let _server = Server::builder().build();
    }

    #[tokio::test]
    async fn server_builder_with_interceptor_chain_and_service() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let int_a = TrackingInterceptor {
            id: 1,
            order: order.clone(),
        };
        let int_b = TrackingInterceptor {
            id: 2,
            order: order.clone(),
        };
        let chain = ComposedIntercept::new(int_a, int_b);

        let svc = TestService {
            order: order.clone(),
        };

        // This should not panic — the chain is applied at add_service time.
        let _server = Server::builder_with(chain).add_service(svc).build();
    }

    #[test]
    fn server_builder_multiple_services() {
        struct SvcA;
        impl Service for SvcA {
            fn descriptor(&self) -> ServiceDescriptor {
                ServiceDescriptor::new("test.A", vec![])
            }
            fn register_methods(self, _registrar: &mut impl ServiceRegistrar) {}
        }

        struct SvcB;
        impl Service for SvcB {
            fn descriptor(&self) -> ServiceDescriptor {
                ServiceDescriptor::new("test.B", vec![])
            }
            fn register_methods(self, _registrar: &mut impl ServiceRegistrar) {}
        }

        let _server = Server::builder()
            .add_service(SvcA)
            .add_service(SvcB)
            .build();
    }
}
