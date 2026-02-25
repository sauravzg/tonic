use crate::codec::serialization::{Deserialize, Serialize};
use crate::server::message::{AsMut, AsView};
use crate::server::{
    BidiStreamingMethod, ClientStreamingMethod, ServerStreamingMethod, UnaryMethod,
};
/// A trait for registering gRPC methods.
pub trait ServiceRegistrar {
    /// Registers a unary method.
    fn register_unary<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: UnaryMethod<Req, Resp> + Send + Sync + 'static;

    /// Registers a server streaming method.
    fn register_server_streaming<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: ServerStreamingMethod<Req, Resp> + Send + Sync + 'static;

    /// Registers a client streaming method.
    fn register_client_streaming<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: ClientStreamingMethod<Req, Resp> + Send + Sync + 'static;

    /// Registers a bidi streaming method.
    fn register_bidi_streaming<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: BidiStreamingMethod<Req, Resp> + Send + Sync + 'static;
}

/// A trait for gRPC services that can register their methods.
pub trait Service {
    /// Registers the service's methods with the given registrar.
    fn register_methods<R>(self, registrar: &mut R)
    where
        R: ServiceRegistrar;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::serialization::{Deserialize, Serialize};
    use crate::server::message::{AsMut, AsView};
    use crate::status::ServerStatus;

    use protobuf_well_known_types::Timestamp;

    struct MockRegistrar;

    impl ServiceRegistrar for MockRegistrar {
        fn register_unary<Req, Resp, H>(&mut self, _path: &str, _handler: H)
        where
            Req: AsView + AsMut + Default + Send + 'static,
            Resp: AsView + AsMut + Default + Send + 'static,
            for<'b> <Req as AsMut>::Mut<'b>: Send + Deserialize,
            for<'b> <Resp as AsMut>::Mut<'b>: Send + Serialize,
            H: UnaryMethod<Req, Resp> + Send + Sync + 'static,
        {
        }

        fn register_server_streaming<Req, Resp, H>(&mut self, _path: &str, _handler: H)
        where
            Req: AsView + AsMut + Default + Send + Deserialize + 'static,
            Resp: AsView + AsMut + Default + Send + Serialize + 'static,
            H: ServerStreamingMethod<Req, Resp> + Send + Sync + 'static,
        {
        }

        fn register_client_streaming<Req, Resp, H>(&mut self, _path: &str, _handler: H)
        where
            Req: AsView + AsMut + Default + Send + Deserialize + 'static,
            Resp: AsView + AsMut + Default + Send + Serialize + 'static,
            H: ClientStreamingMethod<Req, Resp> + Send + Sync + 'static,
        {
        }

        fn register_bidi_streaming<Req, Resp, H>(&mut self, _path: &str, _handler: H)
        where
            Req: AsView + AsMut + Default + Send + Deserialize + 'static,
            Resp: AsView + AsMut + Default + Send + Serialize + 'static,
            H: BidiStreamingMethod<Req, Resp> + Send + Sync + 'static,
        {
        }
    }

    struct MockService;

    impl Service for MockService {
        fn register_methods<R>(self, registrar: &mut R)
        where
            R: ServiceRegistrar,
        {
            registrar.register_unary("/test", MockUnaryHandler);
        }
    }

    struct MockUnaryHandler;

    impl UnaryMethod<Timestamp, Timestamp> for MockUnaryHandler {
        async fn unary(
            &self,
            _req: <Timestamp as AsView>::View<'_>,
            _resp: <Timestamp as AsMut>::Mut<'_>,
        ) -> Result<(), ServerStatus> {
            Ok(())
        }
    }

    #[test]
    fn test_service_registration() {
        let mut registrar = MockRegistrar;
        let service = MockService;
        service.register_methods(&mut registrar);
    }
}
