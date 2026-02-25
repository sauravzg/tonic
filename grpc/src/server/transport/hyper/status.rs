use crate::status::StatusCode;

/// Infers a gRPC status code from an HTTP status code.
///
/// This function implements a hybrid strategy:
/// - Values <= 16 are treated as standard gRPC status codes.
/// - Values >= 100 are treated as HTTP status codes and mapped to gRPC status codes
///   according to the [standard mapping](https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md).
pub fn infer_grpc_status_from_http_status(code: i32) -> StatusCode {
    match code {
        200 => StatusCode::Ok,
        400 => StatusCode::Internal,
        401 => StatusCode::Unauthenticated,
        403 => StatusCode::PermissionDenied,
        404 => StatusCode::Unimplemented,
        429 => StatusCode::Unavailable,
        502 => StatusCode::Unavailable,
        503 => StatusCode::Unavailable,
        504 => StatusCode::Unavailable,
        _ => StatusCode::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_grpc_status_from_http_status() {
        // gRPC status codes should be treated as Unknown (since they are not valid HTTP codes)
        assert_eq!(infer_grpc_status_from_http_status(0), StatusCode::Unknown);
        assert_eq!(infer_grpc_status_from_http_status(5), StatusCode::Unknown);
        assert_eq!(infer_grpc_status_from_http_status(16), StatusCode::Unknown);

        // HTTP status codes
        assert_eq!(infer_grpc_status_from_http_status(200), StatusCode::Ok);
        assert_eq!(
            infer_grpc_status_from_http_status(400),
            StatusCode::Internal
        );
        assert_eq!(
            infer_grpc_status_from_http_status(401),
            StatusCode::Unauthenticated
        );
        assert_eq!(
            infer_grpc_status_from_http_status(403),
            StatusCode::PermissionDenied
        );
        assert_eq!(
            infer_grpc_status_from_http_status(404),
            StatusCode::Unimplemented
        );
        assert_eq!(
            infer_grpc_status_from_http_status(429),
            StatusCode::Unavailable
        );
        assert_eq!(
            infer_grpc_status_from_http_status(502),
            StatusCode::Unavailable
        );
        assert_eq!(
            infer_grpc_status_from_http_status(503),
            StatusCode::Unavailable
        );
        assert_eq!(
            infer_grpc_status_from_http_status(504),
            StatusCode::Unavailable
        );

        // Unknown
        assert_eq!(infer_grpc_status_from_http_status(99), StatusCode::Unknown);
        assert_eq!(infer_grpc_status_from_http_status(500), StatusCode::Unknown);
    }
}
