use super::buffer::BufList;
use bytes::{Buf, Bytes};

use crate::server::call::message_wrapper::MessageReadOptions;
use crate::server::call::Incoming;

/// A synchronous decoder for Hyper body chunks.
///
/// This decoder accumulates bytes from a `Hyper` body and decodes them into
/// gRPC messages. It handles the gRPC header parsing (compression flag + length)
/// and returns `RawMessage`s containing the message payload.
///
/// # Example
///
/// ```
/// use grpc::transport::hyper::decoder::HyperBodyDecoder;
/// use bytes::{Bytes, Buf};
///
/// let mut decoder = HyperBodyDecoder::new();
/// decoder.push(Bytes::from_static(b"\x00\x00\x00\x00\x05hello"));
///
/// let msg = decoder.decode().unwrap();
/// assert_eq!(msg.message_bytes.chunk(), b"hello");
/// assert!(!msg.options.unwrap().compressed);
/// ```
#[derive(Debug)]
pub struct HyperBodyDecoder {
    buffer: BufList<Bytes>,
}

impl HyperBodyDecoder {
    /// Creates a new, empty `HyperBodyDecoder`.
    pub fn new() -> Self {
        Self {
            buffer: BufList::empty(),
        }
    }

    /// Pushes a chunk of bytes into the decoder's internal buffer.
    pub fn push(&mut self, chunk: Bytes) {
        self.buffer.push(chunk);
    }

    /// Attempts to decode a single gRPC message from the internal buffer.
    ///
    /// Returns `Some(RawMessage)` if a complete message is available, or `None`
    /// if more data is needed.
    ///
    /// This method strips the 5-byte gRPC header and includes the compression
    /// flag in the returned `RawMessage`.
    pub fn decode(&mut self) -> Option<Incoming<BufList<Bytes>>> {
        const GRPC_HEADER_LEN: usize = 5;

        if self.buffer.remaining() < GRPC_HEADER_LEN {
            return None;
        }

        let mut header_buf = [0u8; GRPC_HEADER_LEN];
        self.buffer.peek_copy_to(&mut &mut header_buf[..]);

        let body_len =
            u32::from_be_bytes([header_buf[1], header_buf[2], header_buf[3], header_buf[4]])
                as usize;

        let msg_len = GRPC_HEADER_LEN + body_len;

        if self.buffer.remaining() < msg_len {
            return None;
        }

        let mut msg = self
            .buffer
            .split_to(msg_len)
            .expect("buffer should have enough bytes");
        msg.advance(GRPC_HEADER_LEN);

        Some(Incoming {
            message_bytes: msg,
            options: Some(MessageReadOptions {
                compressed: header_buf[0] != 0,
            }),
        })
    }

    /// Returns the number of bytes currently in the internal buffer.
    pub fn buffer_len(&self) -> usize {
        self.buffer.remaining()
    }
}

impl Default for HyperBodyDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    #[test]
    fn test_decode_partial_header() {
        let mut decoder = HyperBodyDecoder::new();
        decoder.push(Bytes::from_static(&[0, 0, 0]));
        assert!(decoder.decode().is_none());
    }

    #[test]
    fn test_decode_partial_body() {
        let mut decoder = HyperBodyDecoder::new();
        // Header: not compressed, length 5
        decoder.push(Bytes::from_static(&[0, 0, 0, 0, 5]));
        // Partial body
        decoder.push(Bytes::from_static(b"hel"));
        assert!(decoder.decode().is_none());
    }

    #[test]
    fn test_decode_complete_message() {
        let mut decoder = HyperBodyDecoder::new();
        decoder.push(Bytes::from_static(b"\x00\x00\x00\x00\x05hello"));

        let msg = decoder.decode().expect("should decode");
        assert_eq!(msg.message_bytes.remaining(), 5);
        assert_eq!(msg.message_bytes.chunk(), b"hello");
        assert!(!msg.options.unwrap().compressed);
    }

    #[test]
    fn test_decode_multiple_messages() {
        let mut decoder = HyperBodyDecoder::new();

        // Msg 1: "hi"
        decoder.push(Bytes::from_static(b"\x00\x00\x00\x00\x02hi"));
        // Msg 2: "world"
        decoder.push(Bytes::from_static(b"\x00\x00\x00\x00\x05world"));

        let msg1 = decoder.decode().expect("should decode msg1");
        assert_eq!(msg1.message_bytes.remaining(), 2);

        let msg2 = decoder.decode().expect("should decode msg2");
        assert_eq!(msg2.message_bytes.remaining(), 5);

        assert!(decoder.decode().is_none());
    }

    #[test]
    fn test_decode_compressed_flag() {
        let mut decoder = HyperBodyDecoder::new();
        // Compressed flag set (1)
        decoder.push(Bytes::from_static(b"\x01\x00\x00\x00\x05hello"));

        let msg = decoder.decode().expect("should decode");
        assert!(msg.options.unwrap().compressed);
    }

    #[test]
    fn test_decode_split_frames() {
        let mut decoder = HyperBodyDecoder::new();
        // Header split across pushes
        decoder.push(Bytes::from_static(&[0, 0]));
        decoder.push(Bytes::from_static(&[0, 0, 5]));
        // Body split across pushes
        decoder.push(Bytes::from_static(b"he"));
        decoder.push(Bytes::from_static(b"llo"));

        let msg = decoder.decode().expect("should decode");
        assert_eq!(msg.message_bytes.remaining(), 5);
    }
}
