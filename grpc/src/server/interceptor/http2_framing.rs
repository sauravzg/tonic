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

//! HTTP/2 gRPC framing: Length-Prefixed Message (LPM) deframing and framing.
//!
//! This module provides the building blocks for parsing gRPC messages from
//! raw HTTP/2 byte streams and framing outgoing messages with LPM headers.

use std::collections::VecDeque;

use bytes::{Buf, Bytes};

use crate::client::CallOptions;
use crate::codec::message::{ChunkedBuf, IncomingRawMessage};
use crate::core::{RecvMessage, RequestHeaders, SendMessage, ServerResponseStreamItem, Trailers};
use crate::server::interceptor::Intercept;
use crate::server::{Handle, RecvStream, SendOptions, SendStream};

/// Size of the gRPC Length-Prefixed Message header: 1 byte compressed flag + 4 bytes length.
const LPM_HEADER_SIZE: usize = 5;

/// Default maximum message payload size (4MB).
const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// DeframeConfig
// ---------------------------------------------------------------------------

/// Configuration for the deframing layer.
#[derive(Debug, Clone)]
pub(crate) struct DeframeConfig {
    /// Maximum allowed message payload size in bytes.
    pub max_message_size: usize,
}

impl Default for DeframeConfig {
    fn default() -> Self {
        Self {
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

// ---------------------------------------------------------------------------
// LpmHeader
// ---------------------------------------------------------------------------

/// A parsed gRPC LPM header.
#[derive(Debug, Clone, Copy)]
struct LpmHeader {
    /// Whether the payload is compressed.
    compressed: bool,
    /// Payload length in bytes.
    length: usize,
}

impl LpmHeader {
    /// Parses an LPM header from a 5-byte slice.
    fn from_bytes(hdr: &[u8; LPM_HEADER_SIZE]) -> Self {
        Self {
            compressed: hdr[0] != 0,
            length: u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize,
        }
    }
}

// ---------------------------------------------------------------------------
// ExtractedPayload
// ---------------------------------------------------------------------------

/// A payload extracted from the deframe buffer. Implements `Buf` so callers
/// can consume it uniformly regardless of whether it came from one chunk or
/// many.
pub(crate) enum ExtractedPayload {
    /// Payload was contained in a single chunk — zero-copy via `Bytes::split_to`.
    Single(Bytes),
    /// Payload spanned multiple chunks — collected handles, no copy.
    Multi(ChunkedBuf),
}

impl Buf for ExtractedPayload {
    fn remaining(&self) -> usize {
        match self {
            ExtractedPayload::Single(b) => b.remaining(),
            ExtractedPayload::Multi(c) => c.remaining(),
        }
    }

    fn chunk(&self) -> &[u8] {
        match self {
            ExtractedPayload::Single(b) => b.chunk(),
            ExtractedPayload::Multi(c) => c.chunk(),
        }
    }

    fn advance(&mut self, cnt: usize) {
        match self {
            ExtractedPayload::Single(b) => b.advance(cnt),
            ExtractedPayload::Multi(c) => c.advance(cnt),
        }
    }
}

// ---------------------------------------------------------------------------
// BytesContainer
// ---------------------------------------------------------------------------

/// A [`RecvMessage`] implementation that captures a single `Bytes` handle
/// from the source via `copy_to_bytes()` — O(1) when the source is `Bytes`.
struct BytesContainer {
    bytes: Option<Bytes>,
}

impl BytesContainer {
    fn new() -> Self {
        Self { bytes: None }
    }

    /// Takes the captured bytes, returning `Err` if nothing was captured.
    fn take(&mut self) -> Result<Bytes, ()> {
        self.bytes.take().ok_or(())
    }
}

impl RecvMessage for BytesContainer {
    fn decode(&mut self, data: &mut dyn Buf) -> Result<(), String> {
        // O(1) when data is Bytes — ref-count bump, no memcpy.
        self.bytes = Some(data.copy_to_bytes(data.remaining()));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DeframeBuf
// ---------------------------------------------------------------------------

/// Accumulates `Bytes` chunks and provides operations for parsing gRPC
/// Length-Prefixed Messages.
///
/// All operations are zero-copy in the fast path (data within a single chunk)
/// via `Bytes::split_to()` which is O(1).
struct DeframeBuf {
    chunks: VecDeque<Bytes>,
    total: usize,
}

impl DeframeBuf {
    fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            total: 0,
        }
    }

    /// Total bytes buffered across all chunks.
    fn remaining(&self) -> usize {
        self.total
    }

    fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Adds a chunk to the buffer. Empty chunks are skipped.
    fn push(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.total += chunk.len();
            self.chunks.push_back(chunk);
        }
    }

    /// Removes the front chunk if it is empty.
    fn trim_front(&mut self) {
        if let Some(front) = self.chunks.front() {
            if front.is_empty() {
                self.chunks.pop_front();
            }
        }
    }

    /// Tries to parse a 5-byte LPM header from the front of the buffer.
    ///
    /// Returns `None` if fewer than 5 bytes are buffered.
    /// Consumes the 5 header bytes on success.
    fn try_read_header(&mut self) -> Option<LpmHeader> {
        if self.total < LPM_HEADER_SIZE {
            return None;
        }

        let front = self.chunks.front()?;
        if front.len() >= LPM_HEADER_SIZE {
            Some(self.read_header_fast())
        } else {
            Some(self.read_header_slow())
        }
    }

    /// Fast path: header is entirely within the front chunk.
    /// `Bytes::split_to()` is O(1).
    fn read_header_fast(&mut self) -> LpmHeader {
        let front = self.chunks.front_mut().expect("checked by caller");
        let hdr_bytes = front.split_to(LPM_HEADER_SIZE);
        self.total -= LPM_HEADER_SIZE;
        self.trim_front();
        LpmHeader::from_bytes(&[
            hdr_bytes[0],
            hdr_bytes[1],
            hdr_bytes[2],
            hdr_bytes[3],
            hdr_bytes[4],
        ])
    }

    /// Slow path: header spans multiple chunks.
    /// Copies exactly 5 bytes to a stack array.
    fn read_header_slow(&mut self) -> LpmHeader {
        let mut hdr = [0u8; LPM_HEADER_SIZE];
        let mut offset = 0;
        while offset < LPM_HEADER_SIZE {
            let front = self.chunks.front_mut().expect("total >= 5 checked by caller");
            let available = front.len();
            let take = available.min(LPM_HEADER_SIZE - offset);
            hdr[offset..offset + take].copy_from_slice(&front[..take]);
            front.advance(take);
            self.total -= take;
            offset += take;
            self.trim_front();
        }
        LpmHeader::from_bytes(&hdr)
    }

    /// Extracts exactly `len` bytes from the front of the buffer as a
    /// zero-copy payload.
    ///
    /// Returns `None` if fewer than `len` bytes are buffered.
    fn take_payload(&mut self, len: usize) -> Option<ExtractedPayload> {
        if len == 0 {
            // Zero-length payloads are valid (e.g., protobuf Empty message).
            return Some(ExtractedPayload::Single(Bytes::new()));
        }
        if self.total < len {
            return None;
        }

        let front = self.chunks.front()?;
        if front.len() >= len {
            Some(self.take_payload_single(len))
        } else {
            Some(self.take_payload_multi(len))
        }
    }

    /// Fast path: payload fits within the front chunk.
    /// `Bytes::split_to()` is O(1).
    fn take_payload_single(&mut self, len: usize) -> ExtractedPayload {
        let front = self.chunks.front_mut().expect("checked by caller");
        let payload = front.split_to(len);
        self.total -= len;
        self.trim_front();
        ExtractedPayload::Single(payload)
    }

    /// Multi-chunk path: collects `Bytes` handles without copying.
    fn take_payload_multi(&mut self, len: usize) -> ExtractedPayload {
        let mut remaining = len;
        let mut collected = VecDeque::new();

        while remaining > 0 {
            let front = self.chunks.front_mut().expect("total >= len checked by caller");
            let available = front.len();

            if available <= remaining {
                // Take the entire front chunk.
                remaining -= available;
                self.total -= available;
                if let Some(chunk) = self.chunks.pop_front() {
                    collected.push_back(chunk);
                }
            } else {
                // Split: take what we need from the front chunk.
                // Bytes::split_to() is O(1).
                let slice = front.split_to(remaining);
                self.total -= remaining;
                collected.push_back(slice);
                remaining = 0;
            }
        }

        self.trim_front();
        ExtractedPayload::Multi(ChunkedBuf { chunks: collected })
    }
}

// ---------------------------------------------------------------------------
// DeframeState
// ---------------------------------------------------------------------------

enum DeframeState {
    /// Waiting for the 5-byte LPM header.
    ReadingHeader,
    /// Header parsed; waiting for payload bytes.
    ReadingPayload(LpmHeader),
}

// ---------------------------------------------------------------------------
// DeframingRecvStream
// ---------------------------------------------------------------------------

/// Wraps an inner `RecvStream` that yields raw `Bytes` chunks and implements
/// `RecvStream` by deframing gRPC Length-Prefixed Messages.
///
/// Each call to `next()` yields one complete deframed message.
pub(crate) struct DeframingRecvStream<R: RecvStream> {
    inner: R,
    buf: DeframeBuf,
    state: DeframeState,
    config: DeframeConfig,
}

impl<R: RecvStream> DeframingRecvStream<R> {
    pub(crate) fn new(inner: R, config: DeframeConfig) -> Self {
        Self {
            inner,
            buf: DeframeBuf::new(),
            state: DeframeState::ReadingHeader,
            config,
        }
    }

    /// Reads one chunk from the inner byte source into the deframe buffer.
    ///
    /// Returns `Ok(true)` if data was read, `Ok(false)` if the stream ended,
    /// or `Err(())` on error.
    async fn read_more(&mut self) -> Result<bool, ()> {
        let mut container = BytesContainer::new();
        match self.inner.next(&mut container).await {
            Some(Ok(())) => {
                let bytes = container.take()?;
                self.buf.push(bytes);
                Ok(true)
            }
            Some(Err(())) => Err(()),
            None => Ok(false),
        }
    }

    /// Attempts to transition from ReadingHeader to ReadingPayload.
    ///
    /// Returns `Some(Err(()))` if the message exceeds max size.
    /// Returns `None` if more data is needed or transition succeeded
    /// (caller should continue the loop).
    fn try_advance_header(&mut self) -> Option<Result<(), ()>> {
        if let Some(header) = self.buf.try_read_header() {
            if header.length > self.config.max_message_size {
                return Some(Err(()));
            }
            self.state = DeframeState::ReadingPayload(header);
        }
        None
    }

    /// Attempts to extract the payload and decode it into `msg`.
    ///
    /// Returns `Some(result)` if payload was extracted and decoded.
    /// Returns `None` if more data is needed.
    fn try_extract_payload(
        &mut self,
        header: LpmHeader,
        msg: &mut dyn RecvMessage,
    ) -> Option<Result<(), ()>> {
        let mut payload = self.buf.take_payload(header.length)?;

        // Set compression flag if the message supports it.
        if let Some(raw) = msg.downcast_mut::<IncomingRawMessage>() {
            raw.set_compressed(header.compressed);
        }

        self.state = DeframeState::ReadingHeader;
        Some(msg.decode(&mut payload).map_err(|_| ()))
    }
}

impl<R: RecvStream> RecvStream for DeframingRecvStream<R> {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
        loop {
            match self.state {
                DeframeState::ReadingHeader => {
                    // Try to parse header from buffered data.
                    if let Some(result) = self.try_advance_header() {
                        return Some(result);
                    }

                    // If we transitioned to ReadingPayload, continue the loop.
                    if matches!(self.state, DeframeState::ReadingPayload(_)) {
                        continue;
                    }

                    // Need more data.
                    match self.read_more().await {
                        Ok(true) => continue,
                        Ok(false) if self.buf.is_empty() => return None, // Clean EOF
                        Ok(false) => return Some(Err(())), // Truncated header
                        Err(()) => return Some(Err(())),
                    }
                }
                DeframeState::ReadingPayload(header) => {
                    // Try to extract payload.
                    if let Some(result) = self.try_extract_payload(header, msg) {
                        return Some(result);
                    }

                    // Need more data.
                    match self.read_more().await {
                        Ok(true) => continue,
                        Ok(false) | Err(()) => return Some(Err(())), // Truncated payload
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LpmFrame (send-side)
// ---------------------------------------------------------------------------

/// A gRPC Length-Prefixed Message frame that implements [`Buf`].
///
/// Contains a 5-byte header (on the stack) followed by the encoded payload.
/// Only the header is allocated; the payload is consumed directly from
/// the `Box<dyn Buf>` returned by `SendMessage::encode()`.
struct LpmFrame {
    header: [u8; LPM_HEADER_SIZE],
    /// How many header bytes have been consumed.
    header_pos: usize,
    payload: Box<dyn Buf + Send + Sync>,
}

impl LpmFrame {
    fn new(compressed: bool, payload: Box<dyn Buf + Send + Sync>) -> Self {
        let len = payload.remaining() as u32;
        let mut header = [0u8; LPM_HEADER_SIZE];
        header[0] = compressed as u8;
        header[1..5].copy_from_slice(&len.to_be_bytes());
        Self {
            header,
            header_pos: 0,
            payload,
        }
    }
}

impl Buf for LpmFrame {
    fn remaining(&self) -> usize {
        (LPM_HEADER_SIZE - self.header_pos) + self.payload.remaining()
    }

    fn chunk(&self) -> &[u8] {
        if self.header_pos < LPM_HEADER_SIZE {
            &self.header[self.header_pos..]
        } else {
            self.payload.chunk()
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        let header_remaining = LPM_HEADER_SIZE - self.header_pos;
        if cnt <= header_remaining {
            self.header_pos += cnt;
            return;
        }
        cnt -= header_remaining;
        self.header_pos = LPM_HEADER_SIZE;
        self.payload.advance(cnt);
    }
}

// ---------------------------------------------------------------------------
// LpmFrameMessage (send-side)
// ---------------------------------------------------------------------------

/// A [`SendMessage`] wrapper that frames the inner message with a 5-byte
/// LPM header. When `encode()` is called, it encodes the inner message
/// and wraps the result in an [`LpmFrame`].
struct LpmFrameMessage<'a> {
    compressed: bool,
    inner: &'a dyn SendMessage,
}

impl SendMessage for LpmFrameMessage<'_> {
    fn encode(&self) -> Result<Box<dyn Buf + Send + Sync>, String> {
        let payload = self.inner.encode()?;
        Ok(Box::new(LpmFrame::new(self.compressed, payload)))
    }
}

// ---------------------------------------------------------------------------
// FramingSendStream (send-side)
// ---------------------------------------------------------------------------

/// Wraps an inner [`SendStream`], adding 5-byte LPM framing to outgoing
/// messages. Headers pass through unchanged.
///
/// Follows the same `&mut` borrow pattern as `CompressedSendStream`.
pub(crate) struct FramingSendStream<'a, S: SendStream> {
    inner: &'a mut S,
}

impl<'a, S: SendStream> FramingSendStream<'a, S> {
    pub(crate) fn new(inner: &'a mut S) -> Self {
        Self { inner }
    }
}

impl<'a, S: SendStream> SendStream for FramingSendStream<'a, S> {
    async fn send<'b>(
        &mut self,
        item: ServerResponseStreamItem<'b>,
        options: SendOptions,
    ) -> Result<(), ()> {
        match item {
            ServerResponseStreamItem::Headers(h) => {
                // Pass through to inner — the transport handles headers.
                self.inner
                    .send(ServerResponseStreamItem::Headers(h), options)
                    .await
            }
            ServerResponseStreamItem::Message(msg) => {
                // Wrap the message with LPM framing.
                // The framing layer always writes compressed=false because
                // it does not perform compression itself. If a compression
                // interceptor is present in the stack, it handles
                // compression and sets the flag on the already-framed data.
                let framed = LpmFrameMessage {
                    compressed: false,
                    inner: msg,
                };
                self.inner
                    .send(ServerResponseStreamItem::Message(&framed), options)
                    .await
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GrpcFramingInterceptor
// ---------------------------------------------------------------------------

/// A server interceptor that adds gRPC Length-Prefixed Message framing
/// to outgoing messages and deframes incoming messages.
///
/// This interceptor should be the outermost in the chain — it bridges
/// raw HTTP byte streams and typed gRPC messages.
#[derive(Debug, Clone)]
pub(crate) struct GrpcFramingInterceptor {
    config: DeframeConfig,
}

impl GrpcFramingInterceptor {
    pub(crate) fn new(config: DeframeConfig) -> Self {
        Self { config }
    }
}

impl Intercept for GrpcFramingInterceptor {
    async fn intercept(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
        tx: &mut impl SendStream,
        rx: impl RecvStream + 'static,
        next: &impl Handle,
    ) -> Trailers {
        let mut framing_tx = FramingSendStream::new(tx);
        let deframing_rx = DeframingRecvStream::new(rx, self.config.clone());
        next.handle(headers, options, &mut framing_tx, deframing_rx).await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    /// Helper: builds a complete LPM frame (header + payload).
    fn make_lpm_frame(compressed: bool, payload: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(LPM_HEADER_SIZE + payload.len());
        buf.put_u8(if compressed { 1 } else { 0 });
        buf.put_u32(payload.len() as u32);
        buf.extend_from_slice(payload);
        buf.freeze()
    }

    // --- DeframeBuf tests ---

    #[test]
    fn deframe_buf_push_skips_empty() {
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::new());
        assert!(buf.is_empty());
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn deframe_buf_push_and_remaining() {
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::from_static(b"hello"));
        assert_eq!(buf.remaining(), 5);
        buf.push(Bytes::from_static(b"world"));
        assert_eq!(buf.remaining(), 10);
    }

    #[test]
    fn deframe_buf_header_not_enough_data() {
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::from_static(b"abcd")); // only 4 bytes
        assert!(buf.try_read_header().is_none());
    }

    #[test]
    fn deframe_buf_header_single_chunk() {
        let mut buf = DeframeBuf::new();
        let frame = make_lpm_frame(false, b"test");
        buf.push(frame);

        let header = buf.try_read_header().expect("should parse header");
        assert!(!header.compressed);
        assert_eq!(header.length, 4);
        assert_eq!(buf.remaining(), 4); // payload remains
    }

    #[test]
    fn deframe_buf_header_compressed() {
        let mut buf = DeframeBuf::new();
        let frame = make_lpm_frame(true, b"data");
        buf.push(frame);

        let header = buf.try_read_header().expect("should parse header");
        assert!(header.compressed);
        assert_eq!(header.length, 4);
    }

    #[test]
    fn deframe_buf_header_spanning_chunks() {
        let mut buf = DeframeBuf::new();
        // Split the header across two chunks: 3 bytes + 2 bytes + payload
        let frame = make_lpm_frame(false, b"hi");
        buf.push(frame.slice(0..3));
        buf.push(frame.slice(3..));

        let header = buf.try_read_header().expect("should parse spanning header");
        assert!(!header.compressed);
        assert_eq!(header.length, 2);
        assert_eq!(buf.remaining(), 2); // payload remains
    }

    #[test]
    fn deframe_buf_payload_single_chunk() {
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::from_static(b"hello"));

        let payload = buf.take_payload(5).expect("should extract payload");
        assert!(matches!(payload, ExtractedPayload::Single(_)));
        assert_eq!(payload.remaining(), 5);
        assert!(buf.is_empty());
    }

    #[test]
    fn deframe_buf_payload_multi_chunk() {
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::from_static(b"hel"));
        buf.push(Bytes::from_static(b"lo"));

        let payload = buf.take_payload(5).expect("should extract payload");
        assert!(matches!(payload, ExtractedPayload::Multi(_)));
        assert_eq!(payload.remaining(), 5);
        assert!(buf.is_empty());
    }

    #[test]
    fn deframe_buf_payload_partial_chunk() {
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::from_static(b"hello world"));

        let payload = buf.take_payload(5).expect("should extract 5 bytes");
        assert!(matches!(payload, ExtractedPayload::Single(_)));
        assert_eq!(payload.remaining(), 5);
        assert_eq!(buf.remaining(), 6); // " world" remains
    }

    #[test]
    fn deframe_buf_payload_not_enough() {
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::from_static(b"hi"));
        assert!(buf.take_payload(5).is_none());
    }

    #[test]
    fn deframe_buf_multiple_messages_one_chunk() {
        let mut buf = DeframeBuf::new();
        // Two messages in one chunk
        let mut data = BytesMut::new();
        data.extend_from_slice(&make_lpm_frame(false, b"msg1"));
        data.extend_from_slice(&make_lpm_frame(true, b"msg2!!"));
        buf.push(data.freeze());

        // First message
        let h1 = buf.try_read_header().expect("header 1");
        assert!(!h1.compressed);
        assert_eq!(h1.length, 4);
        let p1 = buf.take_payload(h1.length).expect("payload 1");
        assert_eq!(p1.remaining(), 4);

        // Second message
        let h2 = buf.try_read_header().expect("header 2");
        assert!(h2.compressed);
        assert_eq!(h2.length, 6);
        let p2 = buf.take_payload(h2.length).expect("payload 2");
        assert_eq!(p2.remaining(), 6);

        assert!(buf.is_empty());
    }

    // --- BytesContainer tests ---

    #[test]
    fn bytes_container_captures_data() {
        let mut container = BytesContainer::new();
        let mut data = Bytes::from_static(b"test data");
        container.decode(&mut data).expect("decode should succeed");
        let captured = container.take().expect("should have data");
        assert_eq!(&captured[..], b"test data");
    }

    #[test]
    fn bytes_container_take_empty_is_error() {
        let mut container = BytesContainer::new();
        assert!(container.take().is_err());
    }

    // --- ExtractedPayload Buf tests ---

    #[test]
    fn extracted_payload_single_buf() {
        let mut payload = ExtractedPayload::Single(Bytes::from_static(b"hello"));
        assert_eq!(payload.remaining(), 5);
        assert_eq!(payload.chunk(), b"hello");
        payload.advance(3);
        assert_eq!(payload.remaining(), 2);
        assert_eq!(payload.chunk(), b"lo");
    }

    #[test]
    fn extracted_payload_multi_buf() {
        let mut chunks = VecDeque::new();
        chunks.push_back(Bytes::from_static(b"hel"));
        chunks.push_back(Bytes::from_static(b"lo"));
        let mut payload = ExtractedPayload::Multi(ChunkedBuf { chunks });
        assert_eq!(payload.remaining(), 5);
        assert_eq!(payload.chunk(), b"hel"); // first chunk
        payload.advance(3);
        assert_eq!(payload.remaining(), 2);
        assert_eq!(payload.chunk(), b"lo"); // second chunk
    }

    // --- LpmFrame tests ---

    #[test]
    fn lpm_frame_uncompressed() {
        let payload = Bytes::from_static(b"test");
        let mut frame = LpmFrame::new(false, Box::new(payload));

        // Total: 5 (header) + 4 (payload) = 9
        assert_eq!(frame.remaining(), 9);

        // First chunk is the header
        let chunk = frame.chunk();
        assert_eq!(chunk.len(), 5);
        assert_eq!(chunk[0], 0); // not compressed
        assert_eq!(u32::from_be_bytes([chunk[1], chunk[2], chunk[3], chunk[4]]), 4);

        // Advance past header
        frame.advance(5);
        assert_eq!(frame.remaining(), 4);
        assert_eq!(frame.chunk(), b"test");

        // Advance past payload
        frame.advance(4);
        assert_eq!(frame.remaining(), 0);
    }

    #[test]
    fn lpm_frame_compressed() {
        let payload = Bytes::from_static(b"data");
        let frame = LpmFrame::new(true, Box::new(payload));

        assert_eq!(frame.remaining(), 9);
        assert_eq!(frame.chunk()[0], 1); // compressed
    }

    #[test]
    fn lpm_frame_partial_advance_in_header() {
        let payload = Bytes::from_static(b"ab");
        let mut frame = LpmFrame::new(false, Box::new(payload));

        // Advance 3 bytes into header
        frame.advance(3);
        assert_eq!(frame.remaining(), 4); // 2 header + 2 payload
        assert_eq!(frame.chunk().len(), 2); // remaining header bytes

        // Advance past rest of header
        frame.advance(2);
        assert_eq!(frame.remaining(), 2);
        assert_eq!(frame.chunk(), b"ab");
    }

    #[test]
    fn lpm_frame_empty_payload() {
        let payload = Bytes::new();
        let mut frame = LpmFrame::new(false, Box::new(payload));

        assert_eq!(frame.remaining(), 5); // header only
        let chunk = frame.chunk();
        assert_eq!(u32::from_be_bytes([chunk[1], chunk[2], chunk[3], chunk[4]]), 0);

        frame.advance(5);
        assert_eq!(frame.remaining(), 0);
    }

    // --- LpmFrameMessage tests ---

    /// A simple SendMessage for testing.
    struct TestMessage {
        data: Bytes,
    }

    impl SendMessage for TestMessage {
        fn encode(&self) -> Result<Box<dyn Buf + Send + Sync>, String> {
            Ok(Box::new(self.data.clone()))
        }
    }

    #[test]
    fn lpm_frame_message_encode() {
        let msg = TestMessage {
            data: Bytes::from_static(b"hello"),
        };
        let framed = LpmFrameMessage {
            compressed: false,
            inner: &msg,
        };

        let mut buf = framed.encode().expect("encode should succeed");
        assert_eq!(buf.remaining(), 10); // 5 header + 5 payload

        // Read header
        let mut hdr = [0u8; 5];
        buf.copy_to_slice(&mut hdr);
        assert_eq!(hdr[0], 0); // not compressed
        assert_eq!(u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]), 5);

        // Read payload
        let payload = buf.copy_to_bytes(buf.remaining());
        assert_eq!(&payload[..], b"hello");
    }

    #[test]
    fn lpm_frame_message_encode_compressed() {
        let msg = TestMessage {
            data: Bytes::from_static(b"data"),
        };
        let framed = LpmFrameMessage {
            compressed: true,
            inner: &msg,
        };

        let buf = framed.encode().expect("encode should succeed");
        assert_eq!(buf.remaining(), 9); // 5 header + 4 payload
        assert_eq!(buf.chunk()[0], 1); // compressed flag
    }

    // --- Bug fix tests ---

    #[test]
    fn deframe_buf_take_payload_zero_length() {
        // Bug fix: take_payload(0) should return an empty Bytes, not None.
        // This is needed for 0-byte protobuf messages like Empty.
        let mut buf = DeframeBuf::new();
        // Don't push any data — chunks is empty.
        let payload = buf.take_payload(0);
        assert!(payload.is_some(), "take_payload(0) should return Some");
        assert_eq!(payload.unwrap().remaining(), 0);
    }

    #[test]
    fn deframe_buf_take_payload_zero_length_with_data() {
        // take_payload(0) should work even when there's buffered data.
        let mut buf = DeframeBuf::new();
        buf.push(Bytes::from_static(b"leftover"));
        let payload = buf.take_payload(0);
        assert!(payload.is_some(), "take_payload(0) should return Some");
        assert_eq!(payload.unwrap().remaining(), 0);
        // The buffered data should be untouched.
        assert_eq!(buf.remaining(), 8);
    }

    #[test]
    fn framing_send_stream_always_writes_uncompressed() {
        // Bug fix: FramingSendStream should always write compressed=false
        // in the LPM header because it does not perform compression itself.

        // Create a mock inner SendStream that captures what was sent.
        struct CaptureSendStream {
            captured: std::cell::RefCell<Vec<Bytes>>,
        }
        impl SendStream for CaptureSendStream {
            async fn send<'a>(
                &mut self,
                item: ServerResponseStreamItem<'a>,
                _options: SendOptions,
            ) -> Result<(), ()> {
                if let ServerResponseStreamItem::Message(msg) = item {
                    let mut encoded = msg.encode().map_err(|_| ())?;
                    let bytes = encoded.copy_to_bytes(encoded.remaining());
                    self.captured.borrow_mut().push(bytes);
                }
                Ok(())
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut inner = CaptureSendStream {
                captured: std::cell::RefCell::new(Vec::new()),
            };
            let mut framing = FramingSendStream::new(&mut inner);

            let msg = TestMessage {
                data: Bytes::from_static(b"hello"),
            };

            // Send with default options (disable_compression=false).
            framing
                .send(
                    ServerResponseStreamItem::Message(&msg),
                    SendOptions::default(),
                )
                .await
                .expect("send should succeed");

            let captured = inner.captured.borrow();
            assert_eq!(captured.len(), 1);
            // First byte of LPM frame is the compressed flag.
            assert_eq!(captured[0][0], 0, "compressed flag should be 0 (false)");
        });
    }

    #[tokio::test]
    async fn deframing_recv_stream_handles_empty_message() {
        // Bug fix: DeframingRecvStream should successfully deframe a
        // 0-byte LPM message (e.g., protobuf Empty).
        use crate::core::RecvMessage;

        // Build a raw LPM frame with 0-length payload: [0x00, 0x00, 0x00, 0x00, 0x00]
        let frame = make_lpm_frame(false, &[]);

        // Create a mock RecvStream that yields the frame bytes.
        struct SingleChunkRecvStream {
            data: Option<Bytes>,
        }
        impl RecvStream for SingleChunkRecvStream {
            async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
                match self.data.take() {
                    Some(mut bytes) => Some(msg.decode(&mut bytes).map_err(|_| ())),
                    None => None,
                }
            }
        }

        let inner = SingleChunkRecvStream {
            data: Some(frame),
        };
        let mut deframing = DeframingRecvStream::new(inner, DeframeConfig::default());

        // Should successfully deframe the empty message.
        let mut container = BytesContainer::new();
        let result = deframing.next(&mut container).await;
        assert!(
            matches!(result, Some(Ok(()))),
            "expected Some(Ok(())), got {:?}",
            result
        );

        // The decoded payload should be empty.
        let captured = container.take().expect("should have data");
        assert_eq!(captured.len(), 0, "empty message payload should be 0 bytes");

        // Stream should be exhausted.
        let mut container2 = BytesContainer::new();
        let result2 = deframing.next(&mut container2).await;
        assert!(result2.is_none(), "stream should be exhausted");
    }
}
