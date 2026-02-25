use bytes::{Buf, Bytes};
use std::collections::VecDeque;

/// A `Buf` that chains a 5-byte gRPC header with a body.
///
/// This allows us to yield a single `Buf` to `hyper` that contains both the
/// header (compression flag + length) and the message payload, avoiding
/// memory allocation/copying for the header.
#[derive(Debug)]
pub struct GrpcLengthEncodedBuf<B> {
    header: [u8; 5],
    header_pos: usize,
    body: B,
}

impl<B: Buf> GrpcLengthEncodedBuf<B> {
    /// Creates a new `GrpcLengthEncodedBuf`.
    pub fn new(header: [u8; 5], body: B) -> Self {
        Self {
            header,
            header_pos: 0,
            body,
        }
    }
}

impl<B: Buf> Buf for GrpcLengthEncodedBuf<B> {
    fn remaining(&self) -> usize {
        (5 - self.header_pos) + self.body.remaining()
    }

    fn chunk(&self) -> &[u8] {
        if self.header_pos < 5 {
            &self.header[self.header_pos..]
        } else {
            self.body.chunk()
        }
    }

    fn advance(&mut self, cnt: usize) {
        let header_remaining = 5 - self.header_pos;
        if cnt < header_remaining {
            self.header_pos += cnt;
        } else {
            self.header_pos = 5;
            self.body.advance(cnt - header_remaining);
        }
    }
}

/// A buffer that can be non-contiguous.
///
/// `BufList` allows collecting multiple `Buf` chunks and treating them as a single buffer.
/// It supports efficient appending (`push`), peeking without advancing (`peek_copy_to`),
/// and zero-copy splitting (`split_to` for `Bytes`).
///
/// # Examples
///
/// ```
/// use grpc::transport::hyper::buffer::BufList;
/// use bytes::{Bytes, Buf};
///
/// let mut buf = BufList::empty();
/// buf.push(Bytes::from("hello"));
/// buf.push(Bytes::from(" world"));
///
/// assert_eq!(buf.remaining(), 11);
///
/// let mut dst = [0u8; 5];
/// buf.peek_copy_to(&mut &mut dst[..]);
/// assert_eq!(&dst, b"hello");
///
/// let hello = buf.split_to(5).unwrap();
/// assert_eq!(hello.remaining(), 5);
/// assert_eq!(buf.remaining(), 6);
/// ```
#[derive(Debug, Clone)]
pub struct BufList<B> {
    chunks: VecDeque<B>,
    remaining: usize,
}

impl<B> BufList<B> {
    /// Creates a new `BufList` from a list of chunks.
    pub fn new(chunks: VecDeque<B>) -> Self
    where
        B: Buf,
    {
        let remaining = chunks.iter().map(|buf| buf.remaining()).sum();
        Self { chunks, remaining }
    }

    /// Creates an empty `BufList`.
    pub fn empty() -> Self {
        Self {
            chunks: VecDeque::new(),
            remaining: 0,
        }
    }

    /// Pushes a chunk to the end of the buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use grpc::transport::hyper::buffer::BufList;
    /// use bytes::{Bytes, Buf};
    ///
    /// let mut buf = BufList::<Bytes>::empty();
    /// buf.push(Bytes::from("data"));
    /// assert_eq!(buf.remaining(), 4);
    /// ```
    pub fn push(&mut self, chunk: B)
    where
        B: Buf,
    {
        if chunk.has_remaining() {
            self.remaining += chunk.remaining();
            self.chunks.push_back(chunk);
        }
    }

    /// Peeks at the buffer and copies bytes into `dst`.
    ///
    /// This does NOT advance the buffer. It copies bytes until `dst` has no remaining space
    /// or the buffer runs out of data.
    ///
    /// # Examples
    ///
    /// ```
    /// use grpc::transport::hyper::buffer::BufList;
    /// use bytes::{Bytes, Buf};
    ///
    /// let mut buf = BufList::empty();
    /// buf.push(Bytes::from("hello"));
    ///
    /// let mut dst = [0u8; 3];
    /// buf.peek_copy_to(&mut &mut dst[..]);
    /// assert_eq!(&dst, b"hel");
    /// assert_eq!(buf.remaining(), 5); // Buffer not advanced
    /// ```
    pub fn peek_copy_to<D>(&self, dst: &mut D)
    where
        B: Buf,
        D: bytes::BufMut,
    {
        let mut chunks_iter = self.chunks.iter();
        while dst.has_remaining_mut() {
            let chunk_ref = match chunks_iter.next() {
                Some(c) => c,
                None => break,
            };
            let chunk = chunk_ref.chunk();
            let to_copy = std::cmp::min(dst.remaining_mut(), chunk.len());
            dst.put_slice(&chunk[..to_copy]);
        }
    }
}

impl BufList<Bytes> {
    /// Splits the buffer into two at the given index.
    ///
    /// Returns a new `BufList` containing the first `at` bytes, and advances
    /// this buffer by `at` bytes.
    ///
    /// This is a zero-copy operation for `Bytes` (uses reference counting).
    ///
    /// # Examples
    ///
    /// ```
    /// use grpc::transport::hyper::buffer::BufList;
    /// use bytes::{Bytes, Buf};
    ///
    /// let mut buf = BufList::empty();
    /// buf.push(Bytes::from("hello world"));
    ///
    /// let hello = buf.split_to(5).unwrap();
    /// assert_eq!(hello.remaining(), 5);
    /// assert_eq!(buf.remaining(), 6);
    /// ```
    pub fn split_to(&mut self, mut at: usize) -> Result<Self, std::io::Error> {
        if at > self.remaining {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }

        let mut split_chunks = VecDeque::new();
        self.remaining -= at;

        while at > 0 {
            let mut front = self.chunks.pop_front().expect("chunks should not be empty");
            if front.len() <= at {
                at -= front.len();
                split_chunks.push_back(front);
            } else {
                let split_part = front.split_to(at);
                split_chunks.push_back(split_part);
                self.chunks.push_front(front);
                at = 0;
            }
        }

        Ok(Self::new(split_chunks))
    }
}

impl<B: Buf> Default for BufList<B> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<B: Buf> Buf for BufList<B> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.chunks.front().map_or(&[], |buf| buf.chunk())
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(
            cnt <= self.remaining,
            "cannot advance past remaining length"
        );
        self.remaining -= cnt;

        while cnt > 0 {
            let front = self.chunks.front_mut().expect("chunks should not be empty");
            let front_rem = front.remaining();

            if front_rem > cnt {
                front.advance(cnt);
                cnt = 0;
            } else {
                cnt -= front_rem;
                self.chunks.pop_front();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_push_and_len() {
        let mut buf = BufList::<Bytes>::empty();
        assert!(!buf.has_remaining());

        buf.push(Bytes::from("hello"));
        assert_eq!(buf.remaining(), 5);

        buf.push(Bytes::from(" world"));
        assert_eq!(buf.remaining(), 11);
    }

    #[test]
    fn test_peek_copy_to() {
        let mut buf = BufList::empty();
        buf.push(Bytes::from("hello"));
        buf.push(Bytes::from(" world"));

        let mut dst = [0u8; 5];
        buf.peek_copy_to(&mut &mut dst[..]);
        assert_eq!(&dst, b"hello");
        assert_eq!(buf.remaining(), 11);

        let mut dst = [0u8; 11];
        buf.peek_copy_to(&mut &mut dst[..]);
        assert_eq!(&dst, b"hello world");
        assert_eq!(buf.remaining(), 11);
    }

    #[test]
    fn test_peek_copy_to_partial() {
        let mut buf = BufList::empty();
        buf.push(Bytes::from("hi"));

        // Destination is larger than buffer
        let mut dst = [0u8; 5];
        let mut dst_slice = &mut dst[..];
        buf.peek_copy_to(&mut dst_slice);

        // Should have copied "hi" and left the rest as 0
        assert_eq!(&dst[..2], b"hi");
        assert_eq!(&dst[2..], &[0, 0, 0]);
        // Buffer should remain unchanged
        assert_eq!(buf.remaining(), 2);
    }

    #[test]
    fn test_split_to() {
        let mut buf = BufList::empty();
        buf.push(Bytes::from("hello"));
        buf.push(Bytes::from(" world"));

        let mut part1 = buf.split_to(5).unwrap();
        assert_eq!(part1.remaining(), 5);
        assert_eq!(buf.remaining(), 6);

        let mut dst = [0u8; 5];
        use bytes::Buf; // Ensure Buf trait is in scope for copy_to_slice
        part1.copy_to_slice(&mut dst);
        assert_eq!(&dst, b"hello");

        let mut part2 = buf.split_to(6).unwrap();
        assert_eq!(part2.remaining(), 6);
        assert_eq!(buf.remaining(), 0);

        let mut dst = [0u8; 6];
        part2.copy_to_slice(&mut dst);
        assert_eq!(&dst, b" world");
    }

    #[test]
    fn test_split_to_out_of_bounds() {
        let mut buf = BufList::empty();
        buf.push(Bytes::from("hello"));

        let err = buf.split_to(6).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(buf.remaining(), 5);
    }

    #[test]
    fn test_advance() {
        let mut buf = BufList::empty();
        buf.push(Bytes::from("hello"));
        buf.push(Bytes::from(" world"));

        buf.advance(6);
        assert_eq!(buf.remaining(), 5);

        let mut dst = [0u8; 5];
        use bytes::Buf; // Ensure Buf trait is in scope for copy_to_slice
        buf.copy_to_slice(&mut dst);
        assert_eq!(&dst, b"world");
    }

    #[test]
    fn test_grpc_length_encoded_buf() {
        let header = [1, 2, 3, 4, 5];
        let body = &b"hello"[..];
        let mut buf = GrpcLengthEncodedBuf::new(header, body);

        assert_eq!(buf.remaining(), 10);
        assert_eq!(buf.chunk(), &[1, 2, 3, 4, 5]);

        buf.advance(2);
        assert_eq!(buf.remaining(), 8);
        assert_eq!(buf.chunk(), &[3, 4, 5]);

        buf.advance(3);
        assert_eq!(buf.remaining(), 5);
        assert_eq!(buf.chunk(), b"hello");

        buf.advance(2);
        assert_eq!(buf.remaining(), 3);
        assert_eq!(buf.chunk(), b"llo");

        buf.advance(3);
        assert_eq!(buf.remaining(), 0);
    }
}
