use bytes::BufMut;

/// A trait for serializing messages.
pub trait Serialize {
    /// Encodes the message into a growable, abstract buffer.
    fn serialize<B: BufMut>(&self, buf: &mut B) -> Result<(), crate::Status>;
}
