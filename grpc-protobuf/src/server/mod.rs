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

//! Server-side integration for the [`grpc`] crate (protobuf).
//!
//! These types bridge protobuf-typed service methods to the `grpc` server
//! runtime.  They are generally used by generated code produced by
//! [`protoc-gen-rust-grpc`](https://docs.rs/protoc-gen-rust-grpc): the generated
//! `*Server<T>` type implements [`grpc::server_internal::Service`] and registers
//! one adapter (e.g. [`UnaryAdapter`]) per method.

mod bidi;
mod client_streaming;
mod server_streaming;
mod stream;
mod unary;

#[cfg(test)]
mod streaming_tests;

pub use bidi::BidiStreamingAdapter;
pub use bidi::BidiStreamingMethod;
pub use client_streaming::ClientStreamingAdapter;
pub use client_streaming::ClientStreamingMethod;
pub use server_streaming::ServerStreamingAdapter;
pub use server_streaming::ServerStreamingMethod;
pub use stream::GrpcStreamingRequest;
pub use stream::GrpcStreamingResponse;
pub use unary::UnaryAdapter;
pub use unary::UnaryMethod;
