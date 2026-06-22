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

use std::borrow::Cow;

/// The type (cardinality) of a gRPC method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MethodType {
    /// One request message followed by one response message.
    Unary,
    /// Zero or more request messages with one response message.
    ClientStreaming,
    /// One request message followed by zero or more response messages.
    ServerStreaming,
    /// Zero or more request and response messages arbitrarily interleaved.
    BidiStreaming,
}

/// Pure metadata about a single gRPC method.
///
/// This is a data class — it carries no handler logic. It describes what a
/// method looks like (its path and cardinality) without specifying how it's
/// implemented.
///
/// Uses `Cow<'static, str>` for zero-allocation in codegen (the common case)
/// while supporting dynamic construction for hand-built services.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MethodDescriptor {
    /// Full method path, e.g., `"/helloworld.Greeter/SayHello"`.
    pub full_path: Cow<'static, str>,
    /// The method cardinality.
    pub method_type: MethodType,
}

impl MethodDescriptor {
    /// Creates a descriptor from a static path (codegen, zero-allocation).
    pub const fn new_static(full_path: &'static str, method_type: MethodType) -> Self {
        Self {
            full_path: Cow::Borrowed(full_path),
            method_type,
        }
    }

    /// Creates a descriptor from a dynamic path (hand-built services).
    pub fn new(full_path: impl Into<String>, method_type: MethodType) -> Self {
        Self {
            full_path: Cow::Owned(full_path.into()),
            method_type,
        }
    }
}

/// Pure metadata about a gRPC service.
///
/// This is a data class — it carries no handler logic. It describes what a
/// service looks like (its name and the methods it contains) without
/// specifying how they're implemented.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ServiceDescriptor {
    /// Fully qualified service name, e.g., `"helloworld.Greeter"`.
    pub name: Cow<'static, str>,
    /// Descriptors for all methods in this service.
    pub methods: Cow<'static, [MethodDescriptor]>,
}

impl ServiceDescriptor {
    /// Creates a descriptor from static data (codegen, zero-allocation).
    pub const fn new_static(
        name: &'static str,
        methods: &'static [MethodDescriptor],
    ) -> Self {
        Self {
            name: Cow::Borrowed(name),
            methods: Cow::Borrowed(methods),
        }
    }

    /// Creates a descriptor from dynamic data (hand-built services).
    pub fn new(name: impl Into<String>, methods: Vec<MethodDescriptor>) -> Self {
        Self {
            name: Cow::Owned(name.into()),
            methods: Cow::Owned(methods),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_type_variants() {
        assert_eq!(MethodType::Unary, MethodType::Unary);
        assert_ne!(MethodType::Unary, MethodType::ClientStreaming);
        assert_ne!(MethodType::ServerStreaming, MethodType::BidiStreaming);
    }

    #[test]
    fn method_descriptor_static_is_borrowed() {
        let desc = MethodDescriptor::new_static("/pkg.Svc/Method", MethodType::Unary);
        assert!(matches!(desc.full_path, Cow::Borrowed(_)));
        assert_eq!(desc.full_path, "/pkg.Svc/Method");
        assert_eq!(desc.method_type, MethodType::Unary);
    }

    #[test]
    fn method_descriptor_dynamic_is_owned() {
        let desc = MethodDescriptor::new("/pkg.Svc/Method".to_string(), MethodType::ServerStreaming);
        assert!(matches!(desc.full_path, Cow::Owned(_)));
        assert_eq!(desc.full_path, "/pkg.Svc/Method");
        assert_eq!(desc.method_type, MethodType::ServerStreaming);
    }

    #[test]
    fn method_descriptor_new_accepts_str() {
        let desc = MethodDescriptor::new("/pkg.Svc/Method", MethodType::Unary);
        assert_eq!(desc.full_path, "/pkg.Svc/Method");
    }

    #[test]
    fn method_descriptor_clone() {
        let desc = MethodDescriptor::new_static("/pkg.Svc/Method", MethodType::BidiStreaming);
        let cloned = desc.clone();
        assert_eq!(cloned.full_path, desc.full_path);
        assert_eq!(cloned.method_type, desc.method_type);
    }

    #[test]
    fn service_descriptor_static_is_borrowed() {
        static METHODS: &[MethodDescriptor] = &[
            MethodDescriptor::new_static("/pkg.Svc/M1", MethodType::Unary),
            MethodDescriptor::new_static("/pkg.Svc/M2", MethodType::ServerStreaming),
        ];
        let desc = ServiceDescriptor::new_static("pkg.Svc", METHODS);
        assert!(matches!(desc.name, Cow::Borrowed(_)));
        assert!(matches!(desc.methods, Cow::Borrowed(_)));
        assert_eq!(desc.name, "pkg.Svc");
        assert_eq!(desc.methods.len(), 2);
    }

    #[test]
    fn service_descriptor_dynamic_is_owned() {
        let desc = ServiceDescriptor::new(
            "pkg.Svc",
            vec![
                MethodDescriptor::new("/pkg.Svc/M1", MethodType::Unary),
            ],
        );
        assert!(matches!(desc.name, Cow::Owned(_)));
        assert!(matches!(desc.methods, Cow::Owned(_)));
        assert_eq!(desc.name, "pkg.Svc");
        assert_eq!(desc.methods.len(), 1);
    }

    #[test]
    fn service_descriptor_empty_methods() {
        let desc = ServiceDescriptor::new("pkg.Empty", vec![]);
        assert_eq!(desc.methods.len(), 0);
    }

    #[test]
    fn service_descriptor_clone() {
        let desc = ServiceDescriptor::new(
            "pkg.Svc",
            vec![MethodDescriptor::new("/pkg.Svc/M1", MethodType::Unary)],
        );
        let cloned = desc.clone();
        assert_eq!(cloned.name, desc.name);
        assert_eq!(cloned.methods.len(), desc.methods.len());
    }
}
