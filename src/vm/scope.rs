use std::collections::HashMap;

use flexbuffers::{Blob, Builder, Reader};

/// FlexBuffer-backed runtime scope. Stores all step outputs as opaque bytes.
/// Clone = clone the underlying FlexBuffer bytes.
#[derive(Debug, Clone)]
pub struct Scope {
    buf: Vec<u8>,
}

impl Scope {
    /// Create empty scope with given slots bytes.
    pub fn new(slots: &[u8]) -> Self {
        let mut b = Builder::default();
        let mut map = b.start_map();
        map.push("slots", Blob(slots));
        map.end_map();
        Self { buf: b.take_buffer() }
    }

    /// Get step output as raw bytes. Callers are responsible for parsing
    /// if they need field-level navigation (outputs may not be JSON).
    pub fn get_output(&self, step_id: &str) -> Option<Vec<u8>> {
        let r = Reader::get_root(self.buf.as_slice()).ok()?;
        let map = r.as_map();
        let val = map.index(step_id).ok()?;
        let blob = val.get_blob().ok()?;
        Some(blob.0.to_vec())
    }

    /// Return all non-slot entries in the scope.
    pub fn all_outputs(&self) -> HashMap<String, Vec<u8>> {
        let mut result = HashMap::new();
        let r = match Reader::get_root(self.buf.as_slice()) {
            Ok(r) => r,
            Err(_) => return result,
        };
        let m = r.as_map();
        for (k, v) in m.iter_keys().zip(m.iter_values()) {
            if k == "slots" {
                continue;
            }
            if let Ok(blob) = v.get_blob() {
                result.insert(k.to_string(), blob.0.to_vec());
            }
        }
        result
    }

    /// Get slots bytes. Returns owned bytes.
    pub fn slots(&self) -> Option<Vec<u8>> {
        let r = Reader::get_root(self.buf.as_slice()).ok()?;
        let map = r.as_map();
        let val = map.index("slots").ok()?;
        let blob = val.get_blob().ok()?;
        Some(blob.0.to_vec())
    }

    /// Set or update a step output. Rebuilds the FlexBuffer.
    pub fn set_output(&mut self, step_id: &str, data: &[u8]) {
        let mut b = Builder::default();
        let mut map = b.start_map();

        // Copy existing entries (except the one being overwritten and slots)
        if let Ok(r) = Reader::get_root(self.buf.as_slice()) {
            let m = r.as_map();
            for (k, v) in m.iter_keys().zip(m.iter_values()) {
                if k == step_id || k == "slots" {
                    continue;
                }
                if let Ok(blob) = v.get_blob() {
                    map.push(k, blob);
                }
            }
        }

        // Add/update the entry
        map.push(step_id, Blob(data));

        // Preserve slots
        if let Ok(r) = Reader::get_root(self.buf.as_slice()) {
            let m = r.as_map();
            if let Ok(sv) = m.index("slots")
                && let Ok(slots_blob) = sv.get_blob() {
                    map.push("slots", slots_blob);
                }
        }

        map.end_map();
        self.buf = b.take_buffer();
    }

    /// Access raw bytes for snapshot.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Consume self and return raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}
