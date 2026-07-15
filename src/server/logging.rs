use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const MAX_RING_BYTES: usize = 1_000_000;

#[derive(Clone)]
pub struct RingWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl RingWriter {
    pub fn new() -> Self {
        RingWriter {
            buf: Arc::new(Mutex::new(Vec::with_capacity(MAX_RING_BYTES))),
        }
    }

    pub fn read_since(&self, offset: usize) -> Vec<u8> {
        let buf = self.buf.lock().unwrap();
        if offset >= buf.len() {
            Vec::new()
        } else {
            buf[offset..].to_vec()
        }
    }
}

impl Write for RingWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        if buf.len() > MAX_RING_BYTES {
            let trim = buf.len() - MAX_RING_BYTES;
            let cut = buf[trim..]
                .iter()
                .position(|&b| b == b'\n')
                .map(|p| trim + p + 1)
                .unwrap_or(buf.len());
            buf.drain(..cut);
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RingWriter {
    type Writer = RingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

pub fn init_logging() -> RingWriter {
    let ring = RingWriter::new();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_filter(env_filter);

    let ring_layer = tracing_subscriber::fmt::layer()
        .with_writer(ring.clone())
        .with_ansi(false)
        .with_target(false)
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(ring_layer)
        .init();

    tracing::info!("logging initialized (in-memory ring buffer)");
    ring
}
