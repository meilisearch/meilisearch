use std::io;

#[derive(Debug, Default, Clone, Copy)]
pub struct BytesCounter {
    bytes_written: usize,
}

impl BytesCounter {
    pub fn new() -> Self {
        BytesCounter { bytes_written: 0 }
    }

    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

impl io::Write for BytesCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes_written += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
