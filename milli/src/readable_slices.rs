use std::io::{self, Read};
use std::iter::FromIterator;

pub struct ReadableSlices<A> {
    inner: Vec<A>,
    pos: u64,
}

impl<A> FromIterator<A> for ReadableSlices<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        ReadableSlices { inner: iter.into_iter().collect(), pos: 0 }
    }
}

impl<A: AsRef<[u8]>> Read for ReadableSlices<A> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let original_buf_len = buf.len();

        // We explore the list of slices to find the one where we must start reading.
        let mut pos = self.pos;
        let index = match self
            .inner
            .iter()
            .map(|s| s.as_ref().len() as u64)
            .position(|size| pos.checked_sub(size).map(|p| pos = p).is_none())
        {
            Some(index) => index,
            None => return Ok(0),
        };

        let mut inner_pos = pos as usize;
        for slice in &self.inner[index..] {
            let slice = &slice.as_ref()[inner_pos..];

            if buf.len() > slice.len() {
                // We must exhaust the current slice and go to the next one there is not enough here.
                buf[..slice.len()].copy_from_slice(slice);
                buf = &mut buf[slice.len()..];
                inner_pos = 0;
            } else {
                // There is enough in this slice to fill the remaining bytes of the buffer.
                // Let's break just after filling it.
                buf.copy_from_slice(&slice[..buf.len()]);
                buf = &mut [];
                break;
            }
        }

        let written = original_buf_len - buf.len();
        self.pos += written as u64;
        Ok(written)
    }
}

#[cfg(test)]
mod test {
    use std::io::Read;

    use super::ReadableSlices;

    #[test]
    fn basic() {
        let data: Vec<_> = (0..100).collect();
        let splits: Vec<_> = data.chunks(3).collect();
        let mut rdslices: ReadableSlices<_> = splits.into_iter().collect();

        let mut output = Vec::new();
        let length = rdslices.read_to_end(&mut output).unwrap();
        assert_eq!(length, data.len());
        assert_eq!(output, data);
    }

    #[test]
    fn small_reads() {
        let data: Vec<_> = (0..u8::MAX).collect();
        let splits: Vec<_> = data.chunks(27).collect();
        let mut rdslices: ReadableSlices<_> = splits.into_iter().collect();

        let buffer = &mut [0; 45];
        let length = rdslices.read(buffer).unwrap();
        let expected: Vec<_> = (0..buffer.len() as u8).collect();
        assert_eq!(length, buffer.len());
        assert_eq!(buffer, &expected[..]);
    }
}
