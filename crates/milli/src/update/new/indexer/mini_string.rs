const MAX_LENGTH: usize = 12;

/// A string up to 12 bytes in length, stored inline.
#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MiniString {
    length: u8,
    data: [u8; MAX_LENGTH],
}

impl MiniString {
    pub fn new(s: &str) -> Option<MiniString> {
        if s.len() > MAX_LENGTH {
            None
        } else {
            let mut data: [u8; _] = Default::default();
            data.copy_from_slice(s.as_bytes());
            Some(MiniString { length: s.len() as u8, data })
        }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..self.length as usize]
    }
}

impl AsRef<[u8]> for MiniString {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<str> for MiniString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::hash::Hash for MiniString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}
