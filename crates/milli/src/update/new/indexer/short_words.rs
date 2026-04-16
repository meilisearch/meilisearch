use std::num::TryFromIntError;

#[derive(Default)]
pub struct ShortWords {
    // TODO use a linked list of increasing boxes
    data: String,
    // TODO use a linked of increasing boxes
    indices: Vec<ShortWordIndex>,
}

impl ShortWords {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, s: &str) -> Result<(), TryFromIntError> {
        let index = self.data.len();
        let length = s.len();
        let swindex = ShortWordIndex::new(index, length)?;
        self.data.push_str(s);
        self.indices.push(swindex);
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ str> + '_ {
        self.indices.iter().map(|iprefix| {
            let index = iprefix.index();
            let length = iprefix.length();
            &self.data[index..index + length]
        })
    }
}

struct ShortWordIndex(u32);

impl ShortWordIndex {
    const NUM_BITS_INDEX: u32 = 27; // max: 134 217 728
    const NUM_BITS_LENGTH: u32 = 5; // max: 32

    pub fn new(index: usize, length: usize) -> Result<Self, TryFromIntError> {
        let index: u32 = index.try_into()?;
        let length: u32 = length.try_into()?;

        if index > (1 << Self::NUM_BITS_INDEX) {
            // An ugly way to create this kind of error
            return Err(TryInto::<u32>::try_into(usize::MAX).unwrap_err());
        }

        if length > (1 << Self::NUM_BITS_LENGTH) {
            // An ugly way to create this kind of error
            return Err(TryInto::<u32>::try_into(usize::MAX).unwrap_err());
        }

        Ok(Self(index | (length << Self::NUM_BITS_INDEX)))
    }

    fn index(&self) -> usize {
        (self.0 & ((1 << Self::NUM_BITS_INDEX) - 1)) as usize
    }

    fn length(&self) -> usize {
        // Shift right by NUM_BITS_INDEX to get the length. NUM_BITS_LENGTH specifies
        // the number of bits used for the length field, not its position.
        (self.0 >> Self::NUM_BITS_INDEX) as usize
    }
}

// TODO add tests
