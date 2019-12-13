use std::cmp;

#[derive(Default, Clone)]
pub struct ReorderedAttrs {
    reorders: Vec<Option<u16>>,
    reverse: Vec<u16>,
}

impl ReorderedAttrs {
    pub fn new() -> ReorderedAttrs {
        ReorderedAttrs { reorders: Vec::new(), reverse: Vec::new() }
    }

    pub fn insert_attribute(&mut self, attribute: u16) {
        let new_len = cmp::max(attribute as usize + 1, self.reorders.len());
        self.reorders.resize(new_len, None);
        self.reorders[attribute as usize] = Some(self.reverse.len() as u16);
        self.reverse.push(attribute);
    }

    pub fn get(&self, attribute: u16) -> Option<u16> {
        match self.reorders.get(attribute as usize)? {
            Some(attribute) => Some(*attribute),
            None => None,
        }
    }

    pub fn reverse(&self, attribute: u16) -> Option<u16> {
        self.reverse.get(attribute as usize).copied()
    }
}
