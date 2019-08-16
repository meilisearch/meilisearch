#[derive(Default, Clone)]
pub struct ReorderedAttrs {
    count: usize,
    reorders: Vec<Option<u16>>,
}

impl ReorderedAttrs {
    pub fn new() -> ReorderedAttrs {
        ReorderedAttrs { count: 0, reorders: Vec::new() }
    }

    pub fn insert_attribute(&mut self, attribute: u16) {
        self.reorders.resize(attribute as usize + 1, None);
        self.reorders[attribute as usize] = Some(self.count as u16);
        self.count += 1;
    }

    pub fn get(&self, attribute: u16) -> Option<u16> {
        match self.reorders.get(attribute as usize) {
            Some(Some(attribute)) => Some(*attribute),
            _ => None,
        }
    }
}
