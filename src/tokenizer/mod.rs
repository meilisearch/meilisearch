use std::mem;
use self::Separator::*;

pub trait TokenizerBuilder {
    fn build<'a>(&self, text: &'a str) -> Box<Iterator<Item=(usize, &'a str)> + 'a>;
}

pub struct DefaultBuilder;

impl DefaultBuilder {
    pub fn new() -> DefaultBuilder {
        DefaultBuilder
    }
}

impl TokenizerBuilder for DefaultBuilder {
    fn build<'a>(&self, text: &'a str) -> Box<Iterator<Item=(usize, &'a str)> + 'a> {
        Box::new(Tokenizer::new(text))
    }
}

pub struct Tokenizer<'a> {
    index: usize,
    inner: &'a str,
}

impl<'a> Tokenizer<'a> {
    pub fn new(string: &str) -> Tokenizer {
        Tokenizer {
            index: 0,
            inner: string.trim_matches(&[' ', '.', ';', ',', '!', '?', '-', '\'', '"'][..]),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Separator {
    Short,
    Long,
}

impl Separator {
    fn add(self, add: Separator) -> Separator {
        match (self, add) {
            (_,     Long)  => Long,
            (Short, Short) => Short,
            (Long,  Short) => Long,
        }
    }

    fn to_usize(self) -> usize {
        match self {
            Short => 1,
            Long => 8,
        }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = (usize, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        let mut start_word = None;
        let mut distance = None;

        for (i, c) in self.inner.char_indices() {
            let separator = match c {
                '.' | ';' | ',' | '!' | '?' | '-' => Some(Long),
                ' ' | '\'' | '"' => Some(Short),
                _   => None,
            };

            match separator {
                Some(dist) => {
                    if let Some(start_word) = start_word {
                        let (word, tail) = self.inner.split_at(i);

                        self.inner = tail;
                        self.index += distance.map(Separator::to_usize).unwrap_or(0);

                        let word = &word[start_word..];
                        return Some((self.index, word))
                    }
                    distance = Some(distance.map(|s| s.add(dist)).unwrap_or(dist));
                },
                None => { start_word.get_or_insert(i); },
            }
        }

        if let Some(start_word) = start_word {
            let word = mem::replace(&mut self.inner, "");
            self.index += distance.map(Separator::to_usize).unwrap_or(0);

            let word = &word[start_word..];
            return Some((self.index, word))
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn easy() {
        let mut tokenizer = Tokenizer::new("salut");

        assert_eq!(tokenizer.next(), Some((0, "salut")));
        assert_eq!(tokenizer.next(), None);

        let mut tokenizer = Tokenizer::new("yo    ");

        assert_eq!(tokenizer.next(), Some((0, "yo")));
        assert_eq!(tokenizer.next(), None);
    }

    #[test]
    fn hard() {
        let mut tokenizer = Tokenizer::new(" .? yo lolo. aïe");

        assert_eq!(tokenizer.next(), Some((0, "yo")));
        assert_eq!(tokenizer.next(), Some((1, "lolo")));
        assert_eq!(tokenizer.next(), Some((9, "aïe")));
        assert_eq!(tokenizer.next(), None);

        let mut tokenizer = Tokenizer::new("yo ! lolo ? wtf - lol . aïe ,");

        assert_eq!(tokenizer.next(), Some((0, "yo")));
        assert_eq!(tokenizer.next(), Some((8, "lolo")));
        assert_eq!(tokenizer.next(), Some((16, "wtf")));
        assert_eq!(tokenizer.next(), Some((24, "lol")));
        assert_eq!(tokenizer.next(), Some((32, "aïe")));
        assert_eq!(tokenizer.next(), None);
    }
}
