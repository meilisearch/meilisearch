use std::mem;
use self::Separator::*;

pub struct Tokenizer<'a> {
    inner: &'a str,
}

impl<'a> Tokenizer<'a> {
    pub fn new(string: &str) -> Tokenizer {
        Tokenizer { inner: string }
    }

    pub fn iter(&self) -> Tokens {
        Tokens::new(self.inner)
    }
}

pub struct Tokens<'a> {
    index: usize,
    inner: &'a str,
}

impl<'a> Tokens<'a> {
    fn new(string: &str) -> Tokens {
        Tokens {
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

impl<'a> Iterator for Tokens<'a> {
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
        let tokenizer = Tokenizer::new("salut");
        let mut tokens = tokenizer.iter();

        assert_eq!(tokens.next(), Some((0, "salut")));
        assert_eq!(tokens.next(), None);

        let tokenizer = Tokenizer::new("yo    ");
        let mut tokens = tokenizer.iter();

        assert_eq!(tokens.next(), Some((0, "yo")));
        assert_eq!(tokens.next(), None);
    }

    #[test]
    fn hard() {
        let tokenizer = Tokenizer::new(" .? yo lolo. aïe");
        let mut tokens = tokenizer.iter();

        assert_eq!(tokens.next(), Some((0, "yo")));
        assert_eq!(tokens.next(), Some((1, "lolo")));
        assert_eq!(tokens.next(), Some((9, "aïe")));
        assert_eq!(tokens.next(), None);

        let tokenizer = Tokenizer::new("yo ! lolo ? wtf - lol . aïe ,");
        let mut tokens = tokenizer.iter();

        assert_eq!(tokens.next(), Some((0, "yo")));
        assert_eq!(tokens.next(), Some((8, "lolo")));
        assert_eq!(tokens.next(), Some((16, "wtf")));
        assert_eq!(tokens.next(), Some((24, "lol")));
        assert_eq!(tokens.next(), Some((32, "aïe")));
        assert_eq!(tokens.next(), None);
    }
}
