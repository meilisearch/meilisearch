use std::mem;
use self::Separator::*;

pub trait TokenizerBuilder {
    fn build<'a>(&self, text: &'a str) -> Box<Iterator<Item=Token<'a>> + 'a>;
}

pub struct DefaultBuilder;

impl DefaultBuilder {
    pub fn new() -> DefaultBuilder {
        DefaultBuilder
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Token<'a> {
    pub word: &'a str,
    pub word_index: usize,
    pub char_index: usize,
}

impl TokenizerBuilder for DefaultBuilder {
    fn build<'a>(&self, text: &'a str) -> Box<Iterator<Item=Token<'a>> + 'a> {
        Box::new(Tokenizer::new(text))
    }
}

pub struct Tokenizer<'a> {
    word_index: usize,
    char_index: usize,
    inner: &'a str,
}

impl<'a> Tokenizer<'a> {
    pub fn new(string: &str) -> Tokenizer {
        let mut char_advance = 0;
        let mut index_advance = 0;
        for (n, (i, c)) in string.char_indices().enumerate() {
            char_advance = n;
            index_advance = i;
            if detect_separator(c).is_none() { break }
        }

        Tokenizer {
            word_index: 0,
            char_index: char_advance,
            inner: &string[index_advance..],
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

fn detect_separator(c: char) -> Option<Separator> {
    match c {
        '.' | ';' | ',' | '!' | '?' | '-' => Some(Long),
        ' ' | '\'' | '"'                  => Some(Short),
        _                                 => None,
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut start_word = None;
        let mut distance = None;

        for (i, c) in self.inner.char_indices() {
            match detect_separator(c) {
                Some(sep) => {
                    if let Some(start_word) = start_word {
                        let (prefix, tail) = self.inner.split_at(i);
                        let (spaces, word) = prefix.split_at(start_word);

                        self.inner = tail;
                        self.char_index += spaces.len();
                        self.word_index += distance.map(Separator::to_usize).unwrap_or(0);

                        let token = Token {
                            word: word,
                            word_index: self.word_index,
                            char_index: self.char_index,
                        };

                        self.char_index += word.len();
                        return Some(token)
                    }

                    distance.replace(distance.map_or(sep, |s| s.add(sep)));
                },
                None => { start_word.get_or_insert(i); },
            }
        }

        if let Some(start_word) = start_word {
            let prefix = mem::replace(&mut self.inner, "");
            let (spaces, word) = prefix.split_at(start_word);

            let token = Token {
                word: word,
                word_index: self.word_index + distance.map(Separator::to_usize).unwrap_or(0),
                char_index: self.char_index + spaces.len(),
            };
            return Some(token)
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

        assert_eq!(tokenizer.next(), Some(Token { word: "salut", word_index: 0, char_index: 0 }));
        assert_eq!(tokenizer.next(), None);

        let mut tokenizer = Tokenizer::new("yo    ");

        assert_eq!(tokenizer.next(), Some(Token { word: "yo", word_index: 0, char_index: 0 }));
        assert_eq!(tokenizer.next(), None);
    }

    #[test]
    fn hard() {
        let mut tokenizer = Tokenizer::new(" .? yo lolo. aïe");

        assert_eq!(tokenizer.next(), Some(Token { word: "yo", word_index: 0, char_index: 4 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "lolo", word_index: 1, char_index: 7 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "aïe", word_index: 9, char_index: 13 }));
        assert_eq!(tokenizer.next(), None);

        let mut tokenizer = Tokenizer::new("yo ! lolo ? wtf - lol . aïe ,");

        assert_eq!(tokenizer.next(), Some(Token { word: "yo", word_index: 0, char_index: 0 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "lolo", word_index: 8, char_index: 5 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "wtf", word_index: 16, char_index: 12 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "lol", word_index: 24, char_index: 18 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "aïe", word_index: 32, char_index: 24 }));
        assert_eq!(tokenizer.next(), None);
    }

    #[test]
    fn hard_long_chars() {
        let mut tokenizer = Tokenizer::new(" .? yo 😂. aïe");

        assert_eq!(tokenizer.next(), Some(Token { word: "yo", word_index: 0, char_index: 4 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "😂", word_index: 1, char_index: 7 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "aïe", word_index: 9, char_index: 13 }));
        assert_eq!(tokenizer.next(), None);

        let mut tokenizer = Tokenizer::new("yo ! lolo ? 😱 - lol . 😣 ,");

        assert_eq!(tokenizer.next(), Some(Token { word: "yo", word_index: 0, char_index: 0 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "lolo", word_index: 8, char_index: 5 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "😱", word_index: 16, char_index: 12 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "lol", word_index: 24, char_index: 19 }));
        assert_eq!(tokenizer.next(), Some(Token { word: "😣", word_index: 32, char_index: 25 }));
        assert_eq!(tokenizer.next(), None);
    }
}
