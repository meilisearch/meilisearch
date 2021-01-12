use meilisearch_tokenizer::{Token, TokenKind};

#[derive(Debug)]
enum State {
    Free,
    Quoted,
}

impl State {
    fn swap(&mut self) {
        match self {
            State::Quoted => *self = State::Free,
            State::Free => *self = State::Quoted,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryToken<'a> {
    Free(Token<'a>),
    Quoted(Token<'a>),
}

pub fn query_tokens<'a>(mut tokens: impl Iterator<Item = Token<'a>>) -> impl Iterator<Item = QueryToken<'a>> {
    let mut state = State::Free;
    let f = move || {
        loop {
            let token = tokens.next()?;
            match token.kind() {
                _ if token.text().trim() == "\"" => state.swap(),
                TokenKind::Word => {
                    let token = match state {
                        State::Quoted => QueryToken::Quoted(token),
                        State::Free => QueryToken::Free(token),
                    };
                    return Some(token);
                },
                _ => (),
            }
        }
    };
    std::iter::from_fn(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use QueryToken::{Quoted, Free};
    use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
    use fst::Set;

    macro_rules! assert_eq_query_token {
        ($test:expr, Quoted($val:literal)) => {
            match $test {
                Quoted(val) => assert_eq!(val.text(), $val),
                Free(val) => panic!("expected Quoted(\"{}\"), found Free(\"{}\")", $val, val.text()),
            }
        };

        ($test:expr, Free($val:literal)) => {
            match $test {
                Quoted(val) => panic!("expected Free(\"{}\"), found Quoted(\"{}\")", $val, val.text()),
                Free(val) => assert_eq!(val.text(), $val),
            }
        };
    }

    #[test]
    fn empty() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert!(iter.next().is_none());

        let query = " ";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert!(iter.next().is_none());
    }

    #[test]
    fn one_quoted_string() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "\"hello\"";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Quoted("hello"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn one_pending_quoted_string() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "\"hello";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Quoted("hello"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn one_non_quoted_string() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "hello";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Free("hello"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn quoted_directly_followed_by_free_strings() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "\"hello\"world";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Quoted("hello"));
        assert_eq_query_token!(iter.next().unwrap(), Free("world"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn free_directly_followed_by_quoted_strings() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "hello\"world\"";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Free("hello"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("world"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn free_followed_by_quoted_strings() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "hello \"world\"";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Free("hello"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("world"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn multiple_spaces_separated_strings() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "hello    world   ";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Free("hello"));
        assert_eq_query_token!(iter.next().unwrap(), Free("world"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn multi_interleaved_quoted_free_strings() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "hello \"world\" coucou \"monde\"";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Free("hello"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("world"));
        assert_eq_query_token!(iter.next().unwrap(), Free("coucou"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("monde"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn multi_quoted_strings() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "\"hello world\" coucou \"monde est beau\"";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Quoted("hello"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("world"));
        assert_eq_query_token!(iter.next().unwrap(), Free("coucou"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("monde"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("est"));
        assert_eq_query_token!(iter.next().unwrap(), Quoted("beau"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn chinese() {
        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let query = "汽车男生";
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let mut iter = query_tokens(tokens);
        assert_eq_query_token!(iter.next().unwrap(), Free("汽车"));
        assert_eq_query_token!(iter.next().unwrap(), Free("男生"));
        assert!(iter.next().is_none());
    }
}
