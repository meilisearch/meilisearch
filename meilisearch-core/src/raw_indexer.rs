use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;

use meilisearch_schema::IndexedPos;
use meilisearch_tokenizer::analyzer::{Analyzer, AnalyzerConfig};
use meilisearch_tokenizer::{Token, token::SeparatorKind, TokenKind};
use sdset::SetBuf;

use crate::{DocIndex, DocumentId};
use crate::FstSetCow;

const WORD_LENGTH_LIMIT: usize = 80;

type Word = Vec<u8>; // TODO make it be a SmallVec

pub struct RawIndexer<'a, A>
where
    A: AsRef<[u8]>
{
    word_limit: usize, // the maximum number of indexed words
    words_doc_indexes: BTreeMap<Word, Vec<DocIndex>>,
    docs_words: HashMap<DocumentId, Vec<Word>>,
    analyzer: Analyzer<'a, A>,
}

pub struct Indexed<'a> {
    pub words_doc_indexes: BTreeMap<Word, SetBuf<DocIndex>>,
    pub docs_words: HashMap<DocumentId, FstSetCow<'a>>,
}

impl<'a, A> RawIndexer<'a, A>
where
    A: AsRef<[u8]>
{
    pub fn new(stop_words: &'a fst::Set<A>) -> RawIndexer<'a, A> {
        RawIndexer::with_word_limit(stop_words, 1000)
    }

    pub fn with_word_limit(stop_words: &'a fst::Set<A>, limit: usize) -> RawIndexer<A> {
        RawIndexer {
            word_limit: limit,
            words_doc_indexes: BTreeMap::new(),
            docs_words: HashMap::new(),
            analyzer: Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words)),
        }
    }

    pub fn index_text(&mut self, id: DocumentId, indexed_pos: IndexedPos, text: &str) -> usize {
        let mut number_of_words = 0;

        let analyzed_text = self.analyzer.analyze(text);
        for (token_pos, (word_pos, token)) in analyzed_text
            .tokens()
            .scan((0, false), |(offset, is_hard_sep), mut token| {
                match token.kind {
                    TokenKind::Word => {
                        token.char_index += *offset;
                        if *is_hard_sep {
                            *offset += 8;
                        } else {
                            *offset += 1;
                        }
                        *is_hard_sep = false;
                    }
                    TokenKind::Separator(SeparatorKind::Hard) => {
                        *is_hard_sep = true;
                    }
                    _ => (),
                }
                Some((*offset, token))
            })
            .filter(|(_, t)| t.is_word())
            .enumerate() {
            let must_continue = index_token(
                token,
                word_pos,
                token_pos,
                id,
                indexed_pos,
                self.word_limit,
                &mut self.words_doc_indexes,
                &mut self.docs_words,
            );

            number_of_words += 1;

            if !must_continue {
                break;
            }
        }

        number_of_words
    }

    pub fn index_text_seq<'s, I>(&mut self, id: DocumentId, indexed_pos: IndexedPos, iter: I)
    where
        I: IntoIterator<Item = &'s str>,
    {
        let mut byte_offset = 0;
        let mut word_offset = 0;

        for s in iter.into_iter() {
            let current_byte_offset = byte_offset;
            let current_word_offset = word_offset;

            let analyzed_text = self.analyzer.analyze(s);
            let tokens = analyzed_text
                .tokens()
                .scan((0, false), |(offset, is_hard_sep), mut token| {
                    match token.kind {
                        TokenKind::Word | TokenKind::StopWord | TokenKind::Any => {
                            token.char_index += *offset;
                            if *is_hard_sep {
                                *offset += 8;
                            } else {
                                *offset += 1;
                            }
                            *is_hard_sep = false;
                        }
                        TokenKind::Separator(SeparatorKind::Hard) => {
                            *is_hard_sep = true;
                        }
                        _ => (),
                    }
                    Some((*offset, token))
                })
                .filter(|(_, t)| t.is_word())
                .map(|(i, mut t)| {
                    t.byte_start = t.byte_start + current_byte_offset;
                    t.byte_end = t.byte_end + current_byte_offset;
                    (i, t)
                })
                .map(|(i, t)| (i + current_word_offset, t))
                .enumerate();

            for (token_pos, (word_pos, token)) in tokens  {
                word_offset = word_pos + 1;
                byte_offset = token.byte_end + 1;

                let must_continue = index_token(
                    token,
                    word_pos,
                    token_pos,
                    id,
                    indexed_pos,
                    self.word_limit,
                    &mut self.words_doc_indexes,
                    &mut self.docs_words,
                );

                if !must_continue {
                    break;
                }
            }
        }
    }

    pub fn build(self) -> Indexed<'static> {
        let words_doc_indexes = self
            .words_doc_indexes
            .into_iter()
            .map(|(word, indexes)| (word, SetBuf::from_dirty(indexes)))
            .collect();

        let docs_words = self
            .docs_words
            .into_iter()
            .map(|(id, mut words)| {
                words.sort_unstable();
                words.dedup();
                let fst = fst::Set::from_iter(words).unwrap().map_data(Cow::Owned).unwrap();
                (id, fst)
            })
            .collect();

        Indexed {
            words_doc_indexes,
            docs_words,
        }
    }
}

fn index_token(
    token: Token,
    word_pos: usize,
    token_pos: usize,
    id: DocumentId,
    indexed_pos: IndexedPos,
    word_limit: usize,
    words_doc_indexes: &mut BTreeMap<Word, Vec<DocIndex>>,
    docs_words: &mut HashMap<DocumentId, Vec<Word>>,
) -> bool
{
    if token_pos >= word_limit {
        return false;
    }

    if !token.is_stopword() {
        match token_to_docindex(id, indexed_pos, &token, word_pos) {
            Some(docindex) => {
                let word = Vec::from(token.word.as_ref());

                if word.len() <= WORD_LENGTH_LIMIT {
                    words_doc_indexes
                        .entry(word.clone())
                        .or_insert_with(Vec::new)
                        .push(docindex);
                    docs_words.entry(id).or_insert_with(Vec::new).push(word);
                }
            }
            None => return false,
        }
    }

    true
}

fn token_to_docindex(id: DocumentId, indexed_pos: IndexedPos, token: &Token, word_index: usize) -> Option<DocIndex> {
    let word_index = u16::try_from(word_index).ok()?;
    let char_index = u16::try_from(token.char_index).ok()?;
    let char_length = u16::try_from(token.word.chars().count()).ok()?;

    let docindex = DocIndex {
        document_id: id,
        attribute: indexed_pos.0,
        word_index,
        char_index,
        char_length,
    };

    Some(docindex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meilisearch_schema::IndexedPos;

    #[test]
    fn strange_apostrophe() {
        let stop_words = fst::Set::default();
        let mut indexer = RawIndexer::new(&stop_words);

        let docid = DocumentId(0);
        let indexed_pos = IndexedPos(0);
        let text = "Zut, l’aspirateur, j’ai oublié de l’éteindre !";
        indexer.index_text(docid, indexed_pos, text);

        let Indexed {
            words_doc_indexes, ..
        } = indexer.build();

        assert!(words_doc_indexes.get(&b"l"[..]).is_some());
        assert!(words_doc_indexes.get(&b"aspirateur"[..]).is_some());
        assert!(words_doc_indexes.get(&b"ai"[..]).is_some());
        assert!(words_doc_indexes.get(&b"eteindre"[..]).is_some());
    }

    #[test]
    fn strange_apostrophe_in_sequence() {
        let stop_words = fst::Set::default();
        let mut indexer = RawIndexer::new(&stop_words);

        let docid = DocumentId(0);
        let indexed_pos = IndexedPos(0);
        let text = vec!["Zut, l’aspirateur, j’ai oublié de l’éteindre !"];
        indexer.index_text_seq(docid, indexed_pos, text);

        let Indexed {
            words_doc_indexes, ..
        } = indexer.build();

        assert!(words_doc_indexes.get(&b"l"[..]).is_some());
        assert!(words_doc_indexes.get(&b"aspirateur"[..]).is_some());
        assert!(words_doc_indexes.get(&b"ai"[..]).is_some());
        assert!(words_doc_indexes.get(&b"eteindre"[..]).is_some());
    }

    #[test]
    fn basic_stop_words() {
        let stop_words = sdset::SetBuf::from_dirty(vec!["l", "j", "ai", "de"]);
        let stop_words = fst::Set::from_iter(stop_words).unwrap();

        let mut indexer = RawIndexer::new(&stop_words);

        let docid = DocumentId(0);
        let indexed_pos = IndexedPos(0);
        let text = "Zut, l’aspirateur, j’ai oublié de l’éteindre !";
        indexer.index_text(docid, indexed_pos, text);

        let Indexed {
            words_doc_indexes, ..
        } = indexer.build();

        assert!(words_doc_indexes.get(&b"l"[..]).is_none());
        assert!(words_doc_indexes.get(&b"aspirateur"[..]).is_some());
        assert!(words_doc_indexes.get(&b"j"[..]).is_none());
        assert!(words_doc_indexes.get(&b"ai"[..]).is_none());
        assert!(words_doc_indexes.get(&b"de"[..]).is_none());
        assert!(words_doc_indexes.get(&b"eteindre"[..]).is_some());
    }

    #[test]
    fn no_empty_unidecode() {
        let stop_words = fst::Set::default();
        let mut indexer = RawIndexer::new(&stop_words);

        let docid = DocumentId(0);
        let indexed_pos = IndexedPos(0);
        let text = "🇯🇵";
        indexer.index_text(docid, indexed_pos, text);

        let Indexed {
            words_doc_indexes, ..
        } = indexer.build();

        assert!(words_doc_indexes
            .get(&"🇯🇵".to_owned().into_bytes())
            .is_some());
    }

    #[test]
    // test sample from 807
    fn very_long_text() {
        let stop_words = fst::Set::default();
        let mut indexer = RawIndexer::new(&stop_words);
        let indexed_pos = IndexedPos(0);
        let docid = DocumentId(0);
        let text = " The locations block is the most powerful, and potentially most involved, section of the .platform.app.yaml file. It allows you to control how the application container responds to incoming requests at a very fine-grained level. Common patterns also vary between language containers due to the way PHP-FPM handles incoming requests.\nEach entry of the locations block is an absolute URI path (with leading /) and its value includes the configuration directives for how the web server should handle matching requests. That is, if your domain is example.com then '/' means &ldquo;requests for example.com/&rdquo;, while '/admin' means &ldquo;requests for example.com/admin&rdquo;. If multiple blocks could match an incoming request then the most-specific will apply.\nweb:locations:&#39;/&#39;:# Rules for all requests that don&#39;t otherwise match....&#39;/sites/default/files&#39;:# Rules for any requests that begin with /sites/default/files....The simplest possible locations configuration is one that simply passes all requests on to your application unconditionally:\nweb:locations:&#39;/&#39;:passthru:trueThat is, all requests to /* should be forwarded to the process started by web.commands.start above. Note that for PHP containers the passthru key must specify what PHP file the request should be forwarded to, and must also specify a docroot under which the file lives. For example:\nweb:locations:&#39;/&#39;:root:&#39;web&#39;passthru:&#39;/app.php&#39;This block will serve requests to / from the web directory in the application, and if a file doesn&rsquo;t exist on disk then the request will be forwarded to the /app.php script.\nA full list of the possible subkeys for locations is below.\n  root: The folder from which to serve static assets for this location relative to the application root. The application root is the directory in which the .platform.app.yaml file is located. Typical values for this property include public or web. Setting it to '' is not recommended, and its behavior may vary depending on the type of application. Absolute paths are not supported.\n  passthru: Whether to forward disallowed and missing resources from this location to the application and can be true, false or an absolute URI path (with leading /). The default value is false. For non-PHP applications it will generally be just true or false. In a PHP application this will typically be the front controller such as /index.php or /app.php. This entry works similar to mod_rewrite under Apache. Note: If the value of passthru does not begin with the same value as the location key it is under, the passthru may evaluate to another entry. That may be useful when you want different cache settings for different paths, for instance, but want missing files in all of them to map back to the same front controller. See the example block below.\n  index: The files to consider when serving a request for a directory: an array of file names or null. (typically ['index.html']). Note that in order for this to work, access to the static files named must be allowed by the allow or rules keys for this location.\n  expires: How long to allow static assets from this location to be cached (this enables the Cache-Control and Expires headers) and can be a time or -1 for no caching (default). Times can be suffixed with &ldquo;ms&rdquo; (milliseconds), &ldquo;s&rdquo; (seconds), &ldquo;m&rdquo; (minutes), &ldquo;h&rdquo; (hours), &ldquo;d&rdquo; (days), &ldquo;w&rdquo; (weeks), &ldquo;M&rdquo; (months, 30d) or &ldquo;y&rdquo; (years, 365d).\n  scripts: Whether to allow loading scripts in that location (true or false). This directive is only meaningful on PHP.\n  allow: Whether to allow serving files which don&rsquo;t match a rule (true or false, default: true).\n  headers: Any additional headers to apply to static assets. This section is a mapping of header names to header values. Responses from the application aren&rsquo;t affected, to avoid overlap with the application&rsquo;s own ability to include custom headers in the response.\n  rules: Specific overrides for a specific location. The key is a PCRE (regular expression) that is matched against the full request path.\n  request_buffering: Most application servers do not support chunked requests (e.g. fpm, uwsgi), so Platform.sh enables request_buffering by default to handle them. That default configuration would look like this if it was present in .platform.app.yaml:\nweb:locations:&#39;/&#39;:passthru:truerequest_buffering:enabled:truemax_request_size:250mIf the application server can already efficiently handle chunked requests, the request_buffering subkey can be modified to disable it entirely (enabled: false). Additionally, applications that frequently deal with uploads greater than 250MB in size can update the max_request_size key to the application&rsquo;s needs. Note that modifications to request_buffering will need to be specified at each location where it is desired.\n ";
        indexer.index_text(docid, indexed_pos, text);
        let Indexed {
            words_doc_indexes, ..
        } = indexer.build();
        assert!(words_doc_indexes.get(&"request".to_owned().into_bytes()).is_some());
    }

    #[test]
    fn words_over_index_1000_not_indexed() {
        let stop_words = fst::Set::default();
        let mut indexer = RawIndexer::new(&stop_words);
        let indexed_pos = IndexedPos(0);
        let docid = DocumentId(0);
        let mut text = String::with_capacity(5000);
        for _ in 0..1000 {
            text.push_str("less ");
        }
        text.push_str("more");
        indexer.index_text(docid, indexed_pos, &text);
        let Indexed {
            words_doc_indexes, ..
        } = indexer.build();
        assert!(words_doc_indexes.get(&"less".to_owned().into_bytes()).is_some());
        assert!(words_doc_indexes.get(&"more".to_owned().into_bytes()).is_none());
    }
}
