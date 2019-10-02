mod words;
mod synonyms;

pub use self::words::Words;
pub use self::synonyms::Synonyms;

const SCHEMA_KEY:              &str = "schema";
const WORDS_KEY:               &str = "words";
const SYNONYMS_KEY:            &str = "synonyms";
const RANKED_MAP_KEY:          &str = "ranked-map";
const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

pub fn create(env: &rkv::Rkv, name: &str) -> Result<(Words, Synonyms), rkv::StoreError> {
    let main = env.open_single(name, rkv::StoreOptions::create())?;
    let words_indexes = env.open_single(format!("{}-words-indexes", name).as_str(), rkv::StoreOptions::create())?;
    let synonyms = env.open_single(format!("{}-synonyms", name).as_str(), rkv::StoreOptions::create())?;

    let words = Words { main, words_indexes };
    let synonyms = Synonyms { main, synonyms };

    Ok((words, synonyms))
}
