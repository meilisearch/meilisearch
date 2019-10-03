mod documents_fields;
mod synonyms;
mod words;

pub use self::documents_fields::{DocumentsFields, DocumentFieldsIter};
pub use self::synonyms::Synonyms;
pub use self::words::Words;

const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY:          &str = "ranked-map";
const SCHEMA_KEY:              &str = "schema";
const SYNONYMS_KEY:            &str = "synonyms";
const WORDS_KEY:               &str = "words";

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

fn words_indexes_name(name: &str) -> String {
    format!("{}-words-indexes", name)
}

fn synonyms_name(name: &str) -> String {
    format!("{}-synonyms", name)
}

fn documents_fields_name(name: &str) -> String {
    format!("{}-documents-fields", name)
}

pub fn create(
    env: &rkv::Rkv,
    name: &str,
) -> Result<(Words, Synonyms, DocumentsFields), rkv::StoreError>
{
    open_options(env, name, rkv::StoreOptions::create())
}

pub fn open(
    env: &rkv::Rkv,
    name: &str,
) -> Result<(Words, Synonyms, DocumentsFields), rkv::StoreError>
{
    let mut options = rkv::StoreOptions::default();
    options.create = false;
    open_options(env, name, options)
}

fn open_options(
    env: &rkv::Rkv,
    name: &str,
    options: rkv::StoreOptions,
) -> Result<(Words, Synonyms, DocumentsFields), rkv::StoreError>
{
    // create all the database names
    let main_name = name;
    let words_indexes_name = words_indexes_name(name);
    let synonyms_name = synonyms_name(name);
    let documents_fields_name = documents_fields_name(name);

    // open all the database names
    let main = env.open_single(main_name, options)?;
    let words_indexes = env.open_single(words_indexes_name.as_str(), options)?;
    let synonyms = env.open_single(synonyms_name.as_str(), options)?;
    let documents_fields = env.open_single(documents_fields_name.as_str(), options)?;

    let words = Words { main, words_indexes };
    let synonyms = Synonyms { main, synonyms };
    let documents_fields = DocumentsFields { documents_fields };

    Ok((words, synonyms, documents_fields))
}
