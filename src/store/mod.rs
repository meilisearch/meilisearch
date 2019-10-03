mod docs_words;
mod documents_fields;
mod main;
mod postings_lists;
mod synonyms;
mod updates;

pub use self::docs_words::DocsWords;
pub use self::documents_fields::{DocumentsFields, DocumentFieldsIter};
pub use self::main::Main;
pub use self::postings_lists::PostingsLists;
pub use self::synonyms::Synonyms;
pub use self::updates::Updates;

const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY:          &str = "ranked-map";
const SCHEMA_KEY:              &str = "schema";
const SYNONYMS_KEY:            &str = "synonyms";
const WORDS_KEY:               &str = "words";

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

fn postings_lists_name(name: &str) -> String {
    format!("{}-postings-lists", name)
}

fn documents_fields_name(name: &str) -> String {
    format!("{}-documents-fields", name)
}

fn synonyms_name(name: &str) -> String {
    format!("{}-synonyms", name)
}

fn docs_words_name(name: &str) -> String {
    format!("{}-docs-words", name)
}

fn updates_name(name: &str) -> String {
    format!("{}-updates", name)
}

#[derive(Copy, Clone)]
pub struct Index {
    pub main: Main,
    pub postings_lists: PostingsLists,
    pub documents_fields: DocumentsFields,
    pub synonyms: Synonyms,
    pub docs_words: DocsWords,
    pub updates: Updates,
}

pub fn create(env: &rkv::Rkv, name: &str) -> Result<Index, rkv::StoreError> {
    open_options(env, name, rkv::StoreOptions::create())
}

pub fn open(env: &rkv::Rkv, name: &str) -> Result<Index, rkv::StoreError> {
    let mut options = rkv::StoreOptions::default();
    options.create = false;
    open_options(env, name, options)
}

fn open_options(
    env: &rkv::Rkv,
    name: &str,
    options: rkv::StoreOptions,
) -> Result<Index, rkv::StoreError>
{
    // create all the database names
    let main_name = name;
    let postings_lists_name = postings_lists_name(name);
    let documents_fields_name = documents_fields_name(name);
    let synonyms_name = synonyms_name(name);
    let docs_words_name = docs_words_name(name);
    let updates_name = updates_name(name);

    // open all the database names
    let main = env.open_single(main_name, options)?;
    let postings_lists = env.open_single(postings_lists_name.as_str(), options)?;
    let documents_fields = env.open_single(documents_fields_name.as_str(), options)?;
    let synonyms = env.open_single(synonyms_name.as_str(), options)?;
    let docs_words = env.open_single(docs_words_name.as_str(), options)?;
    let updates = env.open_single(updates_name.as_str(), options)?;

    Ok(Index {
        main: Main { main },
        postings_lists: PostingsLists { postings_lists },
        documents_fields: DocumentsFields { documents_fields },
        synonyms: Synonyms { synonyms },
        docs_words: DocsWords { docs_words },
        updates: Updates { updates },
    })
}
