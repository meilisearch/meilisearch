#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use log::{error, info};
use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use std::time::SystemTime;

use hashbrown::{HashMap, HashSet};
use chashmap::CHashMap;
use chashmap::ReadGuard;
use elapsed::measure_time;
use meilidb::database::Database;
use meilidb::database::UpdateBuilder;
use meilidb::database::schema::Schema;
use meilidb::database::schema::SchemaBuilder;
use meilidb::tokenizer::DefaultBuilder;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use structopt::StructOpt;
use warp::{Rejection, Filter};

#[derive(Debug, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created.
    #[structopt(parse(from_os_str))]
    pub database_path: PathBuf,

    /// The address and port to bind the server to.
    #[structopt(short = "l", default_value = "127.0.0.1:8080")]
    pub listen_addr: SocketAddr,

    /// The path to the list of stop words (one by line).
    #[structopt(long = "stop-words", parse(from_os_str))]
    pub stop_words: PathBuf,
}

//
// ERRORS FOR THE MULTIDATABASE
//

#[derive(Debug)]
pub enum DatabaseError {
    AlreadyExist,
    NotExist,
    NotFound(String),
    Unknown(Box<Error>),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DatabaseError::AlreadyExist => write!(f, "File already exist"),
            DatabaseError::NotExist => write!(f, "File not exist"),
            DatabaseError::NotFound(ref name) => write!(f, "Database {} not found", name),
            DatabaseError::Unknown(e) => write!(f, "{}", e),
        }
    }
}

impl Error for DatabaseError {}

impl From<Box<Error>> for DatabaseError {
    fn from(e: Box<Error>) -> DatabaseError {
        DatabaseError::Unknown(e)
    }
}

//
// MULTIDATABASE DEFINITION
//

pub struct MultiDatabase {
    databases: CHashMap<String, Database>,
    db_path: PathBuf,
    stop_words: HashSet<String>,
}

impl MultiDatabase {

    pub fn new(path: PathBuf, stop_words: HashSet<String>) -> MultiDatabase {
        MultiDatabase {
            databases: CHashMap::new(),
            db_path: path,
            stop_words: stop_words
        }
    }

    pub fn create(&self, name: String, schema: Schema) -> Result<(), DatabaseError> {
        let rdb_name = format!("{}.mdb", name);
        let database_path = self.db_path.join(rdb_name);

        if database_path.exists() {
            return Err(DatabaseError::AlreadyExist.into());
        }

        let index = Database::create(database_path, &schema)?;

        self.databases.insert_new(name, index);

        Ok(())
    }

    pub fn load(&self, name: String) -> Result<(), DatabaseError> {
        let rdb_name = format!("{}.mdb", name);
        let index_path = self.db_path.join(rdb_name);

        if !index_path.exists() {
            return Err(DatabaseError::NotExist.into());
        }

        let index = Database::open(index_path)?;

        self.databases.insert_new(name, index);

        Ok(())
    }

    pub fn load_existing(&self) {
        let paths = match fs::read_dir(self.db_path.clone()){
            Ok(p) => p,
            Err(e) => {
                error!("{}", e);
                return
            }
        };

        for path in paths {
            let path = match path {
                Ok(p) => p.path(),
                Err(_) => continue
            };

            let path_str = match path.to_str() {
                Some(p) => p,
                None => continue
            };

            let extension = match get_extension_from_path(path_str) {
                Some(e) => e,
                None => continue
            };

            if extension != "mdb" {
                continue
            }

            let name = match get_file_name_from_path(path_str) {
                Some(f) => f,
                None => continue
            };

            let db = match Database::open(path.clone()) {
                Ok(db) => db,
                Err(_) => continue
            };

            self.databases.insert_new(name.to_string(), db);
            info!("Load database {}", name);
        }
    }

    pub fn create_or_load(&self, name: String, schema: Schema) -> Result<(), DatabaseError> {
        match self.create(name.clone(), schema) {
            Err(DatabaseError::AlreadyExist) => self.load(name),
            x => x,
        }
    }

    pub fn get(&self, name: String) -> Result<ReadGuard<String, Database>, Box<Error>> {
        Ok(self.databases.get(&name).ok_or(DatabaseError::NotFound(name))?)
    }
}

fn get_extension_from_path(path: &str) -> Option<&str> {
    Path::new(path).extension().and_then(OsStr::to_str)
}

fn get_file_name_from_path(path: &str) -> Option<&str> {
    Path::new(path).file_stem().and_then(OsStr::to_str)
}

fn retrieve_stop_words(path: &Path) -> io::Result<HashSet<String>> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    let mut words = HashSet::new();

    for line in reader.lines() {
        let line = line?;
        let word = line.trim().to_string();
        words.insert(word);
    }

    Ok(words)
}

//
// PARAMS & BODY FOR HTTPS HANDLERS
//

#[derive(Deserialize)]
struct CreateBody {
    name: String,
    schema: SchemaBuilder,
}

#[derive(Deserialize)]
struct IngestBody {
    insert: Option<Vec<HashMap<String, String>>>,
    delete: Option<Vec<HashMap<String, String>>>
}

#[derive(Serialize)]
struct IngestResponse {
    inserted: usize,
    deleted: usize
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
    limit: Option<usize>,
}

//
// HTTP ROUTES
//

// Create a new index.
// The index name should be unused and the schema valid.
//
// POST /create
// Body:
//     - name: String
//     - schema: JSON
//     - stopwords: Vec<String>
fn create(body: CreateBody, db: Arc<MultiDatabase>) -> Result<String, Rejection>  {
    let schema = body.schema.build();

    match db.create(body.name.clone(), schema) {
        Ok(_) => Ok(format!("{} created ", body.name)),
        Err(e) => {
            error!("{:?}", e);
            return Err(warp::reject::not_found())
        }
    }
}

// Ingest new document.
// It's possible to have positive or/and negative updates.
//
// PUT /:name/ingest
// Body:
//     - insert: Option<Vec<JSON>>
//     - delete: Option<Vec<String>>
fn ingest(index_name: String, body: IngestBody, db: Arc<MultiDatabase>) -> Result<String, Rejection>  {

    let schema = {
        let index = match db.get(index_name.clone()){
            Ok(i) => i,
            Err(_) => return Err(warp::reject::not_found()),
        };
        let view = index.view();

        view.schema().clone()
    };

    let tokenizer_builder = DefaultBuilder::new();
    let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    let sst_name = format!("update-{}-{}.sst", index_name, now);
    let sst_path = db.db_path.join(sst_name);

    let mut response = IngestResponse{inserted: 0, deleted: 0};
    let mut update = UpdateBuilder::new(sst_path, schema);

    if let Some(documents) = body.delete {
        for doc in documents {
            if let Err(e) = update.remove_document(doc) {
                error!("Impossible to remove document; {:?}", e);
            } else {
                response.deleted += 1;
            }
        }
    }

    let stop_words = &db.stop_words;
    if let Some(documents) = body.insert {
        for doc in documents {
            if let Err(e) = update.update_document(doc, &tokenizer_builder, &stop_words) {
                error!("Impossible to update document; {:?}", e);
            } else {
                response.inserted += 1;
            }
        }
    }


    let update = match update.build() {
        Ok(u) => u,
        Err(e) => {
            error!("Impossible to create an update file; {:?}", e);
            return Err(warp::reject::not_found())
        }
    };

    {
        let index = match db.get(index_name.clone()){
            Ok(i) => i,
            Err(_) => return Err(warp::reject::not_found()),
        };

        if let Err(e) = index.ingest_update_file(update) {
            error!("Impossible to ingest sst file; {:?}", e);
            return Err(warp::reject::not_found())
        };
    }

    if let Ok(response) = serde_json::to_string(&response) {
        return Ok(response);
    };

    return Err(warp::reject::not_found())
}

// Search in a specific index
// The default limit is 20
//
// GET /:name/search
// Params:
//     - query: String
//     - limit: Option<usize>
fn search(index_name: String, query: SearchQuery, db: Arc<MultiDatabase>) -> Result<String, Rejection>  {

    let view = {
        let index = match db.get(index_name.clone()){
            Ok(i) => i,
            Err(_) => return Err(warp::reject::not_found()),
        };
        index.view()
    };

    let limit = query.limit.unwrap_or(20);

    let query_builder = match view.query_builder() {
        Ok(q) => q,
        Err(_err) => return Err(warp::reject::not_found()),
    };

    let (time, responses) = measure_time(|| {
        let docs = query_builder.query(&query.q, 0..limit);
        let mut results: Vec<HashMap<String, String>> = Vec::with_capacity(limit);
        for doc in docs {
            match view.document_by_id(doc.id) {
                Ok(val) => results.push(val),
                Err(e) => println!("{:?}", e),
            }
        }
        results
    });

    let response = match serde_json::to_string(&responses) {
        Ok(val) => val,
        Err(err) => format!("{:?}", err),
    };

    info!("index: {} - search: {:?} - limit: {} - time: {}", index_name, query.q, limit, time);
    Ok(response)
}

fn start_server(listen_addr: SocketAddr, db: Arc<MultiDatabase>) {
    let index_path = warp::path("index").and(warp::path::param::<String>());
    let db = warp::any().map(move || db.clone());

    let create_path = warp::path("create").and(warp::path::end());
    let ingest_path = index_path.and(warp::path("ingest")).and(warp::path::end());
    let search_path = index_path.and(warp::path("search")).and(warp::path::end());

    let create = warp::post2()
        .and(create_path)
        .and(warp::body::json())
        .and(db.clone())
        .and_then(create);

    let ingest = warp::put2()
        .and(ingest_path)
        .and(warp::body::json())
        .and(db.clone())
        .and_then(ingest);

    let search = warp::get2()
        .and(search_path)
        .and(warp::query())
        .and(db.clone())
        .and_then(search);

    let api = create
        .or(ingest)
        .or(search);

    let logs = warp::log("server");
    let headers = warp::reply::with::header("Content-Type", "application/json");

    let routes = api.with(logs).with(headers);

    info!("Server is started on {}", listen_addr);
    warp::serve(routes).run(listen_addr);
}

fn main() {
    env_logger::init();
    let opt = Opt::from_args();

    let stop_words = match retrieve_stop_words(&opt.stop_words) {
        Ok(s) => s,
        Err(_) => HashSet::new(),
    };

    let db = Arc::new(MultiDatabase::new(opt.database_path.clone(), stop_words));

    db.load_existing();

    start_server(opt.listen_addr, db);
}


