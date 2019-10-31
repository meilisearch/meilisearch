use envconfig::Envconfig;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Envconfig)]
struct Vars {
    /// The destination where the database must be created.
    #[structopt(long)]
    #[envconfig(from = "MEILI_DATABASE_PATH")]
    pub database_path: Option<String>,

    /// The addr on which the http server will listen.
    #[structopt(long)]
    #[envconfig(from = "MEILI_HTTP_ADDR")]
    pub http_addr: Option<String>,

    #[structopt(long)]
    #[envconfig(from = "MEILI_ADMIN_TOKEN")]
    pub admin_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Opt {
    pub database_path: String,
    pub http_addr: String,
    pub admin_token: Option<String>,
}

impl Default for Opt {
    fn default() -> Self {
        Opt {
            database_path: String::from("/tmp/meilidb"),
            http_addr: String::from("127.0.0.1:8080"),
            admin_token: None,
        }
    }
}

impl Opt {
    pub fn new() -> Self {
        let default = Self::default();
        let args = Vars::from_args();
        let env = Vars::init().unwrap();

        Self {
            database_path: env
                .database_path
                .or(args.database_path)
                .unwrap_or(default.database_path),
            http_addr: env
                .http_addr
                .or(args.http_addr)
                .unwrap_or(default.http_addr),
            admin_token: env.admin_token.or(args.admin_token).or(default.admin_token),
        }
    }
}
