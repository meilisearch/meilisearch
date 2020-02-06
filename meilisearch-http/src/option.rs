use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created.
    #[structopt(long, env = "MEILI_DB_PATH", default_value = "./data.ms")]
    pub db_path: String,

    /// The address on which the http server will listen.
    #[structopt(long, env = "MEILI_HTTP_ADDR", default_value = "127.0.0.1:7700")]
    pub http_addr: String,

    /// The master key allowing you to do everything on the server.
    #[structopt(long, env = "MEILI_MASTER_KEY")]
    pub master_key: Option<String>,

    /// Do not send analytics to Meili.
    #[structopt(long, env = "MEILI_NO_ANALYTICS")]
    pub no_analytics: bool,
}
