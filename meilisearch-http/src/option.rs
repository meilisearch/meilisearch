use structopt::StructOpt;

const POSSIBLE_ENV: [&str; 2] = ["development", "production"];

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

    /// This environment variable must be set to `production` if your are running in production.
    /// If the server is running in development mode more logs will be displayed,
    /// and the master key can be avoided which implies that there is no security on the updates routes.
    /// This is useful to debug when integrating the engine with another service.
    #[structopt(long, env = "MEILI_ENV", default_value = "development", possible_values = &POSSIBLE_ENV)]
    pub env: String,

    /// Do not send analytics to Meili.
    #[structopt(long, env = "MEILI_NO_ANALYTICS")]
    pub no_analytics: bool,
}
