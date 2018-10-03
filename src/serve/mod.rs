#[cfg(feature = "serve-http")]
mod http;

#[cfg(feature = "serve-console")]
mod console;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub enum CommandServe {

    #[cfg(feature = "serve-http")]
    /// Serve an index under an http protocol.
    #[structopt(name = "http")]
    Http(self::http_feature::CommandHttp),

    #[cfg(feature = "serve-console")]
    /// Serve an index under a simple console.
    #[structopt(name = "console")]
    Console(self::console_feature::CommandConsole),
}

#[cfg(feature = "serve-http")]
pub mod http_feature {
    use std::error;
    use std::path::PathBuf;
    use std::net::SocketAddr;
    use structopt::StructOpt;

    #[derive(Debug, StructOpt)]
    pub struct CommandHttp {
        /// The address and port to bind the server to.
        #[structopt(short = "l", default_value = "127.0.0.1:3030")]
        pub listen_addr: SocketAddr,

        /// The stop word file, each word must be separated by a newline.
        #[structopt(long = "stop-words", parse(from_os_str))]
        pub stop_words: PathBuf,

        /// Meta file name (e.g. relaxed-colden).
        #[structopt(parse(from_os_str))]
        pub meta_name: PathBuf,
    }

    pub fn http(command: CommandHttp) -> Result<(), Box<error::Error>> {
        use super::http::HttpServer;

        let server = HttpServer::from_command(command)?;
        Ok(server.serve())
    }
}

#[cfg(feature = "serve-console")]
pub mod console_feature {
    use std::error;
    use std::path::PathBuf;
    use structopt::StructOpt;

    #[derive(Debug, StructOpt)]
    pub struct CommandConsole {
        /// The stop word file, each word must be separated by a newline.
        #[structopt(long = "stop-words", parse(from_os_str))]
        pub stop_words: PathBuf,

        /// Meta file name (e.g. relaxed-colden).
        #[structopt(parse(from_os_str))]
        pub meta_name: PathBuf,
    }

    pub fn console(command: CommandConsole) -> Result<(), Box<error::Error>> {
        use super::console::ConsoleSearch;

        let search = ConsoleSearch::from_command(command)?;
        Ok(search.serve())
    }
}
