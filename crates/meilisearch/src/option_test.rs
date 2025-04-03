use clap::Parser;

use crate::option::Opt;

#[test]
fn test_valid_opt() {
    assert!(Opt::try_parse_from(Some("")).is_ok());
}

#[test]
#[ignore]
fn test_meilli_config_file_path_valid() {
    temp_env::with_vars(
        vec![("MEILI_CONFIG_FILE_PATH", Some("../config.toml"))], // Relative path in meilisearch package
        || {
            assert!(Opt::try_build().is_ok());
        },
    );
}

#[test]
#[ignore]
fn test_meilli_config_file_path_invalid() {
    temp_env::with_vars(vec![("MEILI_CONFIG_FILE_PATH", Some("../configgg.toml"))], || {
        let possible_error_messages = [
                "unable to open or read the \"../configgg.toml\" configuration file: No such file or directory (os error 2).",
                "unable to open or read the \"../configgg.toml\" configuration file: The system cannot find the file specified. (os error 2).", // Windows
            ];
        let error_message = Opt::try_build().unwrap_err().to_string();
        assert!(
            possible_error_messages.contains(&error_message.as_str()),
            "Expected onf of {:?}, got {:?}.",
            possible_error_messages,
            error_message
        );
    });
}
