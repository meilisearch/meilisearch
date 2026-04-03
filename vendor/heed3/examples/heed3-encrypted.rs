use std::error::Error;

use argon2::Argon2;
use chacha20poly1305::{ChaCha20Poly1305, Key};
use heed3::types::*;
use heed3::EnvOpenOptions;

fn main() -> Result<(), Box<dyn Error>> {
    let env_path = tempfile::tempdir()?;
    let password = "This is the password that will be hashed by the argon2 algorithm";
    let salt = "The salt added to the password hashes to add more security when stored";

    // We choose to use argon2 as our Key Derivation Function, but you can choose whatever you want.
    // <https://github.com/RustCrypto/traits/tree/master/password-hash#supported-crates>
    let mut key = Key::default();
    Argon2::default().hash_password_into(password.as_bytes(), salt.as_bytes(), &mut key)?;

    // We open the environment
    let mut options = EnvOpenOptions::new();
    let env = unsafe {
        options
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open_encrypted::<ChaCha20Poly1305, _>(key, &env_path)?
    };

    let key1 = "first-key";
    let val1 = "this is a secret info";
    let key2 = "second-key";
    let val2 = "this is another secret info";

    // We create database and write secret values in it
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, Some("first"))?;
    db.put(&mut wtxn, key1, val1)?;
    db.put(&mut wtxn, key2, val2)?;
    wtxn.commit()?;
    env.prepare_for_closing().wait();

    // We reopen the environment now
    let env = unsafe { options.open_encrypted::<ChaCha20Poly1305, _>(key, &env_path)? };

    // We check that the secret entries are correctly decrypted
    let mut rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, Some("first"))?.unwrap();
    let mut iter = db.iter(&mut rtxn)?;
    assert_eq!(iter.next().transpose()?, Some((key1, val1)));
    assert_eq!(iter.next().transpose()?, Some((key2, val2)));
    assert_eq!(iter.next().transpose()?, None);

    eprintln!("Successful test!");

    Ok(())
}
