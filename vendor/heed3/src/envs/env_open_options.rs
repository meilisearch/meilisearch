use std::ffi::CString;
use std::io::ErrorKind::NotFound;
use std::marker::PhantomData;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;
use std::{io, ptr};

#[cfg(master3)]
use aead::{generic_array::typenum::Unsigned, AeadCore, AeadMutInPlace, Key, KeyInit};
use synchronoise::SignalEvent;

#[cfg(master3)]
use super::encrypted_env::{encrypt_func_wrapper, EncryptedEnv};
use super::env::Env;
use super::{canonicalize_path, OPENED_ENV};
#[cfg(windows)]
use crate::envs::OsStrExtLmdb as _;
use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::txn::{TlsUsage, WithTls, WithoutTls};
use crate::{EnvFlags, Error, Result};

/// Options and flags which can be used to configure how an environment is opened.
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EnvOpenOptions<T: TlsUsage = WithTls> {
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: Option<u32>,
    flags: EnvFlags,
    _tls_marker: PhantomData<T>,
}

impl EnvOpenOptions<WithTls> {
    /// Creates a blank new set of options ready for configuration.
    pub fn new() -> EnvOpenOptions<WithTls> {
        EnvOpenOptions {
            map_size: None,
            max_readers: None,
            max_dbs: None,
            flags: EnvFlags::empty(),
            _tls_marker: PhantomData,
        }
    }
}

impl<T: TlsUsage> EnvOpenOptions<T> {
    /// Make the read transactions `!Send` by specifying they will
    /// use Thread Local Storage (TLS). It is often faster to open
    /// TLS-backed transactions.
    ///
    /// A thread can only use one transaction at a time, plus any
    /// child (nested) transactions. Each transaction belongs to one
    /// thread. A `BadRslot` error will be thrown when multiple read
    /// transactions exists on the same thread.
    ///
    /// # Example
    ///
    /// This example shows that the `RoTxn<'_, WithTls>` cannot be sent between threads.
    ///
    /// ```compile_fail
    /// use std::fs;
    /// use std::path::Path;
    /// use heed3::{EnvOpenOptions, Database, EnvFlags};
    /// use heed3::types::*;
    ///
    /// /// Checks, at compile time, that a type can be sent accross threads.
    /// fn is_sendable<S: Send>(_x: S) {}
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut env_builder = EnvOpenOptions::new().read_txn_with_tls();
    /// let dir = tempfile::tempdir().unwrap();
    /// let env = unsafe { env_builder.open(dir.path())? };
    ///
    /// let rtxn = env.read_txn()?;
    /// is_sendable(rtxn);
    /// # Ok(()) }
    /// ```
    pub fn read_txn_with_tls(self) -> EnvOpenOptions<WithTls> {
        let Self { map_size, max_readers, max_dbs, flags, _tls_marker: _ } = self;
        EnvOpenOptions { map_size, max_readers, max_dbs, flags, _tls_marker: PhantomData }
    }

    /// Make the read transactions `Send` by specifying they will
    /// not use Thread Local Storage (TLS).
    ///
    /// A thread can use any number of read transactions at a time on
    /// the same thread. Read transactions can be moved in between
    /// threads (`Send`).
    ///
    /// ## From LMDB's documentation
    ///
    /// Don't use Thread-Local Storage. Tie reader locktable slots to
    /// #MDB_txn objects instead of to threads. I.e. #mdb_txn_reset() keeps
    /// the slot reserved for the #MDB_txn object. A thread may use parallel
    /// read-only transactions. A read-only transaction may span threads if
    /// the user synchronizes its use. Applications that multiplex many
    /// user threads over individual OS threads need this option. Such an
    /// application must also serialize the write transactions in an OS
    /// thread, since LMDB's write locking is unaware of the user threads.
    ///
    /// # Example
    ///
    /// This example shows that the `RoTxn<'_, WithoutTls>` can be sent between threads.
    ///
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use heed3::{EnvOpenOptions, Database, EnvFlags};
    /// use heed3::types::*;
    ///
    /// /// Checks, at compile time, that a type can be sent accross threads.
    /// fn is_sendable<S: Send>(_x: S) {}
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut env_builder = EnvOpenOptions::new().read_txn_without_tls();
    /// let dir = tempfile::tempdir().unwrap();
    /// let env = unsafe { env_builder.open(dir.path())? };
    ///
    /// let rtxn = env.read_txn()?;
    /// is_sendable(rtxn);
    /// # Ok(()) }
    /// ```
    pub fn read_txn_without_tls(self) -> EnvOpenOptions<WithoutTls> {
        let Self { map_size, max_readers, max_dbs, flags, _tls_marker: _ } = self;
        EnvOpenOptions { map_size, max_readers, max_dbs, flags, _tls_marker: PhantomData }
    }

    /// Set the size of the memory map to use for this environment.
    ///
    /// It must be a multiple of the OS page size.
    pub fn map_size(&mut self, size: usize) -> &mut Self {
        self.map_size = Some(size);
        self
    }

    /// Set the maximum number of threads/reader slots for the environment.
    pub fn max_readers(&mut self, readers: u32) -> &mut Self {
        self.max_readers = Some(readers);
        self
    }

    /// Set the maximum number of named databases for the environment.
    pub fn max_dbs(&mut self, dbs: u32) -> &mut Self {
        self.max_dbs = Some(dbs);
        self
    }

    /// Set one or [more LMDB flags](http://www.lmdb.tech/doc/group__mdb__env.html).
    ///
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use heed3::{EnvOpenOptions, Database, EnvFlags};
    /// use heed3::types::*;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut env_builder = EnvOpenOptions::new();
    /// unsafe { env_builder.flags(EnvFlags::NO_META_SYNC); }
    /// let dir = tempfile::tempdir().unwrap();
    /// let env = unsafe { env_builder.open(dir.path())? };
    ///
    /// // we will open the default unamed database
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, U32<byteorder::NativeEndian>> = env.create_database(&mut wtxn, None)?;
    ///
    /// // opening a write transaction
    /// db.put(&mut wtxn, "seven", &7)?;
    /// db.put(&mut wtxn, "zero", &0)?;
    /// db.put(&mut wtxn, "five", &5)?;
    /// db.put(&mut wtxn, "three", &3)?;
    /// wtxn.commit()?;
    ///
    /// // Force the OS to flush the buffers (see Flag::NoSync and Flag::NoMetaSync).
    /// env.force_sync();
    ///
    /// // opening a read transaction
    /// // to check if those values are now available
    /// let mut rtxn = env.read_txn()?;
    ///
    /// let ret = db.get(&rtxn, "zero")?;
    /// assert_eq!(ret, Some(0));
    ///
    /// let ret = db.get(&rtxn, "five")?;
    /// assert_eq!(ret, Some(5));
    /// # Ok(()) }
    /// ```
    ///
    /// # Safety
    ///
    /// It is unsafe to use unsafe LMDB flags such as `NO_SYNC`, `NO_META_SYNC`, or `NO_LOCK`.
    pub unsafe fn flags(&mut self, flags: EnvFlags) -> &mut Self {
        self.flags |= flags;
        self
    }

    /// Open an environment that will be located at the specified path.
    ///
    /// # Safety
    /// LMDB is backed by a memory map [^1] which comes with some safety precautions.
    ///
    /// Memory map constructors are marked `unsafe` because of the potential
    /// for Undefined Behavior (UB) using the map if the underlying file is
    /// subsequently modified, in or out of process.
    ///
    /// LMDB itself has a locking system that solves this problem,
    /// but it will not save you from making mistakes yourself.
    ///
    /// These are some things to take note of:
    ///
    /// - Avoid long-lived transactions, they will cause the database to grow quickly [^2]
    /// - Avoid aborting your process with an active transaction [^3]
    /// - Do not use LMDB on remote filesystems, even between processes on the same host [^4]
    /// - You must manage concurrent accesses yourself if using [`EnvFlags::NO_LOCK`] [^5]
    /// - Anything that causes LMDB's lock file to be broken will cause synchronization issues and may introduce UB [^6]
    ///
    /// `heed` itself upholds some safety invariants, including but not limited to:
    /// - Calling [`EnvOpenOptions::open`] twice in the same process, at the same time is OK [^7]
    ///
    /// For more details, it is highly recommended to read LMDB's official documentation. [^8]
    ///
    /// [^1]: <https://en.wikipedia.org/wiki/Memory_map>
    /// [^2]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L107-L114>
    /// [^3]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L118-L121>
    /// [^4]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    /// [^5]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    /// [^6]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L49-L52>
    /// [^7]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L102-L105>
    /// [^8]: <http://www.lmdb.tech/doc/index.html>
    pub unsafe fn open<P: AsRef<Path>>(&self, path: P) -> Result<Env<T>> {
        self.raw_open_with_encryption(
            path.as_ref(),
            #[cfg(master3)]
            None,
        )
    }

    /// Open an encrypted-at-rest environment that will be located at the specified path.
    ///
    /// # Safety
    /// LMDB is backed by a memory map [^1] which comes with some safety precautions.
    ///
    /// Memory map constructors are marked `unsafe` because of the potential
    /// for Undefined Behavior (UB) using the map if the underlying file is
    /// subsequently modified, in or out of process.
    ///
    /// LMDB itself has a locking system that solves this problem,
    /// but it will not save you from making mistakes yourself.
    ///
    /// These are some things to take note of:
    ///
    /// - Avoid long-lived transactions, they will cause the database to grow quickly [^2]
    /// - Avoid aborting your process with an active transaction [^3]
    /// - Do not use LMDB on remote filesystems, even between processes on the same host [^4]
    /// - You must manage concurrent accesses yourself if using [`EnvFlags::NO_LOCK`] [^5]
    /// - Anything that causes LMDB's lock file to be broken will cause synchronization issues and may introduce UB [^6]
    ///
    /// `heed` itself upholds some safety invariants, including but not limited to:
    /// - Calling [`EnvOpenOptions::open`] twice in the same process, at the same time is OK [^7]
    ///
    /// For more details, it is highly recommended to read LMDB's official documentation. [^8]
    ///
    /// # Basic Example
    ///
    /// Creates and open a database. The [`Env`] is encrypted-at-rest using the `E` algorithm with the
    /// given `key`. You can find more compatible algorithms on
    /// [the RustCrypto/AEADs page](https://github.com/RustCrypto/AEADs#crates).
    ///
    /// Note that you cannot use **any** type of encryption algorithm as LMDB exposes a nonce of 16 bytes.
    /// Heed makes sure to truncate it if necessary.
    ///
    /// As an example, XChaCha20 requires a 20 bytes long nonce. However, XChaCha20 is used to protect
    /// against nonce misuse in systems that use randomly generated nonces i.e., to protect against
    /// weak RNGs. There is no need to use this kind of algorithm in LMDB since LMDB nonces aren't
    /// random and are guaranteed to be unique.
    ///
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use argon2::Argon2;
    /// use chacha20poly1305::{ChaCha20Poly1305, Key};
    /// use heed3::types::*;
    /// use heed3::{EnvOpenOptions, Database};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let env_path = tempfile::tempdir()?;
    /// let password = "This is the password that will be hashed by the argon2 algorithm";
    /// let salt = "The salt added to the password hashes to add more security when stored";
    ///
    /// fs::create_dir_all(&env_path)?;
    ///
    /// let mut key = Key::default();
    /// Argon2::default().hash_password_into(password.as_bytes(), salt.as_bytes(), &mut key)?;
    ///
    /// // We open the environment
    /// let mut options = EnvOpenOptions::new();
    /// let env = unsafe {
    ///     options
    ///         .map_size(10 * 1024 * 1024) // 10MB
    ///         .max_dbs(3)
    ///         .open_encrypted::<ChaCha20Poly1305, _>(key, &env_path)?
    /// };
    ///
    /// let key1 = "first-key";
    /// let val1 = "this is a secret info";
    /// let key2 = "second-key";
    /// let val2 = "this is another secret info";
    ///
    /// // We create database and write secret values in it
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.create_database::<Str, Str>(&mut wtxn, Some("first"))?;
    /// db.put(&mut wtxn, key1, val1)?;
    /// db.put(&mut wtxn, key2, val2)?;
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Example Showing limitations
    ///
    /// At the end of this example file you can see that we can not longer use the `val1`
    /// variable as we performed a read in the database just after fetching it and keeping
    /// a reference to it.
    ///
    /// That's the main limitation of LMDB with the encryption-at-rest feature: entries cannot
    /// be kept for too long as they are kept in a cycling buffer when decrypting them on the fly.
    ///
    /// ```compile_fail,E0499
    /// use std::fs;
    /// use std::path::Path;
    /// use argon2::Argon2;
    /// use chacha20poly1305::{ChaCha20Poly1305, Key};
    /// use heed3::types::*;
    /// use heed3::{EnvOpenOptions, EncryptedDatabase;};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let env_path = tempfile::tempdir()?;
    /// let password = "This is the password that will be hashed by the argon2 algorithm";
    /// let salt = "The salt added to the password hashes to add more security when stored";
    ///
    /// fs::create_dir_all(&env_path)?;
    ///
    /// let mut key = Key::default();
    /// Argon2::default().hash_password_into(password.as_bytes(), salt.as_bytes(), &mut key)?;
    ///
    /// // We open the environment
    /// let mut options = EnvOpenOptions::new();
    /// let env = unsafe {
    ///     options
    ///         .map_size(10 * 1024 * 1024) // 10MB
    ///         .max_dbs(3)
    ///         .open_encrypted::<ChaCha20Poly1305, _>(key, &env_path)?
    /// };
    ///
    /// let key1 = "first-key";
    /// let key2 = "second-key";
    ///
    /// // We create the database
    /// let mut wtxn = env.write_txn()?;
    /// let db: EncryptedDatabase<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;
    /// wtxn.commit()?;
    ///
    /// // Declare the read transaction as mutable because LMDB, when using encryption,
    /// // does not allow keeping keys between reads due to the use of an internal cache.
    /// let mut rtxn = env.read_txn()?;
    /// let val1 = db.get(&mut rtxn, key1)?;
    /// let val2 = db.get(&mut rtxn, key2)?;
    ///
    /// // This example won't compile because val1 cannot be used
    /// // after we performed another read in the database (val2).
    /// let _force_keep = val1;
    /// # Ok(()) }
    /// ```
    ///
    /// [^1]: <https://en.wikipedia.org/wiki/Memory_map>
    /// [^2]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L107-L114>
    /// [^3]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L118-L121>
    /// [^4]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    /// [^5]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    /// [^6]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L49-L52>
    /// [^7]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L102-L105>
    /// [^8]: <http://www.lmdb.tech/doc/index.html>
    #[cfg(master3)]
    pub unsafe fn open_encrypted<E, P>(&self, key: Key<E>, path: P) -> Result<EncryptedEnv<T>>
    where
        E: AeadMutInPlace + KeyInit,
        P: AsRef<Path>,
    {
        self.raw_open_with_encryption(
            path.as_ref(),
            Some((Some(encrypt_func_wrapper::<E>), &key, <E as AeadCore>::TagSize::U32)),
        )
        .map(|inner| EncryptedEnv { inner })
    }

    fn raw_open_with_encryption(
        &self,
        path: &Path,
        #[cfg(master3)] enc: Option<(ffi::MDB_enc_func, &[u8], u32)>,
    ) -> Result<Env<T>> {
        let mut lock = OPENED_ENV.write().unwrap();

        let path = match canonicalize_path(path) {
            Err(err) => {
                if err.kind() == NotFound && self.flags.contains(EnvFlags::NO_SUB_DIR) {
                    match path.parent().zip(path.file_name()) {
                        Some((dir, file_name)) => canonicalize_path(dir)?.join(file_name),
                        None => return Err(err.into()),
                    }
                } else {
                    return Err(err.into());
                }
            }
            Ok(path) => path,
        };

        if lock.contains_key(&path) {
            Err(Error::EnvAlreadyOpened)
        } else {
            let path_str = CString::new(path.as_os_str().as_bytes()).unwrap();

            unsafe {
                let mut env: *mut ffi::MDB_env = ptr::null_mut();
                mdb_result(ffi::mdb_env_create(&mut env))?;

                #[cfg(master3)]
                if let Some((encrypt_func, key, tag_size)) = enc {
                    mdb_result(ffi::mdb_env_set_encrypt(
                        env,
                        encrypt_func,
                        &crate::into_val(key),
                        tag_size,
                    ))?;
                }

                if let Some(size) = self.map_size {
                    if size % page_size::get() != 0 {
                        let msg = format!(
                            "map size ({}) must be a multiple of the system page size ({})",
                            size,
                            page_size::get()
                        );
                        return Err(Error::Io(io::Error::new(io::ErrorKind::InvalidInput, msg)));
                    }
                    mdb_result(ffi::mdb_env_set_mapsize(env, size))?;
                }

                if let Some(readers) = self.max_readers {
                    mdb_result(ffi::mdb_env_set_maxreaders(env, readers))?;
                }

                if let Some(dbs) = self.max_dbs {
                    mdb_result(ffi::mdb_env_set_maxdbs(env, dbs))?;
                }

                // When the `<T as TlsUsage>::ENABLED` is true, we must tell
                // LMDB to avoid using the thread local storage, this way we
                // allow users to move RoTxn between threads safely.
                #[allow(deprecated)] // ok because NO_TLS is inside of the crate
                let flags = if T::ENABLED { self.flags } else { self.flags | EnvFlags::NO_TLS };

                let result = ffi::mdb_env_open(env, path_str.as_ptr(), flags.bits(), 0o600);
                match mdb_result(result) {
                    Ok(()) => {
                        let env_ptr = NonNull::new(env).unwrap();
                        let signal_event = Arc::new(SignalEvent::manual(false));
                        let inserted = lock.insert(path.clone(), signal_event.clone());
                        debug_assert!(inserted.is_none());
                        Ok(Env::new(env_ptr, path, signal_event))
                    }
                    Err(e) => {
                        ffi::mdb_env_close(env);
                        Err(e.into())
                    }
                }
            }
        }
    }
}

impl Default for EnvOpenOptions<WithTls> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: TlsUsage> Clone for EnvOpenOptions<T> {
    fn clone(&self) -> Self {
        let Self { map_size, max_readers, max_dbs, flags, _tls_marker } = *self;
        EnvOpenOptions { map_size, max_readers, max_dbs, flags, _tls_marker }
    }
}
