use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub use insta;
use once_cell::sync::Lazy;

static SNAPSHOT_NAMES: Lazy<Mutex<HashMap<PathBuf, usize>>> = Lazy::new(Mutex::default);

/// Return the md5 hash of the given string
pub fn hash_snapshot(snap: &str) -> String {
    let hash = md5::compute(snap.as_bytes());
    let hash_str = format!("{hash:x}");
    hash_str
}

#[track_caller]
pub fn default_snapshot_settings_for_test<'a>(
    test_name: &str,
    name: Option<&'a str>,
) -> (insta::Settings, Cow<'a, str>, bool) {
    let mut settings = insta::Settings::clone_current();
    settings.set_prepend_module_to_snapshot(false);
    let path = Path::new(std::panic::Location::caller().file());
    let filename = path.file_name().unwrap().to_str().unwrap();
    settings.set_omit_expression(true);

    let test_name = test_name.strip_suffix("::{{closure}}").unwrap_or(test_name);
    let test_name = test_name.rsplit("::").next().unwrap().to_owned();

    let path = Path::new("snapshots").join(filename).join(test_name);
    settings.set_snapshot_path(path.clone());
    let snap_name = if let Some(name) = name {
        Cow::Borrowed(name)
    } else {
        let mut snapshot_names = SNAPSHOT_NAMES.lock().unwrap();
        let counter = snapshot_names.entry(path).or_default();
        *counter += 1;
        Cow::Owned(format!("{counter}"))
    };

    let store_whole_snapshot =
        std::env::var("MEILI_TEST_FULL_SNAPS").unwrap_or_else(|_| "false".to_owned());
    let store_whole_snapshot: bool = store_whole_snapshot.parse().unwrap();

    (settings, snap_name, store_whole_snapshot)
}

/**
Create a hashed snapshot test.

## Arguments:

1. The content of the snapshot. It is an expression whose result implements the `fmt::Display` trait.
2. `name: <name>`: the identifier for the snapshot test (optional)
3. `@""` to write the hash of the snapshot inline

## Behaviour
The content of the snapshot will be saved both in full and as a hash. The full snapshot will
be saved with the name `<name>.full.snap` but will not be saved to the git repository. The hashed
snapshot will be saved inline. If `<name>` is not specified, then a global counter is used to give an
identifier to the snapshot.

Running `cargo test` will check whether the old snapshot is identical to the
current one. If they are equal, the test passes. Otherwise, the test fails.

Use the command line `cargo insta` to approve or reject new snapshots.

## Example
```ignore
// The full snapshot is saved under 1.full.snap and contains `10`
snapshot_hash!(10, @"d3d9446802a44259755d38e6d163e820");
// The full snapshot is saved under snap_name.full.snap and contains `hello world`
snapshot_hash!("hello world", name: "snap_name", @"5f93f983524def3dca464469d2cf9f3e");
```
*/
#[macro_export]
macro_rules! snapshot_hash {
    ($value:expr, @$inline:literal) => {
        let test_name = {
            fn f() {}
            fn type_name_of_val<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            type_name_of_val(f).strip_suffix("::f").unwrap_or("")
        };
        let test_name = test_name
            .strip_suffix("::{{closure}}")
            .unwrap_or(test_name);

        let (settings, snap_name, store_whole_snapshot) = $crate::default_snapshot_settings_for_test(test_name, None);
        settings.bind(|| {
            let snap = format!("{}", $value);
            let hash_snap = $crate::hash_snapshot(&snap);
            meili_snap::insta::assert_snapshot!(hash_snap, @$inline);
            if store_whole_snapshot {
                meili_snap::insta::assert_snapshot!(format!("{}.full", snap_name), snap);
            }
        });
    };
    ($value:expr, name: $name:expr, @$inline:literal) => {
        let test_name = {
            fn f() {}
            fn type_name_of_val<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            type_name_of_val(f).strip_suffix("::f").unwrap_or("")
        };

        let snap_name = format!("{}", $name);
        let (settings, snap_name, store_whole_snapshot) = $crate::default_snapshot_settings_for_test(test_name, Some(&snap_name));
        settings.bind(|| {
            let snap = format!("{}", $value);
            let hash_snap = $crate::hash_snapshot(&snap);
            meili_snap::insta::assert_snapshot!(hash_snap, @$inline);
            if store_whole_snapshot {
                meili_snap::insta::assert_snapshot!(format!("{}.full", snap_name), snap);
            }
        });
    };
}

/**
Create a hashed snapshot test.

## Arguments:
1. The content of the snapshot. It is an expression whose result implements the `fmt::Display` trait.
2. Optionally one of:
    1. `name: <name>`: the identifier for the snapshot test
    2. `@""` to write the hash of the snapshot inline

## Behaviour
The content of the snapshot will be saved in full with the given name
or using a global counter to give it an identifier.

Running `cargo test` will check whether the old snapshot is identical to the
current one. If they are equal, the test passes. Otherwise, the test fails.

Use the command line `cargo insta` to approve or reject new snapshots.

## Example
```ignore
// The full snapshot is saved under 1.snap and contains `10`
snapshot!(10);
// The full snapshot is saved under snap_name.snap and contains `10`
snapshot!("hello world", name: "snap_name");
// The full snapshot is saved inline
snapshot!(format!("{:?}", vec![1, 2]), @"[1, 2]");
```
*/
#[macro_export]
macro_rules! snapshot {
    ($value:expr, name: $name:expr) => {
        let test_name = {
            fn f() {}
            fn type_name_of_val<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            type_name_of_val(f).strip_suffix("::f").unwrap_or("")
        };
        let test_name = test_name
            .strip_suffix("::{{closure}}")
            .unwrap_or(test_name);

        let snap_name = format!("{}", $name);
        let (settings, snap_name, _) = $crate::default_snapshot_settings_for_test(test_name, Some(&snap_name));
        settings.bind(|| {
            let snap = format!("{}", $value);
            insta::allow_duplicates! {
                meili_snap::insta::assert_snapshot!(format!("{}", snap_name), snap);
            }
        });
    };
    ($value:expr, @$inline:literal) => {
        // Note that the name given as argument does not matter since it is only an inline snapshot
        // We don't pass None because otherwise `meili-snap` will try to assign it a unique identifier
        let (settings, _, _) = $crate::default_snapshot_settings_for_test("", Some("_dummy_argument"));
        settings.bind(|| {
            let snap = format!("{}", $value);
            insta::allow_duplicates! {
                meili_snap::insta::assert_snapshot!(snap, @$inline);
            }
        });
    };
    ($value:expr) => {
        let test_name = {
            fn f() {}
            fn type_name_of_val<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            type_name_of_val(f).strip_suffix("::f").unwrap_or("")
        };
        let test_name = test_name
            .strip_suffix("::{{closure}}")
            .unwrap_or(test_name);

        let (settings, snap_name, _) = $crate::default_snapshot_settings_for_test(test_name, None);
        settings.bind(|| {
            let snap = format!("{}", $value);
            insta::allow_duplicates! {
                meili_snap::insta::assert_snapshot!(format!("{}", snap_name), snap);
            }
        });
    };
}

/// Create a string from the value by serializing it as Json, optionally
/// redacting some parts of it.
///
/// The second argument to the macro can be an object expression for redaction.
/// It's in the form { selector => replacement }. For more information about redactions
/// refer to the redactions feature in the `insta` guide.
#[macro_export]
macro_rules! json_string {
    ($value:expr, {$($k:expr => $v:expr),*$(,)?}) => {
        {
            let (_, snap) = meili_snap::insta::_prepare_snapshot_for_redaction!($value, {$($k => $v),*}, Json, File);
            snap
        }
    };
    ($value:expr) => {{
        let value = meili_snap::insta::_macro_support::serialize_value(
            &$value,
            meili_snap::insta::_macro_support::SerializationFormat::Json,
            meili_snap::insta::_macro_support::SnapshotLocation::File
        );
        value
    }};
}

#[cfg(test)]
mod tests {
    use crate as meili_snap;
    #[test]
    fn snap() {
        snapshot_hash!(10, @"d3d9446802a44259755d38e6d163e820");
        snapshot_hash!(20, @"98f13708210194c475687be6106a3b84");
        snapshot_hash!(30, @"34173cb38f07f89ddbebc2ac9128303f");

        snapshot!(40, @"40");
        snapshot!(50, @"50");
        snapshot!(60, @"60");

        snapshot!(70);
        snapshot!(80);
        snapshot!(90);

        snapshot!(100, name: "snap_name_1");
        snapshot_hash!(110, name: "snap_name_2", @"5f93f983524def3dca464469d2cf9f3e");

        snapshot!(120);
        snapshot!(format!("{:?}", vec![1, 2]), @"[1, 2]");
    }

    // Currently the name of this module is not part of the snapshot path
    // It does not bother me, but maybe it is worth changing later on.
    mod snap {
        use crate as meili_snap;
        #[test]
        fn some_test() {
            snapshot_hash!(10, @"d3d9446802a44259755d38e6d163e820");
            snapshot_hash!(20, @"98f13708210194c475687be6106a3b84");
            snapshot_hash!(30, @"34173cb38f07f89ddbebc2ac9128303f");

            snapshot!(40, @"40");
            snapshot!(50, @"50");
            snapshot!(60, @"60");

            snapshot!(70);
            snapshot!(80);
            snapshot!(90);

            snapshot!(100, name: "snap_name_1");
            snapshot_hash!(110, name: "snap_name_2", @"5f93f983524def3dca464469d2cf9f3e");

            snapshot!(120);

            // snapshot_hash!("", name: "", @"d41d8cd98f00b204e9800998ecf8427e");
        }
    }
}
