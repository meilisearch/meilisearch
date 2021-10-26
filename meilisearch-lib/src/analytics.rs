use std::{fs, path::Path};

/// To load a dump we get the user id from the source directory;
/// If there was a user-id, write it to the new destination if not ignore the error
pub fn load_dump(src: &Path, dst: &Path) {
    if let Ok(user_id) = fs::read_to_string(src.join("user-id")) {
        let _ = fs::write(dst.join("user-id"), &user_id);
    }
}

/// To load a dump we get the user id either from the source directory;
/// If there was a user-id, write it to the new destination if not ignore the error
pub fn write_dump(src: &Path, dst: &Path) {
    if let Ok(user_id) = fs::read_to_string(src) {
        let _ = fs::write(dst, &user_id);
    }
}
