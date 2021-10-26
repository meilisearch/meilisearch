use std::{fs, path::Path};

/// Copy the `instance-uid` contained in one db to another. Ignore all errors.
pub fn copy_user_id(src: &Path, dst: &Path) {
    if let Ok(user_id) = fs::read_to_string(src.join("instance-uid")) {
        let _ = fs::write(dst.join("instance-uid"), &user_id);
    }
}
