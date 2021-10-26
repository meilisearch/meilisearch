use std::{fs, path::Path};

/// Copy the `user-id` contained in one db to another. Ignore all errors.
pub fn copy_user_id(src: &Path, dst: &Path) {
    if let Ok(user_id) = fs::read_to_string(src.join("user-id")) {
        let _ = fs::write(dst.join("user-id"), &user_id);
    }
}
