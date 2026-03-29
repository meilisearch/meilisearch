# Detect musl libc (e.g. Alpine Linux). On musl systems the glibc-built
# Meilisearch binaries downloaded by this script are not supported; fail
# early with an informative message.
detect_musl() {
    # If ldd exists, check its version output for 'musl'
    if command -v ldd >/dev/null 2>&1; then
        if ldd --version 2>&1 | grep -qi musl; then
            printf "$RED%s\n$DEFAULT" "$PNAME: Detected musl libc (e.g. Alpine). This script downloads glibc-built Meilisearch binaries which are incompatible with musl-based systems."
            echo "Please use a glibc-based distribution or download/compile a musl-compatible binary."
            exit 1
        fi
    fi

    # Extra heuristic: presence of musl dynamic loader
    if ls /lib/ld-musl* >/dev/null 2>&1 || ls /usr/lib/ld-musl* >/dev/null 2>&1; then
        printf "$RED%s\n$DEFAULT" "$PNAME: Detected musl libc (presence of /lib/ld-musl* or /usr/lib/ld-musl*). This script does not support musl-based systems."
        echo "Please use a glibc-based distribution or download/compile a musl-compatible binary."
        exit 1
    fi
}


