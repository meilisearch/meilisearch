fn main() {
    if let Err(err) = emit_git_variables() {
        println!("cargo:warning=vergen: {}", err);
    }
}

fn emit_git_variables() -> anyhow::Result<()> {
    println!("cargo::rerun-if-env-changed=MEILI_NO_VERGEN");

    let has_vergen =
        !matches!(std::env::var_os("MEILI_NO_VERGEN"), Some(x) if x != "false" && x != "0");

    anyhow::ensure!(has_vergen, "disabled via `MEILI_NO_VERGEN`");

    // Note: any code that needs VERGEN_ environment variables should take care to define them manually in the Dockerfile and pass them
    // in the corresponding GitHub workflow (publish_docker.yml).
    // This is due to the Dockerfile building the binary outside of the git directory.
    let mut builder = vergen_git2::Git2Builder::default();

    builder.branch(true);
    builder.commit_timestamp(true);
    builder.commit_message(true);
    builder.describe(true, true, None);
    builder.sha(false);

    let git2 = builder.build()?;

    vergen_git2::Emitter::default().fail_on_error().add_instructions(&git2)?.emit()
}
