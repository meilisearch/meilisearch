fn main() {
    if let Err(err) = emit_git_variables() {
        println!("cargo:warning=vergen: {}", err);
    }
}

fn emit_git_variables() -> anyhow::Result<()> {
    // Note: any code that needs VERGEN_ environment variables should take care to define them manually in the Dockerfile and pass them
    // in the corresponding GitHub workflow (publish_docker.yml).
    // This is due to the Dockerfile building the binary outside of the git directory.
    let mut builder = vergen_gitcl::GitclBuilder::default();

    builder.branch(true);
    builder.commit_timestamp(true);
    builder.commit_message(true);
    builder.describe(true, true, None);
    builder.sha(false);

    let gitcl = builder.build()?;
    vergen_gitcl::Emitter::default().fail_on_error().add_instructions(&gitcl)?.emit()
}
