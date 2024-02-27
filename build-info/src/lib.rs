use time::format_description::well_known::Iso8601;

#[derive(Debug, Clone)]
pub struct BuildInfo {
    pub branch: Option<&'static str>,
    pub describe: Option<DescribeResult>,
    pub commit_sha1: Option<&'static str>,
    pub commit_msg: Option<&'static str>,
    pub commit_timestamp: Option<time::OffsetDateTime>,
}

impl BuildInfo {
    pub fn from_build() -> Self {
        let branch: Option<&'static str> = option_env!("VERGEN_GIT_BRANCH");
        let describe = DescribeResult::from_build();
        let commit_sha1 = option_env!("VERGEN_GIT_SHA");
        let commit_msg = option_env!("VERGEN_GIT_COMMIT_MESSAGE");
        let commit_timestamp = option_env!("VERGEN_GIT_COMMIT_TIMESTAMP");

        let commit_timestamp = commit_timestamp.and_then(|commit_timestamp| {
            time::OffsetDateTime::parse(commit_timestamp, &Iso8601::DEFAULT).ok()
        });

        Self { branch, describe, commit_sha1, commit_msg, commit_timestamp }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DescribeResult {
    Prototype { name: &'static str },
    Release { version: &'static str, major: u64, minor: u64, patch: u64 },
    Prerelease { version: &'static str, major: u64, minor: u64, patch: u64, rc: u64 },
    NotATag { describe: &'static str },
}

impl DescribeResult {
    pub fn new(describe: &'static str) -> Self {
        if let Some(name) = prototype_name(describe) {
            Self::Prototype { name }
        } else if let Some(release) = release_version(describe) {
            release
        } else if let Some(prerelease) = prerelease_version(describe) {
            prerelease
        } else {
            Self::NotATag { describe }
        }
    }

    pub fn from_build() -> Option<Self> {
        let describe: &'static str = option_env!("VERGEN_GIT_DESCRIBE")?;
        Some(Self::new(describe))
    }

    pub fn as_tag(&self) -> Option<&'static str> {
        match self {
            DescribeResult::Prototype { name } => Some(name),
            DescribeResult::Release { version, .. } => Some(version),
            DescribeResult::Prerelease { version, .. } => Some(version),
            DescribeResult::NotATag { describe: _ } => None,
        }
    }

    pub fn as_prototype(&self) -> Option<&'static str> {
        match self {
            DescribeResult::Prototype { name } => Some(name),
            DescribeResult::Release { .. }
            | DescribeResult::Prerelease { .. }
            | DescribeResult::NotATag { .. } => None,
        }
    }
}

/// Parses the input as a prototype name.
///
/// Returns `Some(prototype_name)` if the following conditions are met on this value:
///
/// 1. starts with `prototype-`,
/// 2. ends with `-<some_number>`,
/// 3. does not end with `<some_number>-<some_number>`.
///
/// Otherwise, returns `None`.
fn prototype_name(describe: &'static str) -> Option<&'static str> {
    if !describe.starts_with("prototype-") {
        return None;
    }

    let mut rsplit_prototype = describe.rsplit('-');
    // last component MUST be a number
    rsplit_prototype.next()?.parse::<u64>().ok()?;
    // before than last component SHALL NOT be a number
    rsplit_prototype.next()?.parse::<u64>().err()?;

    Some(describe)
}

fn release_version(describe: &'static str) -> Option<DescribeResult> {
    if !describe.starts_with('v') {
        return None;
    }

    // full release version don't contain a `-`
    if describe.contains('-') {
        return None;
    }

    // full release version parse as vX.Y.Z, with X, Y, Z numbers.
    let mut dots = describe[1..].split('.');
    let major: u64 = dots.next()?.parse().ok()?;
    let minor: u64 = dots.next()?.parse().ok()?;
    let patch: u64 = dots.next()?.parse().ok()?;

    if dots.next().is_some() {
        return None;
    }

    Some(DescribeResult::Release { version: describe, major, minor, patch })
}

fn prerelease_version(describe: &'static str) -> Option<DescribeResult> {
    // prerelease version is in the shape vM.N.P-rc.C
    let mut hyphen = describe.rsplit('-');
    let prerelease = hyphen.next()?;
    if !prerelease.starts_with("rc.") {
        return None;
    }

    let rc: u64 = prerelease[3..].parse().ok()?;

    let release = hyphen.next()?;

    let DescribeResult::Release { version: _, major, minor, patch } = release_version(release)?
    else {
        return None;
    };

    Some(DescribeResult::Prerelease { version: describe, major, minor, patch, rc })
}

#[cfg(test)]
mod test {
    use super::DescribeResult;

    fn assert_not_a_tag(describe: &'static str) {
        assert_eq!(DescribeResult::NotATag { describe }, DescribeResult::new(describe))
    }

    fn assert_proto(describe: &'static str) {
        assert_eq!(DescribeResult::Prototype { name: describe }, DescribeResult::new(describe))
    }

    fn assert_release(describe: &'static str, major: u64, minor: u64, patch: u64) {
        assert_eq!(
            DescribeResult::Release { version: describe, major, minor, patch },
            DescribeResult::new(describe)
        )
    }

    fn assert_prerelease(describe: &'static str, major: u64, minor: u64, patch: u64, rc: u64) {
        assert_eq!(
            DescribeResult::Prerelease { version: describe, major, minor, patch, rc },
            DescribeResult::new(describe)
        )
    }

    #[test]
    fn not_a_tag() {
        assert_not_a_tag("whatever-fuzzy");
        assert_not_a_tag("whatever-fuzzy-5-ggg-dirty");
        assert_not_a_tag("whatever-fuzzy-120-ggg-dirty");

        // technically a tag, but not a proto nor a version, so not parsed as a tag
        assert_not_a_tag("whatever");

        // dirty version
        assert_not_a_tag("v1.7.0-1-ggga-dirty");
        assert_not_a_tag("v1.7.0-rc.1-1-ggga-dirty");

        // after version
        assert_not_a_tag("v1.7.0-1-ggga");
        assert_not_a_tag("v1.7.0-rc.1-1-ggga");

        // after proto
        assert_not_a_tag("protoype-tag-0-1-ggga");
        assert_not_a_tag("protoype-tag-0-1-ggga-dirty");
    }

    #[test]
    fn prototype() {
        assert_proto("prototype-tag-0");
        assert_proto("prototype-tag-10");
        assert_proto("prototype-long-name-tag-10");
    }

    #[test]
    fn release() {
        assert_release("v1.7.2", 1, 7, 2);
    }

    #[test]
    fn prerelease() {
        assert_prerelease("v1.7.2-rc.3", 1, 7, 2, 3);
    }
}
