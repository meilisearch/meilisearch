pub mod v1;
pub mod v2;
pub mod v3;

mod compat {
    /// Parses the v1 version of the Asc ranking rules `asc(price)`and returns the field name.
    pub fn asc_ranking_rule(text: &str) -> Option<&str> {
        text.split_once("asc(")
            .and_then(|(_, tail)| tail.rsplit_once(")"))
            .map(|(field, _)| field)
    }

    /// Parses the v1 version of the Desc ranking rules `desc(price)`and returns the field name.
    pub fn desc_ranking_rule(text: &str) -> Option<&str> {
        text.split_once("desc(")
            .and_then(|(_, tail)| tail.rsplit_once(")"))
            .map(|(field, _)| field)
    }
}
