use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use once_cell::sync::OnceCell;

static LEVDIST0: OnceCell<LevBuilder> = OnceCell::new();
static LEVDIST1: OnceCell<LevBuilder> = OnceCell::new();
static LEVDIST2: OnceCell<LevBuilder> = OnceCell::new();

#[derive(Copy, Clone)]
enum PrefixSetting {
    Prefix,
    NoPrefix,
}

fn build_dfa_with_setting(query: &str, setting: PrefixSetting) -> DFA {
    use PrefixSetting::{NoPrefix, Prefix};

    match query.len() {
        0..=4 => {
            let builder = LEVDIST0.get_or_init(|| LevBuilder::new(0, true));
            match setting {
                Prefix => builder.build_prefix_dfa(query),
                NoPrefix => builder.build_dfa(query),
            }
        }
        5..=8 => {
            let builder = LEVDIST1.get_or_init(|| LevBuilder::new(1, true));
            match setting {
                Prefix => builder.build_prefix_dfa(query),
                NoPrefix => builder.build_dfa(query),
            }
        }
        _ => {
            let builder = LEVDIST2.get_or_init(|| LevBuilder::new(2, true));
            match setting {
                Prefix => builder.build_prefix_dfa(query),
                NoPrefix => builder.build_dfa(query),
            }
        }
    }
}

pub fn build_prefix_dfa(query: &str) -> DFA {
    build_dfa_with_setting(query, PrefixSetting::Prefix)
}

pub fn build_dfa(query: &str) -> DFA {
    build_dfa_with_setting(query, PrefixSetting::NoPrefix)
}

pub fn build_exact_dfa(query: &str) -> DFA {
    let builder = LEVDIST0.get_or_init(|| LevBuilder::new(0, true));
    builder.build_dfa(query)
}
