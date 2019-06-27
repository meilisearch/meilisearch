use lazy_static::lazy_static;
use levenshtein_automata::{
    LevenshteinAutomatonBuilder as LevBuilder,
    DFA,
};

lazy_static! {
    static ref LEVDIST0: LevBuilder = LevBuilder::new(0, false);
    static ref LEVDIST1: LevBuilder = LevBuilder::new(1, false);
    static ref LEVDIST2: LevBuilder = LevBuilder::new(2, false);
}

#[derive(Copy, Clone)]
enum PrefixSetting {
    Prefix,
    NoPrefix,
}

fn build_dfa_with_setting(query: &str, setting: PrefixSetting) -> DFA {
    use self::PrefixSetting::{Prefix, NoPrefix};

    match query.len() {
        0 ..= 4 => match setting {
            Prefix   => LEVDIST0.build_prefix_dfa(query),
            NoPrefix => LEVDIST0.build_dfa(query),
        },
        5 ..= 8 => match setting {
            Prefix   => LEVDIST1.build_prefix_dfa(query),
            NoPrefix => LEVDIST1.build_dfa(query),
        },
        _ => match setting {
            Prefix   => LEVDIST2.build_prefix_dfa(query),
            NoPrefix => LEVDIST2.build_dfa(query),
        },
    }
}

pub fn build_prefix_dfa(query: &str) -> DFA {
    build_dfa_with_setting(query, PrefixSetting::Prefix)
}

pub fn build_dfa(query: &str) -> DFA {
    build_dfa_with_setting(query, PrefixSetting::NoPrefix)
}
