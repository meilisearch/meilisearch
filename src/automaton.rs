use std::ops::Deref;

use fst::Automaton;
use lazy_static::lazy_static;
use levenshtein_automata::{
    LevenshteinAutomatonBuilder as LevBuilder,
    DFA, Distance,
};

lazy_static! {
    static ref LEVDIST0: LevBuilder = LevBuilder::new(0, false);
    static ref LEVDIST1: LevBuilder = LevBuilder::new(1, false);
    static ref LEVDIST2: LevBuilder = LevBuilder::new(2, false);
}

pub struct DfaExt {
    query_len: usize,
    automaton: DFA,
}

impl Automaton for DfaExt {
    type State = <DFA as Automaton>::State;

    fn start(&self) -> Self::State {
        self.automaton.start()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        self.automaton.is_match(state)
    }

    fn can_match(&self, state: &Self::State) -> bool {
        self.automaton.can_match(state)
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        self.automaton.will_always_match(state)
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.automaton.accept(state, byte)
    }
}

impl AutomatonExt for DfaExt {
    fn eval<B: AsRef<[u8]>>(&self, s: B) -> Distance {
        self.automaton.eval(s)
    }

    fn query_len(&self) -> usize {
        self.query_len
    }
}

enum PrefixSetting {
    Prefix,
    NoPrefix,
}

fn build_dfa_with_setting(query: &str, setting: PrefixSetting) -> DfaExt {
    use self::PrefixSetting::{Prefix, NoPrefix};

    let dfa = match query.len() {
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
    };

    DfaExt { query_len: query.len(), automaton: dfa }
}

pub fn build_prefix_dfa(query: &str) -> DfaExt {
    build_dfa_with_setting(query, PrefixSetting::Prefix)
}

pub fn build_dfa(query: &str) -> DfaExt {
    build_dfa_with_setting(query, PrefixSetting::NoPrefix)
}

pub trait AutomatonExt: Automaton {
    fn eval<B: AsRef<[u8]>>(&self, s: B) -> Distance;
    fn query_len(&self) -> usize;
}

impl<T> AutomatonExt for T
where T: Deref,
      T::Target: AutomatonExt,
{
    fn eval<B: AsRef<[u8]>>(&self, s: B) -> Distance {
        (**self).eval(s)
    }

    fn query_len(&self) -> usize {
        (**self).query_len()
    }
}
