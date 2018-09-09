use std::ops::Deref;
use fst::Automaton;
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

impl Deref for DfaExt {
    type Target = DFA;

    fn deref(&self) -> &Self::Target {
        &self.automaton
    }
}

pub fn build(query: &str) -> DfaExt {
    let dfa = match query.len() {
        0 ..= 4 => LEVDIST0.build_prefix_dfa(query),
        5 ..= 8 => LEVDIST1.build_prefix_dfa(query),
        _       => LEVDIST2.build_prefix_dfa(query),
    };

    DfaExt { query_len: query.len(), automaton: dfa }
}

pub trait AutomatonExt: Automaton {
    fn eval<B: AsRef<[u8]>>(&self, s: B) -> Distance;
    fn query_len(&self) -> usize;
}

impl AutomatonExt for DfaExt {
    fn eval<B: AsRef<[u8]>>(&self, s: B) -> Distance {
        self.automaton.eval(s)
    }

    fn query_len(&self) -> usize {
        self.query_len
    }
}
