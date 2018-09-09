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
