//! Some specific fst automatons. Learn more about automatons [in the fst documentation][1].
//!
//! The distances accepted follows these rules,
//! if the string length used to create the automaton is:
//!  - `0 ≤ x ≤ 4` then no typo is accepted
//!  - `5 ≤ x ≤ 8` then one typo is accepted
//!  - `8 ≤ x`     then two typos are accepted
//!
//! [1]: https://docs.rs/fst/0.3.3/fst/automaton/index.html

pub use fst::Automaton;
pub use levenshtein_automata::Distance;

use lazy_static::lazy_static;
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};

lazy_static! {
    static ref LEVDIST0: LevBuilder = LevBuilder::new(0, false);
    static ref LEVDIST1: LevBuilder = LevBuilder::new(1, false);
    static ref LEVDIST2: LevBuilder = LevBuilder::new(2, false);
}

/// An automaton that gives more informations about the string used to build it.
///
/// It is based on the [levenshtein automaton][1] used in Tantivy.
///
/// [1]: https://github.com/tantivy-search/levenshtein-automata
///
/// # Examples
///
/// ```rust
/// use meilidb::automaton::*;
///
/// let dfa = build_prefix_dfa("hello");
/// assert_eq!(dfa.query_len(),   5);
///
/// assert_eq!(dfa.eval("hello").to_u8(), 0);
/// assert_eq!(dfa.eval("he").to_u8(),    2);
/// assert_eq!(dfa.eval("hallo").to_u8(), 1);
///
/// let dfa = build_dfa("hello");
/// assert_eq!(dfa.query_len(),   5);
///
/// assert_eq!(dfa.eval("hello").to_u8(), 0);
/// assert_eq!(dfa.eval("he").to_u8(),    2);
/// assert_eq!(dfa.eval("hallo").to_u8(), 1);
/// ```
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

/// Create an automaton which accepts prefixes.
///
/// It means that a string that is a prefix of the string used
/// to create the automaton will match with a distance of zero.
///
/// For more informations about the distances accepted [see the module documentation][1].
///
/// [1]: index.html
pub fn build_prefix_dfa(query: &str) -> DfaExt {
    build_dfa_with_setting(query, PrefixSetting::Prefix)
}

/// Creates an automaton which **does not** accepts prefixes.
///
/// It means that a string that is a prefix of the string used
/// to create the automaton will not match.
///
/// For more informations about the distances accepted [see the module documentation][1].
///
/// [1]: index.html
pub fn build_dfa(query: &str) -> DfaExt {
    build_dfa_with_setting(query, PrefixSetting::NoPrefix)
}

/// An extension trait to the original one.
///
/// Used to wrap a levenshtein automaton.
pub trait AutomatonExt: Automaton {

    /// Allow the user to know the distance at which
    /// a query string watch with the creation string.
    fn eval<B: AsRef<[u8]>>(&self, s: B) -> Distance;

    /// Allow the user to know the original string length.
    fn query_len(&self) -> usize;
}
