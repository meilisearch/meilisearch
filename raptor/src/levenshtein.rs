use levenshtein_automata::{LevenshteinAutomatonBuilder, DFA};

pub struct LevBuilder {
    automatons: [LevenshteinAutomatonBuilder; 3],
}

impl LevBuilder {
    pub fn new() -> Self {
        Self {
            automatons: [
                LevenshteinAutomatonBuilder::new(0, false),
                LevenshteinAutomatonBuilder::new(1, false),
                LevenshteinAutomatonBuilder::new(2, false),
            ],
        }
    }

    pub fn get_automaton(&self, query: &str) -> Levenshtein {
        assert!(!query.is_empty());

        let dfa = if query.len() <= 4 {
            self.automatons[0].build_prefix_dfa(query)
        } else if query.len() <= 8 {
            self.automatons[1].build_prefix_dfa(query)
        } else {
            self.automatons[2].build_prefix_dfa(query)
        };

        Levenshtein { dfa, query_len: query.len() }
    }
}

#[derive(Clone)]
pub struct Levenshtein {
    pub dfa: DFA,
    pub query_len: usize,
}
