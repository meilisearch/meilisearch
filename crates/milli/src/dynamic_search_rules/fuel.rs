use std::num::Saturating;
use std::ops::ControlFlow;

use filter_parser::FilterConstraintFuel;

/// A structure containing various limits meant to set an upperbound to DSR evaluation time.
#[derive(Debug, Clone, Copy)]
pub struct DsrFuel {
    max_counted_words: u8,
    max_active_rules: u32,
    max_pin_actions: u32,
    max_scale_actions: u8,
    remaining_word_fuel: Saturating<u32>,
    remaining_filter_fuel: Saturating<u32>,
    remaining_scale_fuel: Saturating<u8>,
    pub(super) filter_constraint_fuel: FilterConstraintFuel,
}

impl DsrFuel {
    /// Create a DSR Fuel structure from its parameters
    ///
    /// - `max_counted_words`: maximum number of words that are considered in a query
    /// - `max_active_rules`: maximum number of active rules whose actions are examined
    /// - `max_pin_actions`: maximum number of applicable pin actions that are examined
    /// - `max_scale_actions`: maximum number of applicable scale actions that are examined
    /// - `word_fuel`: maximum number of constraint combinations that are evaluated during query constraint resolution.
    /// - `filter_fuel`: maximum number of constraint combinations that are evaluated during filter constraint resolution.
    /// - `scale_fuel`: maximum number of constraint combinations that are evaluated during scale action application.
    /// - `filter_constraint_fuel`: maximum number of operations that are processed while turning a filter into a set of filter constraints.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_counted_words: u8,
        max_active_rules: u32,
        max_pin_actions: u32,
        max_scale_actions: u8,
        word_fuel: u32,
        filter_fuel: u32,
        scale_fuel: u8,
        filter_constraint_fuel: FilterConstraintFuel,
    ) -> Self {
        Self {
            max_counted_words,
            max_active_rules,
            max_pin_actions,
            max_scale_actions,
            remaining_word_fuel: Saturating(word_fuel),
            remaining_filter_fuel: Saturating(filter_fuel),
            remaining_scale_fuel: Saturating(scale_fuel),
            filter_constraint_fuel,
        }
    }

    /// maximum number of words that are considered in a query
    pub fn max_counted_words(&self) -> usize {
        self.max_counted_words.into()
    }

    /// Consumes a word combination.
    ///
    /// Returns `Break` is no fuel remains, otherwise `Continue`.
    pub fn consume_word_combination(&mut self) -> ControlFlow<(), ()> {
        self.remaining_word_fuel -= 1;
        if self.remaining_word_fuel.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    /// Consumes a filter combination.
    ///
    /// Returns `Break` if no fuel remains, otherwise `Continue`.
    pub fn consume_filter_combination(&mut self) -> ControlFlow<(), ()> {
        self.remaining_filter_fuel -= 1;
        if self.remaining_filter_fuel.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    /// Consumes a scale combination.
    ///
    /// Returns `Break` if no fuel remains, otherwise `Continue`.
    pub fn consume_scale_combination(&mut self) -> ControlFlow<(), ()> {
        self.remaining_scale_fuel -= 1;
        if self.remaining_scale_fuel.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    /// maximum number of active rules whose actions are examined
    pub fn max_active_rules(&self) -> usize {
        self.max_active_rules as usize
    }

    /// maximum number of applicable pin actions that are examined
    pub fn max_pin_actions(&self) -> usize {
        self.max_pin_actions as usize
    }

    /// maximum number of constraint combinations that are evaluated during scale action application.
    pub fn max_scale_actions(&self) -> usize {
        self.max_scale_actions as usize
    }
}
