/// This mod is necessary until https://github.com/BurntSushi/fst/pull/137 gets merged.
/// All credits for this code go to BurntSushi.
use fst::Automaton;

pub struct StartsWith<A>(pub A);

/// The `Automaton` state for `StartsWith<A>`.
pub struct StartsWithState<A: Automaton>(pub StartsWithStateKind<A>);

impl<A: Automaton> Clone for StartsWithState<A>
where
    A::State: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// The inner state of a `StartsWithState<A>`.
pub enum StartsWithStateKind<A: Automaton> {
    /// Sink state that is reached when the automaton has matched the prefix.
    Done,
    /// State in which the automaton is while it hasn't matched the prefix.
    Running(A::State),
}

impl<A: Automaton> Clone for StartsWithStateKind<A>
where
    A::State: Clone,
{
    fn clone(&self) -> Self {
        match self {
            StartsWithStateKind::Done => StartsWithStateKind::Done,
            StartsWithStateKind::Running(inner) => StartsWithStateKind::Running(inner.clone()),
        }
    }
}

impl<A: Automaton> Automaton for StartsWith<A> {
    type State = StartsWithState<A>;

    fn start(&self) -> StartsWithState<A> {
        StartsWithState({
            let inner = self.0.start();
            if self.0.is_match(&inner) {
                StartsWithStateKind::Done
            } else {
                StartsWithStateKind::Running(inner)
            }
        })
    }
    fn is_match(&self, state: &StartsWithState<A>) -> bool {
        match state.0 {
            StartsWithStateKind::Done => true,
            StartsWithStateKind::Running(_) => false,
        }
    }
    fn can_match(&self, state: &StartsWithState<A>) -> bool {
        match state.0 {
            StartsWithStateKind::Done => true,
            StartsWithStateKind::Running(ref inner) => self.0.can_match(inner),
        }
    }
    fn will_always_match(&self, state: &StartsWithState<A>) -> bool {
        match state.0 {
            StartsWithStateKind::Done => true,
            StartsWithStateKind::Running(_) => false,
        }
    }
    fn accept(&self, state: &StartsWithState<A>, byte: u8) -> StartsWithState<A> {
        StartsWithState(match state.0 {
            StartsWithStateKind::Done => StartsWithStateKind::Done,
            StartsWithStateKind::Running(ref inner) => {
                let next_inner = self.0.accept(inner, byte);
                if self.0.is_match(&next_inner) {
                    StartsWithStateKind::Done
                } else {
                    StartsWithStateKind::Running(next_inner)
                }
            }
        })
    }
}
/// An automaton that matches when one of its component automata match.
#[derive(Clone, Debug)]
pub struct Union<A, B>(pub A, pub B);

/// The `Automaton` state for `Union<A, B>`.
pub struct UnionState<A: Automaton, B: Automaton>(pub A::State, pub B::State);

impl<A: Automaton, B: Automaton> Clone for UnionState<A, B>
where
    A::State: Clone,
    B::State: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<A: Automaton, B: Automaton> Automaton for Union<A, B> {
    type State = UnionState<A, B>;
    fn start(&self) -> UnionState<A, B> {
        UnionState(self.0.start(), self.1.start())
    }
    fn is_match(&self, state: &UnionState<A, B>) -> bool {
        self.0.is_match(&state.0) || self.1.is_match(&state.1)
    }
    fn can_match(&self, state: &UnionState<A, B>) -> bool {
        self.0.can_match(&state.0) || self.1.can_match(&state.1)
    }
    fn will_always_match(&self, state: &UnionState<A, B>) -> bool {
        self.0.will_always_match(&state.0) || self.1.will_always_match(&state.1)
    }
    fn accept(&self, state: &UnionState<A, B>, byte: u8) -> UnionState<A, B> {
        UnionState(self.0.accept(&state.0, byte), self.1.accept(&state.1, byte))
    }
}
/// An automaton that matches when both of its component automata match.
#[derive(Clone, Debug)]
pub struct Intersection<A, B>(pub A, pub B);

/// The `Automaton` state for `Intersection<A, B>`.
pub struct IntersectionState<A: Automaton, B: Automaton>(pub A::State, pub B::State);

impl<A: Automaton, B: Automaton> Clone for IntersectionState<A, B>
where
    A::State: Clone,
    B::State: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<A: Automaton, B: Automaton> Automaton for Intersection<A, B> {
    type State = IntersectionState<A, B>;
    fn start(&self) -> IntersectionState<A, B> {
        IntersectionState(self.0.start(), self.1.start())
    }
    fn is_match(&self, state: &IntersectionState<A, B>) -> bool {
        self.0.is_match(&state.0) && self.1.is_match(&state.1)
    }
    fn can_match(&self, state: &IntersectionState<A, B>) -> bool {
        self.0.can_match(&state.0) && self.1.can_match(&state.1)
    }
    fn will_always_match(&self, state: &IntersectionState<A, B>) -> bool {
        self.0.will_always_match(&state.0) && self.1.will_always_match(&state.1)
    }
    fn accept(&self, state: &IntersectionState<A, B>, byte: u8) -> IntersectionState<A, B> {
        IntersectionState(self.0.accept(&state.0, byte), self.1.accept(&state.1, byte))
    }
}
/// An automaton that matches exactly when the automaton it wraps does not.
#[derive(Clone, Debug)]
pub struct Complement<A>(pub A);

/// The `Automaton` state for `Complement<A>`.
pub struct ComplementState<A: Automaton>(pub A::State);

impl<A: Automaton> Clone for ComplementState<A>
where
    A::State: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: Automaton> Automaton for Complement<A> {
    type State = ComplementState<A>;
    fn start(&self) -> ComplementState<A> {
        ComplementState(self.0.start())
    }
    fn is_match(&self, state: &ComplementState<A>) -> bool {
        !self.0.is_match(&state.0)
    }
    fn can_match(&self, state: &ComplementState<A>) -> bool {
        !self.0.will_always_match(&state.0)
    }
    fn will_always_match(&self, state: &ComplementState<A>) -> bool {
        !self.0.can_match(&state.0)
    }
    fn accept(&self, state: &ComplementState<A>, byte: u8) -> ComplementState<A> {
        ComplementState(self.0.accept(&state.0, byte))
    }
}
