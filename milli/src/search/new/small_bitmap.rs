// #[macro_export]
// macro_rules! iter_bitmap {
//     ($bitmap:expr, $id:lifetime, $p:pat, $body:block) => {
//         match $bitmap {
//             SmallBitmap::Tiny(mut set) => {
//                 while set > 0 {
//                     let $p = set.trailing_zeros() as u16;
//                     $body;
//                     set &= set - 1;
//                 }
//             }
//             SmallBitmap::Small(sets) => {
//                 let mut base = 0;
//                 for set in sets.iter() {
//                     let mut set = *set;
//                     while set > 0 {
//                         let idx = set.trailing_zeros() as u16;
//                         let $p = idx + base;
//                         set &= set - 1;
//                         $body;
//                     }
//                     base += 64;
//                 }
//             }
//         }
//     };
// }

#[derive(Clone)]
pub enum SmallBitmap {
    Tiny(u64),
    Small(Box<[u64]>),
}
impl SmallBitmap {
    pub fn new(universe_length: u16) -> Self {
        if universe_length <= 64 {
            Self::Tiny(0)
        } else {
            Self::Small(vec![0; 1 + universe_length as usize / 64].into_boxed_slice())
        }
    }
    pub fn from_iter(xs: impl Iterator<Item = u16>, universe_length: u16) -> Self {
        let mut s = Self::new(universe_length);
        for x in xs {
            s.insert(x);
        }
        s
    }
    pub fn from_array(xs: &[u16], universe_length: u16) -> Self {
        let mut s = Self::new(universe_length);
        for x in xs {
            s.insert(*x);
        }
        s
    }
    pub fn is_empty(&self) -> bool {
        match self {
            SmallBitmap::Tiny(set) => *set == 0,
            SmallBitmap::Small(sets) => {
                for set in sets.iter() {
                    if *set != 0 {
                        return false;
                    }
                }
                true
            }
        }
    }
    pub fn clear(&mut self) {
        match self {
            SmallBitmap::Tiny(set) => *set = 0,
            SmallBitmap::Small(sets) => {
                for set in sets.iter_mut() {
                    *set = 0;
                }
            }
        }
    }
    pub fn contains(&self, mut x: u16) -> bool {
        let set = match self {
            SmallBitmap::Tiny(set) => *set,
            SmallBitmap::Small(set) => {
                let idx = x / 64;
                x %= 64;
                set[idx as usize]
            }
        };
        set & 0b1 << x != 0
    }
    pub fn insert(&mut self, mut x: u16) {
        let set = match self {
            SmallBitmap::Tiny(set) => set,
            SmallBitmap::Small(set) => {
                let idx = x / 64;
                x %= 64;
                &mut set[idx as usize]
            }
        };
        *set |= 0b1 << x;
    }
    pub fn remove(&mut self, mut x: u16) {
        let set = match self {
            SmallBitmap::Tiny(set) => set,
            SmallBitmap::Small(set) => {
                let idx = x / 64;
                x %= 64;
                &mut set[idx as usize]
            }
        };
        *set &= !(0b1 << x);
    }
    // fn iter_single(mut set: u64, mut visit: impl FnMut(u16) -> Result<()>) -> Result<()> {
    //     while set > 0 {
    //         let idx = set.trailing_zeros() as u16;
    //         visit(idx)?;
    //         set &= set - 1;
    //     }
    //     Ok(())
    // }
    // pub fn iter(&self, mut visit: impl FnMut(u16) -> Result<()>) -> Result<()> {
    //     match self {
    //         SmallBitmap::Tiny(set) => Self::iter_single(*set, &mut visit),
    //         SmallBitmap::Small(sets) => {
    //             let mut base = 0;
    //             for set in sets.iter() {
    //                 Self::iter_single(*set, |x| visit(base + x))?;
    //                 base += 64;
    //             }
    //             Ok(())
    //         }
    //     }
    // }

    pub fn intersection(&mut self, other: &SmallBitmap) {
        self.apply_op(other, |a, b| *a &= b);
    }
    pub fn union(&mut self, other: &SmallBitmap) {
        self.apply_op(other, |a, b| *a |= b);
    }
    pub fn subtract(&mut self, other: &SmallBitmap) {
        self.apply_op(other, |a, b| *a &= !b);
    }

    pub fn apply_op(&mut self, other: &SmallBitmap, op: impl Fn(&mut u64, u64)) {
        match (self, other) {
            (SmallBitmap::Tiny(a), SmallBitmap::Tiny(b)) => op(a, *b),
            (SmallBitmap::Small(a), SmallBitmap::Small(b)) => {
                assert!(a.len() == b.len(),);
                for (a, b) in a.iter_mut().zip(b.iter()) {
                    op(a, *b);
                }
            }
            _ => {
                panic!();
            }
        }
    }
    pub fn all_satisfy_op(&self, other: &SmallBitmap, op: impl Fn(u64, u64) -> bool) -> bool {
        match (self, other) {
            (SmallBitmap::Tiny(a), SmallBitmap::Tiny(b)) => op(*a, *b),
            (SmallBitmap::Small(a), SmallBitmap::Small(b)) => {
                assert!(a.len() == b.len());
                for (a, b) in a.iter().zip(b.iter()) {
                    if !op(*a, *b) {
                        return false;
                    }
                }
                true
            }
            _ => {
                panic!();
            }
        }
    }
    pub fn any_satisfy_op(&self, other: &SmallBitmap, op: impl Fn(u64, u64) -> bool) -> bool {
        match (self, other) {
            (SmallBitmap::Tiny(a), SmallBitmap::Tiny(b)) => op(*a, *b),
            (SmallBitmap::Small(a), SmallBitmap::Small(b)) => {
                assert!(a.len() == b.len());
                for (a, b) in a.iter().zip(b.iter()) {
                    if op(*a, *b) {
                        return true;
                    }
                }
                false
            }
            _ => {
                panic!();
            }
        }
    }
    pub fn is_subset(&self, other: &SmallBitmap) -> bool {
        self.all_satisfy_op(other, |a, b| a & !b == 0)
    }
    pub fn intersects(&self, other: &SmallBitmap) -> bool {
        self.any_satisfy_op(other, |a, b| a & b != 0)
    }
    pub fn iter(&self) -> SmallBitmapIter<'_> {
        match self {
            SmallBitmap::Tiny(x) => SmallBitmapIter::Tiny(*x),
            SmallBitmap::Small(xs) => {
                SmallBitmapIter::Small { cur: xs[0], next: &xs[1..], base: 0 }
            }
        }
    }
}

pub enum SmallBitmapIter<'b> {
    Tiny(u64),
    Small { cur: u64, next: &'b [u64], base: u16 },
}
impl<'b> Iterator for SmallBitmapIter<'b> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SmallBitmapIter::Tiny(set) => {
                if *set > 0 {
                    let idx = set.trailing_zeros() as u16;
                    *set &= *set - 1;
                    Some(idx)
                } else {
                    None
                }
            }
            SmallBitmapIter::Small { cur, next, base } => {
                if *cur > 0 {
                    let idx = cur.trailing_zeros() as u16;
                    *cur &= *cur - 1;
                    Some(idx + *base)
                } else if next.is_empty() {
                    return None;
                } else {
                    *base += 64;
                    *cur = next[0];
                    *next = &next[1..];
                    self.next()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SmallBitmap;

    #[test]
    fn test_small_bitmap() {
        let mut bitmap1 = SmallBitmap::new(32);
        for x in 0..16 {
            bitmap1.insert(x * 2);
        }
        let mut bitmap2 = SmallBitmap::new(32);
        for x in 0..=10 {
            bitmap2.insert(x * 3);
        }
        bitmap1.intersection(&bitmap2);
        // println!("{}", bitmap.contains(12));
        // bitmap1
        //     .iter(|x| {
        //         println!("{x}");
        //         Ok(())
        //     })
        //     .unwrap();

        // iter_bitmap!(bitmap1, 'loop1, x, {
        //     println!("{x}");
        // })
    }
}
