use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::{Index, IndexMut};

// A simple wrapper around vec so we can get contiguous but index it like it's 2D array.
struct N2Array<T> {
    y_size: usize,
    buf: Vec<T>,
}

impl<T: Clone> N2Array<T> {
    fn new(x: usize, y: usize, value: T) -> N2Array<T> {
        N2Array {
            y_size: y,
            buf: vec![value; x * y],
        }
    }
}

impl<T> Index<(usize, usize)> for N2Array<T> {
    type Output = T;

    #[inline]
    fn index(&self, (x, y): (usize, usize)) -> &T {
        &self.buf[(x * self.y_size) + y]
    }
}

impl<T> IndexMut<(usize, usize)> for N2Array<T> {
    #[inline]
    fn index_mut(&mut self, (x, y): (usize, usize)) -> &mut T {
        &mut self.buf[(x * self.y_size) + y]
    }
}

pub fn prefix_damerau_levenshtein(source: &[u8], target: &[u8]) -> (u32, usize) {
    let (n, m) = (source.len(), target.len());

    assert!(
        n <= m,
        "the source string must be shorter than the target one"
    );

    if n == 0 {
        return (m as u32, 0);
    }
    if m == 0 {
        return (n as u32, 0);
    }

    if n == m && source == target {
        return (0, m);
    }

    let inf = n + m;
    let mut matrix = N2Array::new(n + 2, m + 2, 0);

    matrix[(0, 0)] = inf;
    for i in 0..n + 1 {
        matrix[(i + 1, 0)] = inf;
        matrix[(i + 1, 1)] = i;
    }
    for j in 0..m + 1 {
        matrix[(0, j + 1)] = inf;
        matrix[(1, j + 1)] = j;
    }

    let mut last_row = BTreeMap::new();

    for (row, char_s) in source.iter().enumerate() {
        let mut last_match_col = 0;
        let row = row + 1;

        for (col, char_t) in target.iter().enumerate() {
            let col = col + 1;
            let last_match_row = *last_row.get(&char_t).unwrap_or(&0);
            let cost = if char_s == char_t { 0 } else { 1 };

            let dist_add = matrix[(row, col + 1)] + 1;
            let dist_del = matrix[(row + 1, col)] + 1;
            let dist_sub = matrix[(row, col)] + cost;
            let dist_trans = matrix[(last_match_row, last_match_col)]
                + (row - last_match_row - 1)
                + 1
                + (col - last_match_col - 1);

            let dist = min(min(dist_add, dist_del), min(dist_sub, dist_trans));

            matrix[(row + 1, col + 1)] = dist;

            if cost == 0 {
                last_match_col = col;
            }
        }

        last_row.insert(char_s, row);
    }

    let mut minimum = (u32::max_value(), 0);

    for x in n..=m {
        let dist = matrix[(n + 1, x + 1)] as u32;
        if dist < minimum.0 {
            minimum = (dist, x)
        }
    }

    minimum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matched_length() {
        let query = "Levenste";
        let text = "Levenshtein";

        let (dist, length) = prefix_damerau_levenshtein(query.as_bytes(), text.as_bytes());
        assert_eq!(dist, 1);
        assert_eq!(&text[..length], "Levenshte");
    }

    #[test]
    #[should_panic]
    fn matched_length_panic() {
        let query = "Levenshtein";
        let text = "Levenste";

        // this function will panic if source if longer than target
        prefix_damerau_levenshtein(query.as_bytes(), text.as_bytes());
    }
}
