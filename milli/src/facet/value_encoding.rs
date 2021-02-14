// https://stackoverflow.com/a/43305015/1941280
#[inline]
pub fn f64_into_bytes(float: f64) -> Option<[u8; 8]> {
    if float.is_finite() {
        if float == 0.0 || float == -0.0 {
            return Some(xor_first_bit(0.0_f64.to_be_bytes()));
        } else if float.is_sign_negative() {
            return Some(xor_all_bits(float.to_be_bytes()));
        } else if float.is_sign_positive() {
            return Some(xor_first_bit(float.to_be_bytes()));
        }
    }
    None
}

#[inline]
pub fn i64_into_bytes(int: i64) -> [u8; 8] {
    xor_first_bit(int.to_be_bytes())
}

#[inline]
pub fn i64_from_bytes(bytes: [u8; 8]) -> i64 {
    i64::from_be_bytes(xor_first_bit(bytes))
}

#[inline]
fn xor_first_bit(mut x: [u8; 8]) -> [u8; 8] {
    x[0] ^= 0x80;
    x
}

#[inline]
fn xor_all_bits(mut x: [u8; 8]) -> [u8; 8] {
    x.iter_mut().for_each(|b| *b ^= 0xff);
    x
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering::Less;
    use super::*;

    fn is_sorted<T: Ord>(x: &[T]) -> bool {
        x.windows(2).map(|x| x[0].cmp(&x[1])).all(|o| o == Less)
    }

    #[test]
    fn ordered_f64_bytes() {
        let a = -13_f64;
        let b = -10.0;
        let c = -0.0;
        let d =  1.0;
        let e =  43.0;

        let vec: Vec<_> = [a, b, c, d, e].iter().cloned().map(f64_into_bytes).collect();
        assert!(is_sorted(&vec), "{:?}", vec);
    }

    #[test]
    fn ordered_i64_bytes() {
        let a = -10_i64;
        let b = -0_i64;
        let c =  1_i64;
        let d =  43_i64;

        let vec: Vec<_> = [a, b, c, d].iter().cloned().map(i64_into_bytes).collect();
        assert!(is_sorted(&vec), "{:?}", vec);
    }
}
