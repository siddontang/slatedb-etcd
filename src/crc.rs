//! CRC-32C combine in O(popcount(len)) small carry-less products.
//!
//! `crc32c::crc32c_combine` 0.6 squares 32 x 32 GF(2) matrices about log2(len) times per call.
//! That costs several microseconds. Hash records and compaction call it once per stored row, so
//! it dominated the compaction hash pass. This module uses the method of zlib 1.2.12
//! (`multmodp` and `x2nmodp`). It returns the same value as `crc32c::crc32c_combine` for every
//! input, including the `len == 0` case.

/// The reflected CRC-32C polynomial.
const POLY: u32 = 0x82F6_3B78;

/// Multiply `a` by `b` modulo the polynomial. Bit 31 is x^0 in the reflected form.
const fn multmodp(a: u32, mut b: u32) -> u32 {
    let mut m = 1_u32 << 31;
    let mut p = 0;
    while m != 0 {
        if a & m != 0 {
            p ^= b;
            if a & (m - 1) == 0 {
                break;
            }
        }
        m >>= 1;
        b = if b & 1 != 0 { (b >> 1) ^ POLY } else { b >> 1 };
    }
    p
}

/// `X2N[k]` is x^(2^k) modulo the polynomial. `x2nmodp` reaches k = 3 + 63.
const X2N: [u32; 67] = {
    let mut table = [0; 67];
    let mut p = 1_u32 << 30; // x^1
    table[0] = p;
    let mut n = 1;
    while n < 67 {
        p = multmodp(p, p);
        table[n] = p;
        n += 1;
    }
    table
};

/// x^(n * 2^k) modulo the polynomial.
fn x2nmodp(mut n: u64, mut k: usize) -> u32 {
    let mut p = 1_u32 << 31; // x^0
    while n != 0 {
        if n & 1 != 0 {
            p = multmodp(X2N[k], p);
        }
        n >>= 1;
        k += 1;
    }
    p
}

/// The CRC-32C of `a || b` from `crc32c(a)`, `crc32c(b)` and `b.len()`.
pub(crate) fn combine(crc1: u32, crc2: u32, len2: usize) -> u32 {
    if len2 == 0 {
        // `crc32c::crc32c_combine` returns `crc1` here, whatever `crc2` is.
        return crc1;
    }
    // Appending one byte multiplies by x^8, so k = 3.
    multmodp(x2nmodp(len2 as u64, 3), crc1) ^ crc2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_crate_combine() {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut lengths: Vec<usize> = (0..600).collect();
        lengths.extend([
            1 << 12,
            (1 << 16) + 3,
            1 << 20,
            (1 << 31) + 7,
            usize::MAX >> 1,
        ]);
        for _ in 0..2000 {
            lengths.push((next() % (1 << 24)) as usize);
        }
        for len in lengths {
            let (a, b) = (next() as u32, next() as u32);
            assert_eq!(
                combine(a, b, len),
                crc32c::crc32c_combine(a, b, len),
                "{len}"
            );
        }
    }

    #[test]
    fn combines_concatenated_bytes() {
        let data: Vec<u8> = (0..5000_u32).map(|n| (n * 31 + 7) as u8).collect();
        for split in [0, 1, 7, 255, 256, 2500, 4999, 5000] {
            let (a, b) = data.split_at(split);
            let whole = crc32c::crc32c(&data);
            assert_eq!(
                combine(crc32c::crc32c(a), crc32c::crc32c(b), b.len()),
                whole
            );
        }
    }
}
