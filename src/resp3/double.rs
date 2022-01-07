use std::hash::Hash;
use std::str::FromStr;

/// Double precision floating point number which can't be NaN.
#[derive(Debug, Clone, Copy, Default)]
pub struct Double(f64);

impl Double {
    /// Construct `Double` from the `f64`.
    ///
    /// # Panic
    ///
    /// Panics if the `num` is NaN.
    pub fn new(num: f64) -> Self {
        assert!(!num.is_nan(), "RESP3 Double can't be NaN");
        Double(num)
    }

    /// Get the underlying `f64` value.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Double {}

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Hash for Double {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.0 == 0.0 {
            // -0.0 == 0.0
            0.0f64.to_bits().hash(state);
        } else {
            self.0.to_bits().hash(state)
        }
    }
}

impl FromStr for Double {
    type Err = <f64 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Double(s.parse()?))
    }
}
