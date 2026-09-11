use crate::Result;
use crate::error::bail;

/// An exact decimal: `unscaled * 10^(-scale)`, with at most 38 digits.
/// Scale is retained; columns require an exact scale match rather than implicit rounding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal {
    unscaled: i128,
    scale: u8,
}
impl Decimal {
    /// `Decimal::new(12345, 2)` represents `123.45`.
    pub fn new(unscaled: i128, scale: u8) -> Result<Self> {
        if scale > 38 || unscaled.unsigned_abs() >= 10u128.pow(38) {
            bail!(Schema, "decimal exceeds 38 digits or scale 38");
        }
        Ok(Self { unscaled, scale })
    }
    pub fn unscaled(&self) -> i128 {
        self.unscaled
    }
    pub fn scale(&self) -> u8 {
        self.scale
    }
    pub(crate) fn fits_precision(&self, precision: u8) -> bool {
        (1..=38).contains(&precision) && self.unscaled.unsigned_abs() < 10u128.pow(precision as u32)
    }
}
