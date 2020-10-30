use std::fmt;

#[derive(Debug, Clone)]
pub struct ParseMetadataSourceError {}

impl fmt::Display for ParseMetadataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid metadata source.")
    }
}
