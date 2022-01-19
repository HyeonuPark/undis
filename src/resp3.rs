//! Utility modules to serialize and deserialize
//! [RESP3 protocol](https://github.com/antirez/RESP3/blob/74adea588783e463c7e84793b325b088fe6edd1c/spec.md).

pub mod de;
mod double;
pub mod ser_cmd;
pub mod token;
pub mod value;

pub use de::from_msg;
pub use double::Double;
pub use ser_cmd::write_cmd;
pub use token::Reader;
pub use value::Value;

fn parse_str<T: std::str::FromStr>(msg: &[u8]) -> Option<T> {
    std::str::from_utf8(msg).ok()?.parse().ok()
}
