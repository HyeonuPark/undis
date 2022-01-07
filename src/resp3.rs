pub mod de;
pub mod double;
pub mod ser_cmd;
pub mod token;
pub mod value;

pub use de::from_msg;
pub use ser_cmd::write_cmd;
pub use token::Reader;
pub use value::Value;

fn parse_str<T: std::str::FromStr>(msg: &[u8]) -> Option<T> {
    std::str::from_utf8(msg).ok()?.parse().ok()
}
