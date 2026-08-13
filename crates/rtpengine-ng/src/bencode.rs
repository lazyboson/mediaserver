use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BencodeError {
    #[error("unexpected end of input at byte {0}")]
    UnexpectedEof(usize),
    #[error("invalid token {token:?} at byte {pos}")]
    InvalidToken { token: u8, pos: usize },
    #[error("invalid integer at byte {0}")]
    InvalidInt(usize),
    #[error("string length overflows input at byte {0}")]
    BadLength(usize),
    #[error("trailing bytes after value")]
    TrailingData,
}

impl Value {
    pub fn str(s: &str) -> Value {
        Value::Bytes(s.as_bytes().to_vec())
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(self.as_bytes()?).ok()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Dict(d) => d.get(key.as_bytes()),
            _ => None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Int(i) => {
                out.push(b'i');
                out.extend_from_slice(i.to_string().as_bytes());
                out.push(b'e');
            }
            Value::Bytes(b) => {
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(b);
            }
            Value::List(items) => {
                out.push(b'l');
                for item in items {
                    item.encode_into(out);
                }
                out.push(b'e');
            }
            Value::Dict(map) => {
                out.push(b'd');
                for (k, v) in map {
                    out.extend_from_slice(k.len().to_string().as_bytes());
                    out.push(b':');
                    out.extend_from_slice(k);
                    v.encode_into(out);
                }
                out.push(b'e');
            }
        }
    }

    pub fn decode(input: &[u8]) -> Result<Value, BencodeError> {
        let (value, consumed) = decode_at(input, 0)?;
        if consumed != input.len() {
            return Err(BencodeError::TrailingData);
        }
        Ok(value)
    }
}

fn decode_at(input: &[u8], pos: usize) -> Result<(Value, usize), BencodeError> {
    let b = *input.get(pos).ok_or(BencodeError::UnexpectedEof(pos))?;
    match b {
        b'i' => {
            let end = find(input, pos + 1, b'e')?;
            let s = std::str::from_utf8(&input[pos + 1..end])
                .map_err(|_| BencodeError::InvalidInt(pos))?;
            let i = s
                .parse::<i64>()
                .map_err(|_| BencodeError::InvalidInt(pos))?;
            Ok((Value::Int(i), end + 1))
        }
        b'l' => {
            let mut items = Vec::new();
            let mut cur = pos + 1;
            loop {
                match input.get(cur) {
                    Some(b'e') => return Ok((Value::List(items), cur + 1)),
                    Some(_) => {
                        let (v, next) = decode_at(input, cur)?;
                        items.push(v);
                        cur = next;
                    }
                    None => return Err(BencodeError::UnexpectedEof(cur)),
                }
            }
        }
        b'd' => {
            let mut map = BTreeMap::new();
            let mut cur = pos + 1;
            loop {
                match input.get(cur) {
                    Some(b'e') => return Ok((Value::Dict(map), cur + 1)),
                    Some(_) => {
                        let (key, next) = decode_at(input, cur)?;
                        let key = match key {
                            Value::Bytes(k) => k,
                            _ => {
                                return Err(BencodeError::InvalidToken {
                                    token: input[cur],
                                    pos: cur,
                                })
                            }
                        };
                        let (val, after) = decode_at(input, next)?;
                        map.insert(key, val);
                        cur = after;
                    }
                    None => return Err(BencodeError::UnexpectedEof(cur)),
                }
            }
        }
        b'0'..=b'9' => {
            let colon = find(input, pos, b':')?;
            let len: usize = std::str::from_utf8(&input[pos..colon])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or(BencodeError::BadLength(pos))?;
            let start = colon + 1;
            let end = start.checked_add(len).ok_or(BencodeError::BadLength(pos))?;
            if end > input.len() {
                return Err(BencodeError::BadLength(pos));
            }
            Ok((Value::Bytes(input[start..end].to_vec()), end))
        }
        token => Err(BencodeError::InvalidToken { token, pos }),
    }
}

fn find(input: &[u8], from: usize, needle: u8) -> Result<usize, BencodeError> {
    input[from..]
        .iter()
        .position(|&b| b == needle)
        .map(|i| from + i)
        .ok_or(BencodeError::UnexpectedEof(input.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_nested() {
        let mut d = BTreeMap::new();
        d.insert(b"command".to_vec(), Value::str("subscribe request"));
        d.insert(b"call-id".to_vec(), Value::str("abc-123"));
        d.insert(
            b"from-tags".to_vec(),
            Value::List(vec![Value::str("tagA"), Value::str("tagB")]),
        );
        d.insert(b"ttl".to_vec(), Value::Int(30));
        let v = Value::Dict(d);
        let encoded = v.encode();
        assert_eq!(Value::decode(&encoded).unwrap(), v);
    }

    #[test]
    fn decodes_known_shape() {
        let wire = b"d6:result2:ok3:sdp5:v=0\r\ne";
        let v = Value::decode(wire).unwrap();
        assert_eq!(v.get("result").unwrap().as_str(), Some("ok"));
        assert_eq!(v.get("sdp").unwrap().as_bytes(), Some(&b"v=0\r\n"[..]));
    }

    #[test]
    fn malformed_input_is_error_not_panic() {
        for bad in [
            &b"d3:fooe"[..],
            &b"i42"[..],
            &b"9999:ab"[..],
            &b"x"[..],
            &b"d1:ai1ee junk"[..],
            &b""[..],
        ] {
            assert!(Value::decode(bad).is_err(), "accepted {bad:?}");
        }
    }
}
