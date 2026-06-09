#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    data: Vec<u8>,
    more: bool,
}

impl Message {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            more: false,
        }
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        Self { data, more: false }
    }

    pub fn from_slice(data: &[u8]) -> Self {
        Self::from_vec(data.to_vec())
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn more(&self) -> bool {
        self.more
    }

    pub fn set_more(&mut self, more: bool) {
        self.more = more;
    }
}

impl Default for Message {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Vec<u8>> for Message {
    fn from(value: Vec<u8>) -> Self {
        Self::from_vec(value)
    }
}

impl From<&[u8]> for Message {
    fn from(value: &[u8]) -> Self {
        Self::from_slice(value)
    }
}

impl From<&str> for Message {
    fn from(value: &str) -> Self {
        Self::from_slice(value.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::Message;

    #[test]
    fn message_from_str_keeps_bytes() {
        let msg = Message::from("hello");
        assert_eq!(msg.data(), b"hello");
        assert_eq!(msg.len(), 5);
    }
}
