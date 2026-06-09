const INLINE_CAPACITY: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    data: MessageData,
    more: bool,
    routing_id: u32,
    group: Option<String>,
    metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MessageData {
    Inline {
        len: u8,
        bytes: [u8; INLINE_CAPACITY],
    },
    Heap(Vec<u8>),
}

impl MessageData {
    fn empty() -> Self {
        Self::Inline {
            len: 0,
            bytes: [0; INLINE_CAPACITY],
        }
    }

    fn from_vec(data: Vec<u8>) -> Self {
        if data.len() <= INLINE_CAPACITY {
            let mut bytes = [0; INLINE_CAPACITY];
            bytes[..data.len()].copy_from_slice(&data);
            Self::Inline {
                len: data.len() as u8,
                bytes,
            }
        } else {
            Self::Heap(data)
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { len, bytes } => &bytes[..*len as usize],
            Self::Heap(data) => data,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Inline { len, bytes } => &mut bytes[..*len as usize],
            Self::Heap(data) => data,
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }
}

impl Message {
    pub fn new() -> Self {
        Self {
            data: MessageData::empty(),
            more: false,
            routing_id: 0,
            group: None,
            metadata: Vec::new(),
        }
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        Self {
            data: MessageData::from_vec(data),
            more: false,
            routing_id: 0,
            group: None,
            metadata: Vec::new(),
        }
    }

    pub fn from_slice(data: &[u8]) -> Self {
        Self::from_vec(data.to_vec())
    }

    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        self.data.as_mut_slice()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn is_inline(&self) -> bool {
        self.data.is_inline()
    }

    pub fn more(&self) -> bool {
        self.more
    }

    pub fn set_more(&mut self, more: bool) {
        self.more = more;
    }

    pub fn routing_id(&self) -> u32 {
        self.routing_id
    }

    pub fn set_routing_id(&mut self, routing_id: u32) {
        self.routing_id = routing_id;
    }

    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: &str) -> crate::Result<()> {
        if group.len() > crate::ZMQ_GROUP_MAX_LENGTH as usize || group.as_bytes().contains(&0) {
            return Err(crate::Error::InvalidArgument);
        }
        self.group = Some(group.to_string());
        Ok(())
    }

    pub fn set_metadata(&mut self, key: &str, value: &str) -> crate::Result<()> {
        if key.is_empty() || key.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
            return Err(crate::Error::InvalidArgument);
        }

        if let Some((_, stored_value)) = self
            .metadata
            .iter_mut()
            .find(|(stored_key, _)| stored_key == key)
        {
            *stored_value = value.to_string();
        } else {
            self.metadata.push((key.to_string(), value.to_string()));
        }
        Ok(())
    }

    pub fn metadata(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(stored_key, _)| stored_key == key)
            .map(|(_, value)| value.as_str())
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
        assert!(msg.is_inline());
    }

    #[test]
    fn large_message_uses_heap_storage() {
        let msg = Message::from_vec(vec![7; 1024]);
        assert_eq!(msg.len(), 1024);
        assert!(!msg.is_inline());
    }

    #[test]
    fn routing_id_and_group_are_available_to_native_api() {
        let mut msg = Message::from("payload");
        msg.set_routing_id(42);
        msg.set_group("updates").unwrap();
        msg.set_metadata("User-Id", "alice").unwrap();

        assert_eq!(msg.routing_id(), 42);
        assert_eq!(msg.group(), Some("updates"));
        assert_eq!(msg.metadata("User-Id"), Some("alice"));
    }
}
