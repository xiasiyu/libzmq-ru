use crate::{Error, Result};
use curve25519_dalek::montgomery::MontgomeryPoint;
use zeroize::Zeroize;

const Z85_CHARS: &[u8; 85] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

pub fn z85_encode(data: &[u8]) -> Result<String> {
    if data.len() % 4 != 0 {
        return Err(Error::InvalidArgument);
    }
    let mut encoded = String::with_capacity(data.len() * 5 / 4);
    for chunk in data.chunks_exact(4) {
        let mut value = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let mut block = [0u8; 5];
        for index in (0..5).rev() {
            block[index] = Z85_CHARS[(value % 85) as usize];
            value /= 85;
        }
        encoded.push_str(std::str::from_utf8(&block).map_err(|_| Error::InvalidArgument)?);
    }
    Ok(encoded)
}

pub fn z85_decode(encoded: &str) -> Result<Vec<u8>> {
    if encoded.len() % 5 != 0 {
        return Err(Error::InvalidArgument);
    }
    let mut decoded = Vec::with_capacity(encoded.len() * 4 / 5);
    for chunk in encoded.as_bytes().chunks_exact(5) {
        let mut value = 0u32;
        for byte in chunk {
            let digit = z85_value(*byte).ok_or(Error::InvalidArgument)?;
            value = value
                .checked_mul(85)
                .and_then(|value| value.checked_add(digit as u32))
                .ok_or(Error::InvalidArgument)?;
        }
        decoded.extend_from_slice(&value.to_be_bytes());
    }
    Ok(decoded)
}

pub fn curve_keypair() -> Result<(String, String)> {
    let mut secret = [0u8; 32];
    getrandom::getrandom(&mut secret).map_err(|_| Error::InvalidSocket)?;
    let public = curve_public_bytes(&secret);
    let public = z85_encode(&public)?;
    let secret = SecretBytes(secret);
    let secret_encoded = z85_encode(secret.as_slice())?;
    Ok((public, secret_encoded))
}

pub fn curve_public(z85_secret_key: &str) -> Result<String> {
    let secret = z85_decode(z85_secret_key)?;
    if secret.len() != 32 {
        return Err(Error::InvalidArgument);
    }
    let mut secret_bytes = [0u8; 32];
    secret_bytes.copy_from_slice(&secret);
    let secret = SecretBytes(secret_bytes);
    let public = curve_public_bytes(secret.as_slice());
    z85_encode(&public)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZapRequest {
    pub version: String,
    pub request_id: String,
    pub domain: Vec<u8>,
    pub address: Vec<u8>,
    pub identity: Vec<u8>,
    pub mechanism: String,
    pub credentials: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZapReply {
    pub version: String,
    pub request_id: String,
    pub status_code: String,
    pub status_text: String,
    pub user_id: Vec<u8>,
    pub metadata: Vec<u8>,
}

impl ZapRequest {
    pub fn new(
        request_id: impl Into<String>,
        domain: impl Into<Vec<u8>>,
        address: impl Into<Vec<u8>>,
        identity: impl Into<Vec<u8>>,
        mechanism: impl Into<String>,
        credentials: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> Self {
        Self {
            version: "1.0".to_string(),
            request_id: request_id.into(),
            domain: domain.into(),
            address: address.into(),
            identity: identity.into(),
            mechanism: mechanism.into(),
            credentials: credentials.into_iter().map(Into::into).collect(),
        }
    }

    pub fn encode(&self) -> Vec<Vec<u8>> {
        let mut frames = vec![
            self.version.as_bytes().to_vec(),
            self.request_id.as_bytes().to_vec(),
            self.domain.clone(),
            self.address.clone(),
            self.identity.clone(),
            self.mechanism.as_bytes().to_vec(),
        ];
        frames.extend(self.credentials.iter().cloned());
        frames
    }

    pub fn decode(frames: &[Vec<u8>]) -> Result<Self> {
        if frames.len() < 6 || frames[0] != b"1.0" {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            version: decode_utf8(&frames[0])?,
            request_id: decode_utf8(&frames[1])?,
            domain: frames[2].clone(),
            address: frames[3].clone(),
            identity: frames[4].clone(),
            mechanism: decode_utf8(&frames[5])?,
            credentials: frames[6..].to_vec(),
        })
    }
}

impl ZapReply {
    pub fn new(
        request_id: impl Into<String>,
        status_code: impl Into<String>,
        status_text: impl Into<String>,
        user_id: impl Into<Vec<u8>>,
        metadata: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            version: "1.0".to_string(),
            request_id: request_id.into(),
            status_code: status_code.into(),
            status_text: status_text.into(),
            user_id: user_id.into(),
            metadata: metadata.into(),
        }
    }

    pub fn encode(&self) -> Vec<Vec<u8>> {
        vec![
            self.version.as_bytes().to_vec(),
            self.request_id.as_bytes().to_vec(),
            self.status_code.as_bytes().to_vec(),
            self.status_text.as_bytes().to_vec(),
            self.user_id.clone(),
            self.metadata.clone(),
        ]
    }

    pub fn decode(frames: &[Vec<u8>]) -> Result<Self> {
        if frames.len() != 6 || frames[0] != b"1.0" {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            version: decode_utf8(&frames[0])?,
            request_id: decode_utf8(&frames[1])?,
            status_code: decode_utf8(&frames[2])?,
            status_text: decode_utf8(&frames[3])?,
            user_id: frames[4].clone(),
            metadata: frames[5].clone(),
        })
    }
}

fn curve_public_bytes(secret: &[u8; 32]) -> [u8; 32] {
    MontgomeryPoint::mul_base_clamped(*secret).to_bytes()
}

fn z85_value(byte: u8) -> Option<u8> {
    Z85_CHARS
        .iter()
        .position(|candidate| *candidate == byte)
        .map(|index| index as u8)
}

fn decode_utf8(bytes: &[u8]) -> Result<String> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| Error::InvalidArgument)
}

struct SecretBytes([u8; 32]);

impl SecretBytes {
    fn as_slice(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn z85_known_vector_round_trips() {
        let data = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
        let encoded = z85_encode(&data).unwrap();
        assert_eq!(encoded, "HelloWorld");
        assert_eq!(z85_decode(&encoded).unwrap(), data);
    }

    #[test]
    fn curve_keypair_public_derives_from_secret() {
        let (public, secret) = curve_keypair().unwrap();
        assert_eq!(public.len(), 40);
        assert_eq!(secret.len(), 40);
        assert_eq!(curve_public(&secret).unwrap(), public);
    }

    #[test]
    fn zap_request_and_reply_codecs_round_trip() {
        let request = ZapRequest::new(
            "1",
            b"domain".to_vec(),
            b"127.0.0.1".to_vec(),
            b"identity".to_vec(),
            "PLAIN",
            [b"user".to_vec(), b"pass".to_vec()],
        );
        assert_eq!(ZapRequest::decode(&request.encode()).unwrap(), request);

        let reply = ZapReply::new("1", "200", "OK", b"user".to_vec(), Vec::new());
        assert_eq!(ZapReply::decode(&reply.encode()).unwrap(), reply);
        assert_eq!(
            ZapReply::decode(&request.encode()),
            Err(Error::InvalidArgument)
        );
    }
}
