use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use quinn::{RecvStream, SendStream};
use sha2::Sha256;

use crate::transport::ChannelPair;

type HmacSha256 = Hmac<Sha256>;

/// Maximum message size in bytes. Default 512 MB, configurable via `set_max_message_size`.
static MAX_MESSAGE_SIZE: AtomicUsize = AtomicUsize::new(512 * 1024 * 1024);

/// Set the maximum message size for all cluster framing operations.
pub fn set_max_message_size(size_bytes: usize) {
    MAX_MESSAGE_SIZE.store(size_bytes, Ordering::Relaxed);
}

// Wire format per message:
// [4 bytes: payload_len (LE u32)] [8 bytes: seq (LE u64)] [payload_len bytes: data] [32 bytes: HMAC-SHA256]
//
// HMAC covers seq || data (replay protection + integrity).
// Every message is HMAC-signed with the shared cluster secret.
// Signed not encrypted — maximizes speed on trusted LANs.

/// Build a signed frame with sequence number.
/// Returns the wire bytes ready to write to a QUIC stream.
pub fn to_signed_bytes(seq: u64, data: &[u8], secret: &[u8]) -> Result<Bytes> {
    let len: u32 = data.len().try_into().context("message too large for u32 length prefix")?;
    let seq_bytes = seq.to_le_bytes();

    // HMAC covers seq || data
    let mut mac = HmacSha256::new_from_slice(secret).context("invalid HMAC key length")?;
    mac.update(&seq_bytes);
    mac.update(data);
    let sig = mac.finalize().into_bytes();

    let mut packet = Vec::with_capacity(4 + 8 + data.len() + 32);
    packet.extend_from_slice(&len.to_le_bytes());
    packet.extend_from_slice(&seq_bytes);
    packet.extend_from_slice(data);
    packet.extend_from_slice(&sig);
    Ok(Bytes::from(packet))
}

/// Send a signed message with sequence number over a QUIC stream.
pub async fn send_signed(s: &mut SendStream, seq: u64, data: &[u8], secret: &[u8]) -> Result<()> {
    let packet = to_signed_bytes(seq, data, secret)?;
    s.write_all(&packet).await.context("failed to write signed message")?;
    Ok(())
}

/// Receive and verify a signed message from a QUIC stream.
/// Returns (sequence_number, verified_payload).
pub async fn recv_signed(r: &mut RecvStream, secret: &[u8]) -> Result<(u64, Vec<u8>)> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await.context("failed to read message length")?;
    let len = u32::from_le_bytes(len_buf) as usize;

    // Guard against absurdly large messages.
    // File transfers via DML channel may carry large document payloads.
    let max_size = MAX_MESSAGE_SIZE.load(Ordering::Relaxed);
    if len > max_size {
        anyhow::bail!("message too large: {len} bytes (max {max_size} bytes)");
    }

    // Read 8-byte sequence number
    let mut seq_buf = [0u8; 8];
    r.read_exact(&mut seq_buf).await.context("failed to read sequence number")?;
    let seq = u64::from_le_bytes(seq_buf);

    // Read payload + 32-byte HMAC
    let mut packet = vec![0u8; len + 32];
    r.read_exact(&mut packet).await.context("failed to read message body + signature")?;

    let (data, received_sig) = packet.split_at(len);

    // Verify HMAC over seq || data
    let mut mac = HmacSha256::new_from_slice(secret).context("invalid HMAC key length")?;
    mac.update(&seq_buf);
    mac.update(data);
    mac.verify_slice(received_sig).map_err(|_| {
        anyhow::anyhow!(
            "HMAC signature verification failed — message tampered or wrong cluster key"
        )
    })?;

    Ok((seq, data.to_vec()))
}

/// Compute HMAC-SHA256 of data with the given secret.
fn compute_challenge_hmac(secret: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(secret).expect("HMAC key length should always be valid");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Perform the acceptor side of the DML channel handshake.
/// Sends a random 32-byte nonce, expects HMAC(nonce, secret) in response.
/// Must be called before spawning the DML handler loop.
pub async fn dml_handshake_acceptor(ch: &mut ChannelPair, secret: &[u8]) -> Result<()> {
    // Generate 32-byte random nonce
    let nonce: [u8; 32] = rand::random();

    // Send nonce as the first DML message
    let seq = ch.send_seq;
    ch.send_seq += 1;
    send_signed(&mut ch.send, seq, &nonce, secret).await.context("DML handshake: send nonce")?;

    // Read challenge response
    let (resp_seq, resp_data) =
        recv_signed(&mut ch.recv, secret).await.context("DML handshake: recv response")?;
    if resp_seq <= ch.recv_seq {
        anyhow::bail!(
            "DML handshake: replay detected (seq {resp_seq}, expected > {})",
            ch.recv_seq
        );
    }
    ch.recv_seq = resp_seq;

    // Verify: response should be HMAC(nonce, secret)
    let expected = compute_challenge_hmac(secret, &nonce);
    if resp_data != expected {
        anyhow::bail!("DML handshake failed: invalid challenge response");
    }

    Ok(())
}

/// Perform the connector side of the DML channel handshake.
/// Reads the nonce from the acceptor, sends back HMAC(nonce, secret).
/// Must be called after connection establishment, before using the DML channel.
pub async fn dml_handshake_connector(ch: &mut ChannelPair, secret: &[u8]) -> Result<()> {
    // Read nonce from acceptor
    let (nonce_seq, nonce) =
        recv_signed(&mut ch.recv, secret).await.context("DML handshake: recv nonce")?;
    if nonce_seq <= ch.recv_seq {
        anyhow::bail!(
            "DML handshake: replay detected (seq {nonce_seq}, expected > {})",
            ch.recv_seq
        );
    }
    ch.recv_seq = nonce_seq;

    // Compute HMAC(nonce, secret) and send
    let response = compute_challenge_hmac(secret, &nonce);
    let seq = ch.send_seq;
    ch.send_seq += 1;
    send_signed(&mut ch.send, seq, &response, secret)
        .await
        .context("DML handshake: send response")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signed_bytes_roundtrip() {
        let secret = b"mysecretkey123";
        let data = b"hello world";
        let seq = 42u64;
        let packet = to_signed_bytes(seq, data, secret).unwrap();

        // Verify structure: 4 bytes len + 8 bytes seq + 11 bytes data + 32 bytes sig
        assert_eq!(packet.len(), 4 + 8 + 11 + 32);

        let len = u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize;
        assert_eq!(len, 11);

        let recv_seq = u64::from_le_bytes(packet[4..12].try_into().unwrap());
        assert_eq!(recv_seq, 42);

        let payload = &packet[12..12 + len];
        assert_eq!(payload, b"hello world");

        // Verify HMAC over seq || data
        let sig = &packet[12 + len..];
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&seq.to_le_bytes());
        mac.update(payload);
        mac.verify_slice(sig).unwrap();
    }

    #[test]
    fn test_tamper_detection() {
        let secret = b"mysecretkey123";
        let data = b"hello world";
        let mut packet = to_signed_bytes(1, data, secret).unwrap().to_vec();

        // Tamper with the payload (byte 13 is first data byte after 4+8 header)
        packet[13] ^= 0xFF;

        let len = u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize;
        let seq_bytes = &packet[4..12];
        let payload = &packet[12..12 + len];
        let sig = &packet[12 + len..];

        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(seq_bytes);
        mac.update(payload);
        assert!(mac.verify_slice(sig).is_err());
    }

    #[test]
    fn test_wrong_key_detection() {
        let secret = b"mysecretkey123";
        let wrong_secret = b"wrongkey456789";
        let data = b"hello world";
        let packet = to_signed_bytes(1, data, secret).unwrap();

        let len = u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize;
        let seq_bytes = &packet[4..12];
        let payload = &packet[12..12 + len];
        let sig = &packet[12 + len..];

        let mut mac = HmacSha256::new_from_slice(wrong_secret).unwrap();
        mac.update(seq_bytes);
        mac.update(payload);
        assert!(mac.verify_slice(sig).is_err());
    }

    #[test]
    fn test_replay_detection_seq_in_hmac() {
        let secret = b"mysecretkey123";
        let data = b"hello world";

        // Sign with seq=1
        let packet = to_signed_bytes(1, data, secret).unwrap();

        // Try to verify as if seq were 2 — should fail because HMAC covers the seq
        let len = u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize;
        let payload = &packet[12..12 + len];
        let sig = &packet[12 + len..];

        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&2u64.to_le_bytes()); // different seq
        mac.update(payload);
        assert!(mac.verify_slice(sig).is_err());
    }

    #[test]
    fn test_empty_payload_roundtrip() {
        // Edge case: 0-byte payload (like an empty file chunk)
        let secret = b"mysecretkey123";
        let data: &[u8] = b"";
        let seq = 1u64;
        let packet = to_signed_bytes(seq, data, secret).unwrap();

        // Structure: 4 bytes len(0) + 8 bytes seq + 0 bytes data + 32 bytes sig
        assert_eq!(packet.len(), 4 + 8 + 0 + 32);

        let len = u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize;
        assert_eq!(len, 0);

        let recv_seq = u64::from_le_bytes(packet[4..12].try_into().unwrap());
        assert_eq!(recv_seq, 1);

        // Verify HMAC still valid for empty payload
        let sig = &packet[12..];
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&seq.to_le_bytes());
        // no data to update with
        mac.verify_slice(sig).unwrap();
    }

    #[test]
    fn test_exact_chunk_size_payload() {
        // Edge case: payload exactly at DML_CHUNK_SIZE boundary (64KB)
        let secret = b"test-cluster-secret-16b";
        let data = vec![0xABu8; crate::rpc_handler::DML_CHUNK_SIZE];
        let seq = 99u64;
        let packet = to_signed_bytes(seq, &data, secret).unwrap();

        let expected_len = 4 + 8 + crate::rpc_handler::DML_CHUNK_SIZE + 32;
        assert_eq!(packet.len(), expected_len);

        let len = u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize;
        assert_eq!(len, crate::rpc_handler::DML_CHUNK_SIZE);

        // Verify HMAC for large payload
        let payload = &packet[12..12 + len];
        let sig = &packet[12 + len..];
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(&seq.to_le_bytes());
        mac.update(payload);
        mac.verify_slice(sig).unwrap();
    }

    #[test]
    fn test_sequential_seq_numbers() {
        // Simulate a DML stream: header + N chunks, each with incrementing seq
        let secret = b"mysecretkey123";
        let mut seqs = Vec::new();

        for i in 1..=10u64 {
            let data = format!("chunk-{i}");
            let packet = to_signed_bytes(i, data.as_bytes(), secret).unwrap();

            let recv_seq = u64::from_le_bytes(packet[4..12].try_into().unwrap());
            assert_eq!(recv_seq, i);

            let len = u32::from_le_bytes(packet[..4].try_into().unwrap()) as usize;
            let payload = &packet[12..12 + len];
            let sig = &packet[12 + len..];

            let mut mac = HmacSha256::new_from_slice(secret).unwrap();
            mac.update(&i.to_le_bytes());
            mac.update(payload);
            mac.verify_slice(sig).unwrap();

            seqs.push(recv_seq);
        }

        // Verify monotonic increase
        for window in seqs.windows(2) {
            assert!(window[1] > window[0], "Sequence numbers must be strictly increasing");
        }
    }

    #[test]
    fn test_dml_challenge_hmac_consistency() {
        // Verify compute_challenge_hmac produces the expected HMAC
        let secret = b"test-cluster-secret-16b";
        let nonce = [0x42u8; 32];

        let hmac1 = compute_challenge_hmac(secret, &nonce);
        let hmac2 = compute_challenge_hmac(secret, &nonce);

        // Same inputs produce same output
        assert_eq!(hmac1, hmac2);
        assert_eq!(hmac1.len(), 32); // HMAC-SHA256 is 32 bytes

        // Different nonce produces different output
        let different_nonce = [0x43u8; 32];
        let hmac3 = compute_challenge_hmac(secret, &different_nonce);
        assert_ne!(hmac1, hmac3);

        // Different secret produces different output
        let different_secret = b"different-secret-16b";
        let hmac4 = compute_challenge_hmac(different_secret, &nonce);
        assert_ne!(hmac1, hmac4);
    }
}
