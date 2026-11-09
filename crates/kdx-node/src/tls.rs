//! P2P TLS: accept exactly one pinned self-signed leaf certificate.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, SignatureScheme};
use sha2::{Digest, Sha256};

use crate::types::{NodeError, Result};

#[derive(Debug)]
struct PinVerifier {
    pin: [u8; 32],
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, Error> {
        let got = Sha256::digest(end_entity.as_ref());
        if got.as_slice() == self.pin {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(Error::General(
                "certificate fingerprint does not match the pin".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

/// rustls client config that trusts exactly the leaf with SHA-256 == `pin`.
pub fn pinned_client_config(pin: [u8; 32]) -> Result<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let algs = provider.signature_verification_algorithms;
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| NodeError::Config(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinVerifier { pin, algs }))
        .with_no_client_auth();
    Ok(cfg)
}

/// Parse a 64-char hex SHA-256 (colons and spaces tolerated).
pub fn parse_sha256_hex(s: &str) -> Result<[u8; 32]> {
    let clean: String = s.chars().filter(|c| !matches!(c, ':' | ' ')).collect();
    let mut out = [0u8; 32];
    hex::decode_to_slice(&clean, &mut out)
        .map_err(|e| NodeError::Config(format!("p2p_cert_sha256: {e}")))?;
    Ok(out)
}
