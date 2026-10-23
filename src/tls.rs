//! TLS client config for the printer's self-signed services (MQTT 8883,
//! FTPS 990, camera 6000).
//!
//! Bambu printers present a per-device certificate signed by Bambu's own CA,
//! with the serial number as CN — never a name the bridge can validate
//! against the LAN IP it dials. We therefore accept any certificate but still
//! verify the handshake signature (so the peer must hold the key for the
//! certificate it presents).

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};

#[derive(Debug)]
struct AcceptAnyCert {
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

/// A fresh client config. Each config owns its own session cache, so FTPS
/// must reuse one `Arc<ClientConfig>` for the control and data connections
/// to get the session resumption the printer's vsftpd demands.
pub fn client_config() -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let algs = provider.signature_verification_algorithms;
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert { algs }))
        .with_no_client_auth();
    Arc::new(config)
}

/// `ServerName` for a host string (IP literal or DNS name).
pub fn server_name(host: &str) -> anyhow::Result<ServerName<'static>> {
    Ok(ServerName::try_from(host.to_owned())?)
}

/// Install ring as the process default so any rustls path that does not
/// take an explicit provider still works.
pub fn install_default_provider() {
    let _ = CryptoProvider::install_default(rustls::crypto::ring::default_provider());
}

/// Self-signed server config for tests that stand in for a printer.
#[cfg(test)]
pub fn test_server_config(
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Arc<rustls::ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec!["01P00A000000000".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(versions)
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key)
        .unwrap();
    Arc::new(cfg)
}
