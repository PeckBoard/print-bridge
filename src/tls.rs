//! TLS client config for the printer's self-signed services (MQTT 8883,
//! FTPS 990, camera 6000).
//!
//! Bambu printers present a per-device certificate signed by Bambu's own CA,
//! with the serial number as CN — never a name the bridge can validate
//! against the LAN IP it dials. They are also X.509 **v1** certificates,
//! which webpki refuses to parse (`UnsupportedCertVersion`), so even the
//! handshake-signature check must not go through webpki. We accept any
//! certificate and any handshake signature: with no pinning, checking the
//! signature of an unauthenticated certificate proves nothing anyway. Run
//! the bridge on a trusted LAN only.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};

/// Shared slot the verifier writes the peer certificate's CN into. On a
/// Bambu printer that CN is the device serial.
pub type CnSink = Arc<std::sync::Mutex<Option<String>>>;

#[derive(Debug)]
struct AcceptAnyCert {
    algs: WebPkiSupportedAlgorithms,
    cn: Option<CnSink>,
}

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if let Some(sink) = &self.cn {
            *sink.lock().unwrap() = common_name(end_entity.as_ref());
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

/// A fresh client config. Each config owns its own session cache, so FTPS
/// must reuse one `Arc<ClientConfig>` for the control and data connections
/// to get the session resumption the printer's vsftpd demands.
pub fn client_config() -> Arc<ClientConfig> {
    build(None)
}

/// Like [`client_config`], but records the peer certificate's CN in `cn`.
pub fn client_config_with_cn(cn: CnSink) -> Arc<ClientConfig> {
    build(Some(cn))
}

fn build(cn: Option<CnSink>) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let algs = provider.signature_verification_algorithms;
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert { algs, cn }))
        .with_no_client_auth();
    Arc::new(config)
}

/// The first commonName (OID 2.5.4.3) in a DER certificate. A byte scan
/// rather than a full X.509 parse: Bambu's v1 certificates are rejected by
/// webpki, and the CN is all we need.
pub fn common_name(der: &[u8]) -> Option<String> {
    const CN_OID: [u8; 5] = [0x06, 0x03, 0x55, 0x04, 0x03];
    // The issuer comes before the subject in the TBS certificate, so take
    // the last CN, which is the subject's.
    let mut found = None;
    let mut i = 0;
    while i + CN_OID.len() + 2 <= der.len() {
        if der[i..i + CN_OID.len()] == CN_OID {
            let tag = der[i + CN_OID.len()];
            let len = der[i + CN_OID.len() + 1] as usize;
            let start = i + CN_OID.len() + 2;
            // UTF8String, PrintableString, IA5String; short-form length only.
            if matches!(tag, 0x0c | 0x13 | 0x16)
                && len < 0x80
                && let Some(bytes) = der.get(start..start + len)
                && let Ok(s) = std::str::from_utf8(bytes)
            {
                found = Some(s.to_owned());
            }
            i = start;
        } else {
            i += 1;
        }
    }
    found
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

#[cfg(test)]
mod tests {
    use super::common_name;

    #[test]
    fn reads_subject_cn_after_issuer_cn() {
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "BBL CA");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();

        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "01P00A431400749");
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &ca, &ca_key).unwrap();

        assert_eq!(common_name(cert.der()).as_deref(), Some("01P00A431400749"));
        assert_eq!(common_name(b"not a cert"), None);
    }
}
