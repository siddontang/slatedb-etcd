//! Check an AutoTLS peer's name, validity, and proof of its own key.

use tokio_rustls::rustls::{
    self,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    client::WebPkiServerVerifier,
    crypto,
    pki_types::{CertificateDer, ServerName, UnixTime},
    DigitallySignedStruct, RootCertStore, SignatureScheme,
};

#[derive(Debug)]
pub(super) struct SelfSigned;

impl ServerCertVerifier for SelfSigned {
    fn verify_server_cert(
        &self,
        leaf: &CertificateDer<'_>,
        chain: &[CertificateDer<'_>],
        name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let mut roots = RootCertStore::empty();
        roots.add(leaf.clone())?;
        WebPkiServerVerifier::builder(roots.into())
            .build()
            .map_err(|err| rustls::Error::General(err.to_string()))?
            .verify_server_cert(leaf, chain, name, ocsp, now)
    }

    fn verify_tls12_signature(
        &self,
        msg: &[u8],
        cert: &CertificateDer<'_>,
        sig: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls12_signature(
            msg,
            cert,
            sig,
            &crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        msg: &[u8],
        cert: &CertificateDer<'_>,
        sig: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls13_signature(
            msg,
            cert,
            sig,
            &crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
