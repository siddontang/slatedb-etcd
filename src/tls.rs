mod auto;
mod self_signed;

use anyhow::{ensure, Context, Result};
use axum::{Extension, Router};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    service::TowerToHyperService,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, RootCertStore};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

#[derive(Clone, Default)]
pub struct Options {
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
    pub client_auth: bool,
    /// Generate a self-signed identity valid for this many years.
    pub auto_tls: Option<std::num::NonZeroU32>,
}

#[derive(Clone, Default)]
pub struct Transport {
    pub tls: Option<ClientTlsConfig>,
    pub http: reqwest::Client,
    auto_tls: bool,
}

#[derive(Clone)]
pub struct VerifiedUser(pub String);

#[derive(Clone)]
pub struct PeerServer;

#[derive(Clone)]
pub struct ClientServer;

pub fn validate_url(address: &str, allow_remote: bool, require_tls: bool) -> Result<()> {
    let url = reqwest::Url::parse(&crate::cluster::url(address))?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host().is_some()
            && url.port_or_known_default().is_some()
            && url.path() == "/"
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "invalid service URL"
    );
    ensure!(
        !require_tls || url.scheme() == "https",
        "TLS requires an HTTPS URL"
    );
    let loopback = url
        .host_str()
        .and_then(|host| {
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .ok()
        })
        .is_some_and(|ip| ip.is_loopback());
    ensure!(
        allow_remote || loopback,
        "remote URLs require --allow-remote"
    );
    Ok(())
}

impl Options {
    pub fn server(&self) -> Result<Option<Arc<rustls::ServerConfig>>> {
        ensure!(
            self.cert.is_some() == self.key.is_some(),
            "TLS requires both certificate and key"
        );
        let Some(cert) = &self.cert else {
            ensure!(
                !self.client_auth && self.ca.is_none(),
                "TLS authentication requires a certificate"
            );
            return Ok(None);
        };
        let certificates = CertificateDer::pem_file_iter(cert)?.collect::<Result<Vec<_>, _>>()?;
        let key = PrivateKeyDer::from_pem_file(self.key.as_ref().unwrap())
            .context("cannot read TLS private key")?;
        let config = rustls::ServerConfig::builder();
        let config = if self.client_auth {
            let ca = self
                .ca
                .as_ref()
                .context("client certificate authentication requires a CA")?;
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_file_iter(ca)? {
                roots.add(cert?)?;
            }
            config.with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?,
            )
        } else {
            config.with_no_client_auth()
        };
        let mut config = config.with_single_cert(certificates, key)?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Some(Arc::new(config)))
    }

    pub fn transport(&self) -> Result<Transport> {
        if self.cert.is_none() {
            return Ok(Transport::default());
        }
        if self.auto_tls.is_some() {
            let config = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(self_signed::SelfSigned))
                .with_no_client_auth();
            let http = reqwest::Client::builder()
                .https_only(true)
                .tls_backend_preconfigured(config)
                .build()?;
            return Ok(Transport {
                tls: Some(ClientTlsConfig::new()),
                http,
                auto_tls: true,
            });
        }
        let ca = std::fs::read(self.ca.as_ref().context("peer TLS requires a trusted CA")?)?;
        let cert = std::fs::read(self.cert.as_ref().unwrap())?;
        let key = std::fs::read(self.key.as_ref().context("peer TLS requires a key")?)?;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca.clone()))
            .identity(Identity::from_pem(cert.clone(), key.clone()));
        let identity = reqwest::Identity::from_pem(&[cert, key].concat())?;
        let http = reqwest::Client::builder()
            .https_only(true)
            .tls_certs_merge([reqwest::Certificate::from_pem(&ca)?])
            .identity(identity)
            .build()?;
        Ok(Transport {
            tls: Some(tls),
            auto_tls: false,
            http,
        })
    }
}

impl Transport {
    pub fn channel(&self, address: &str) -> Result<Channel> {
        if self.auto_tls {
            validate_url(address, false, true)?;
        }
        let mut endpoint =
            Endpoint::from_shared(crate::cluster::url(address))?.timeout(Duration::from_secs(10));
        if let Some(tls) = &self.tls {
            ensure!(
                address.starts_with("https://"),
                "peer TLS cannot use a plaintext peer URL"
            );
            endpoint = if self.auto_tls {
                endpoint.tls_config_with_verifier(tls.clone(), Arc::new(self_signed::SelfSigned))?
            } else {
                endpoint.tls_config(tls.clone())?
            };
        }
        Ok(endpoint.connect_lazy())
    }
}

pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<()> {
    serve_until(listener, app, tls, std::future::pending()).await
}

pub(crate) async fn serve_until(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<rustls::ServerConfig>>,
    stop: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let acceptor = tls.map(tokio_rustls::TlsAcceptor::from);
    let permits = Arc::new(tokio::sync::Semaphore::new(1024));
    let mut connections = tokio::task::JoinSet::new();
    tokio::pin!(stop);
    let result = loop {
        let incoming = tokio::select! {
            _ = &mut stop => break Ok(()),
            _ = connections.join_next(), if !connections.is_empty() => continue,
            incoming = listener.accept() => incoming,
        };
        let (stream, _) = match incoming {
            Ok(incoming) => incoming,
            Err(error) => break Err(error.into()),
        };
        // Send small client and Raft replies without waiting for a delayed ACK.
        if let Err(error) = stream.set_nodelay(true) {
            break Err(error.into());
        }
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue;
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        connections.spawn(async move {
            let _permit = permit;
            let Some(acceptor) = acceptor else {
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(
                        TokioIo::new(stream),
                        TowerToHyperService::new(app),
                    )
                    .await;
                return;
            };
            let stream =
                match tokio::time::timeout(Duration::from_secs(5), acceptor.accept(stream)).await {
                    Ok(Ok(stream)) => stream,
                    _ => return,
                };
            // Rustls verifies the certificate chain before exposing its subject.
            let user = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
                .and_then(|cert| x509_parser::parse_x509_certificate(cert.as_ref()).ok())
                .and_then(|(_, cert)| {
                    cert.subject()
                        .iter_common_name()
                        .next()
                        .and_then(|cn| cn.as_str().ok())
                        .map(str::to_owned)
                });
            let app = if let Some(user) = user {
                app.layer(Extension(VerifiedUser(user)))
            } else {
                app
            };
            let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), TowerToHyperService::new(app))
                .await;
        });
    };
    drop(listener);
    connections.shutdown().await;
    result
}

#[cfg(test)]
mod url_tests {
    use super::validate_url;

    #[test]
    fn remote_urls_require_tls_and_local_urls_use_literal_loopback() {
        assert!(validate_url("127.0.0.1:2379", false, false).is_ok());
        assert!(validate_url("http://[::1]:2379", false, false).is_ok());
        assert!(validate_url("https://node.example:2379", true, true).is_ok());
        for url in [
            "http://node.example",
            "https://user:password@node.example",
            "https://node.example/path",
            "https://node.example?token=value",
        ] {
            assert!(validate_url(url, true, true).is_err());
        }
        assert!(validate_url("http://localhost:2379", false, false).is_err());
        assert!(validate_url("http://192.0.2.1:2379", false, false).is_err());
    }
}
