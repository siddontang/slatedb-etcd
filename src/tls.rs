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
use tokio_rustls::rustls::{self, RootCertStore};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

#[derive(Clone, Default)]
pub struct Options {
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
    pub client_auth: bool,
}

#[derive(Clone, Default)]
pub struct Transport {
    pub tls: Option<ClientTlsConfig>,
    pub http: reqwest::Client,
}

#[derive(Clone)]
pub struct VerifiedUser(pub String);

#[derive(Clone)]
pub struct PeerServer;

#[derive(Clone)]
pub struct ClientServer;

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
        let cert_bytes = std::fs::read(cert)?;
        let certificates = rustls_pemfile::certs(&mut cert_bytes.as_slice())
            .collect::<std::io::Result<Vec<_>>>()?;
        let key = rustls_pemfile::private_key(
            &mut std::fs::read(self.key.as_ref().unwrap())?.as_slice(),
        )?
        .context("TLS private key is missing")?;
        let config = rustls::ServerConfig::builder();
        let config = if self.client_auth {
            let ca = self
                .ca
                .as_ref()
                .context("client certificate authentication requires a CA")?;
            let mut roots = RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut std::fs::read(ca)?.as_slice()) {
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
        let ca = std::fs::read(self.ca.as_ref().context("peer TLS requires a trusted CA")?)?;
        let cert = std::fs::read(self.cert.as_ref().unwrap())?;
        let key = std::fs::read(self.key.as_ref().context("peer TLS requires a key")?)?;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca.clone()))
            .identity(Identity::from_pem(cert.clone(), key.clone()));
        let identity = reqwest::Identity::from_pem(&[cert, key].concat())?;
        let http = reqwest::Client::builder()
            .https_only(true)
            .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
            .identity(identity)
            .build()?;
        Ok(Transport {
            tls: Some(tls),
            http,
        })
    }
}

impl Transport {
    pub fn channel(&self, address: &str) -> Result<Channel> {
        let mut endpoint =
            Endpoint::from_shared(crate::cluster::url(address))?.timeout(Duration::from_secs(10));
        if let Some(tls) = &self.tls {
            ensure!(
                address.starts_with("https://"),
                "peer TLS cannot use a plaintext peer URL"
            );
            endpoint = endpoint.tls_config(tls.clone())?;
        }
        Ok(endpoint.connect_lazy())
    }
}

pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<()> {
    let Some(config) = tls else {
        return axum::serve(listener, app).await.map_err(Into::into);
    };
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let permits = Arc::new(tokio::sync::Semaphore::new(1024));
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue;
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let _permit = permit;
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
    }
}
