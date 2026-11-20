//! Generate a per-listener identity and publish the key and certificate together.

use super::Options;
use anyhow::{ensure, Context, Result};
use rcgen::{CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair};
use std::fs::{self, File};
use std::io::Write;
use std::net::IpAddr;
use std::path::Path;
use time::{Duration, OffsetDateTime};

impl Options {
    pub(crate) fn prepare(&mut self, dir: &Path, role: &str, ip: IpAddr) -> Result<()> {
        let Some(years) = self.auto_tls else {
            return Ok(());
        };
        if self.cert.is_some() || self.key.is_some() {
            // Explicit credentials take precedence, as in etcd.
            self.auto_tls = None;
            return Ok(());
        }
        ensure!(
            !self.client_auth && self.ca.is_none(),
            "AutoTLS cannot verify client identities"
        );
        let fixtures = dir.join("fixtures");
        let target = fixtures.join(role);
        let cert = target.join("cert.pem");
        let key = target.join("key.pem");
        if !cert.is_file() || !key.is_file() {
            fs::create_dir_all(&fixtures)?;
            let stage = tempfile::Builder::new()
                .prefix(".tls-")
                .tempdir_in(&fixtures)?;
            let now = OffsetDateTime::now_utc();
            let mut params = CertificateParams::new(vec![ip.to_string()])?;
            params.not_before = now;
            params.not_after = now
                .checked_add(Duration::days(i64::from(years.get()) * 365))
                .context("certificate validity exceeds the supported date range")?;
            params.distinguished_name = DistinguishedName::new();
            params
                .distinguished_name
                .push(DnType::OrganizationName, "etcd");
            params
                .extended_key_usages
                .push(ExtendedKeyUsagePurpose::ServerAuth);
            let pair = KeyPair::generate()?;
            let pem = params.self_signed(&pair)?.pem();
            for (name, data) in [("key.pem", pair.serialize_pem()), ("cert.pem", pem)] {
                let mut options = File::options();
                options.write(true).create_new(true);
                #[cfg(unix)]
                std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
                let mut file = options.open(stage.path().join(name))?;
                file.write_all(data.as_bytes())?;
                file.sync_all()?;
            }
            File::open(stage.path())?.sync_all()?;
            if let Err(error) = fs::rename(stage.path(), &target) {
                // A concurrent start can publish a complete identity before this one.
                if !cert.is_file() || !key.is_file() {
                    return Err(error).context("cannot publish the AutoTLS identity");
                }
            }
            File::open(fixtures)?.sync_all()?;
        }
        self.cert = Some(cert);
        self.key = Some(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_identities_are_private_stable_and_separate() {
        let dir = tempfile::tempdir().unwrap();
        let ip = "127.0.0.1".parse().unwrap();
        let prepare = |role| {
            let mut options = Options {
                auto_tls: std::num::NonZeroU32::new(1),
                ..Default::default()
            };
            options.prepare(dir.path(), role, ip).unwrap();
            let key = options.key.unwrap();
            #[cfg(unix)]
            assert_eq!(
                std::os::unix::fs::PermissionsExt::mode(&fs::metadata(&key).unwrap().permissions())
                    & 0o077,
                0
            );
            (
                fs::read(options.cert.unwrap()).unwrap(),
                fs::read(key).unwrap(),
            )
        };
        let (client, peer) = std::thread::scope(|scope| {
            let a = scope.spawn(|| prepare("client"));
            let b = scope.spawn(|| prepare("client"));
            let p = prepare("peer");
            let a = a.join().unwrap();
            assert_eq!(a, b.join().unwrap());
            (a, p)
        });
        assert_eq!(client, prepare("client"));
        assert_ne!(client.0, peer.0);
        assert_ne!(client.1, peer.1);
    }

    #[test]
    fn explicit_identity_keeps_its_configured_trust() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = Options {
            cert: Some(dir.path().join("managed.pem")),
            key: Some(dir.path().join("managed.key")),
            auto_tls: std::num::NonZeroU32::new(1),
            ..Default::default()
        };
        options
            .prepare(dir.path(), "peer", "127.0.0.1".parse().unwrap())
            .unwrap();
        assert!(options.auto_tls.is_none());
        assert!(!dir.path().join("fixtures").exists());
    }
}
