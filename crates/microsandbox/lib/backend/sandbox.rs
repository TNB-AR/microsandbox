//! Sandbox lifecycle backend trait.
//!
//! This is the first resource-specific dispatch surface under [`super::Backend`].
//! It intentionally returns bridge enums while the public `Sandbox` /
//! `SandboxHandle` migration is still pending: local implementations delegate to
//! today's SDK types, and cloud implementations return msb-cloud wire records.

use futures::future::BoxFuture;

use super::cloud_wire::{CloudCreateSandboxRequest, CloudSandbox};
use super::{CloudBackend, LocalBackend};
use crate::sandbox::{RootfsSource, Sandbox, SandboxConfig, SandboxHandle};
use crate::{MicrosandboxError, MicrosandboxResult};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// A backend-created sandbox instance.
///
/// Variants are boxed so the enum's size is bounded by the discriminant +
/// pointer rather than the largest variant — `Sandbox` carries a runtime
/// handle that dwarfs the cloud record.
pub enum BackendSandbox {
    /// Local libkrun-backed sandbox.
    Local(Box<Sandbox>),
    /// Cloud msb-cloud sandbox record.
    Cloud(Box<CloudSandbox>),
}

/// A backend sandbox handle/list record.
///
/// Variants are boxed for the same reason as [`BackendSandbox`].
pub enum BackendSandboxHandle {
    /// Local persisted sandbox handle.
    Local(Box<SandboxHandle>),
    /// Cloud msb-cloud sandbox record.
    Cloud(Box<CloudSandbox>),
}

/// Paginated sandbox list result.
pub struct BackendSandboxList {
    /// Returned sandbox records.
    pub sandboxes: Vec<BackendSandboxHandle>,
    /// Cursor for the next cloud page, when one exists.
    pub next_cursor: Option<String>,
}

/// Resource-specific backend for sandbox lifecycle operations.
pub trait SandboxBackend: Send + Sync {
    /// Create a sandbox. Cloud honours `start`; local creation boots immediately.
    fn create<'a>(
        &'a self,
        config: SandboxConfig,
        start: bool,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandbox>>;

    /// List sandboxes. Local ignores pagination; cloud passes it through.
    fn list<'a>(
        &'a self,
        cursor: Option<&'a str>,
        limit: Option<u32>,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandboxList>>;

    /// Get a sandbox by its user-facing name.
    fn get_by_name<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandboxHandle>>;

    /// Start a stopped sandbox by name.
    fn start<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<BackendSandbox>>;

    /// Stop a running sandbox by name.
    fn stop<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<()>>;

    /// Destroy/remove a sandbox by name.
    fn destroy<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<()>>;
}

//--------------------------------------------------------------------------------------------------
// Trait Implementations
//--------------------------------------------------------------------------------------------------

impl SandboxBackend for LocalBackend {
    fn create<'a>(
        &'a self,
        config: SandboxConfig,
        _start: bool,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandbox>> {
        Box::pin(async move {
            Sandbox::create(config)
                .await
                .map(|sb| BackendSandbox::Local(Box::new(sb)))
        })
    }

    fn list<'a>(
        &'a self,
        _cursor: Option<&'a str>,
        _limit: Option<u32>,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandboxList>> {
        Box::pin(async move {
            let sandboxes = Sandbox::list()
                .await?
                .into_iter()
                .map(|h| BackendSandboxHandle::Local(Box::new(h)))
                .collect();
            Ok(BackendSandboxList {
                sandboxes,
                next_cursor: None,
            })
        })
    }

    fn get_by_name<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandboxHandle>> {
        Box::pin(async move {
            Sandbox::get(name)
                .await
                .map(|h| BackendSandboxHandle::Local(Box::new(h)))
        })
    }

    fn start<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<BackendSandbox>> {
        Box::pin(async move {
            Sandbox::start(name)
                .await
                .map(|sb| BackendSandbox::Local(Box::new(sb)))
        })
    }

    fn stop<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<()>> {
        Box::pin(async move { Sandbox::get(name).await?.stop().await })
    }

    fn destroy<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<()>> {
        Box::pin(async move { Sandbox::remove(name).await })
    }
}

impl SandboxBackend for CloudBackend {
    fn create<'a>(
        &'a self,
        config: SandboxConfig,
        start: bool,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandbox>> {
        Box::pin(async move {
            let req = cloud_create_request_from_config(config)?;
            CloudBackend::create_sandbox(self, &req, start)
                .await
                .map(|sb| BackendSandbox::Cloud(Box::new(sb)))
        })
    }

    fn list<'a>(
        &'a self,
        cursor: Option<&'a str>,
        limit: Option<u32>,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandboxList>> {
        Box::pin(async move {
            let page = CloudBackend::list_sandboxes(self, cursor, limit).await?;
            Ok(BackendSandboxList {
                sandboxes: page
                    .data
                    .into_iter()
                    .map(|sb| BackendSandboxHandle::Cloud(Box::new(sb)))
                    .collect(),
                next_cursor: page.next_cursor,
            })
        })
    }

    fn get_by_name<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, MicrosandboxResult<BackendSandboxHandle>> {
        Box::pin(async move {
            CloudBackend::get_sandbox(self, name)
                .await
                .map(|sb| BackendSandboxHandle::Cloud(Box::new(sb)))
        })
    }

    fn start<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<BackendSandbox>> {
        Box::pin(async move {
            CloudBackend::start_sandbox(self, name)
                .await
                .map(|sb| BackendSandbox::Cloud(Box::new(sb)))
        })
    }

    fn stop<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<()>> {
        Box::pin(async move {
            CloudBackend::stop_sandbox(self, name).await?;
            Ok(())
        })
    }

    fn destroy<'a>(&'a self, name: &'a str) -> BoxFuture<'a, MicrosandboxResult<()>> {
        Box::pin(async move {
            CloudBackend::destroy_sandbox(self, name).await?;
            Ok(())
        })
    }
}

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

fn cloud_create_request_from_config(
    config: SandboxConfig,
) -> MicrosandboxResult<CloudCreateSandboxRequest> {
    reject_cloud_deferred(!config.mounts.is_empty(), "mounts", "Phase 6 (volumes)")?;
    reject_cloud_deferred(!config.patches.is_empty(), "patches", "Phase 6")?;
    reject_cloud_deferred(
        !config.rlimits.is_empty(),
        "rlimits",
        "D13 rlimits expansion",
    )?;
    reject_cloud_deferred(config.cmd.is_some(), "cmd", "D13 cmd expansion")?;

    let image = match config.image {
        RootfsSource::Oci(image) => image,
        RootfsSource::Bind(_) => {
            return Err(unsupported("image-from-host-dir", "Phase 6"));
        }
        RootfsSource::DiskImage { .. } => {
            return Err(unsupported("disk-image rootfs", "D4: cloud-unsupported v1"));
        }
    };

    Ok(CloudCreateSandboxRequest {
        name: config.name,
        image,
        vcpus: config.cpus,
        memory_mib: config.memory_mib,
        env: config.env.into_iter().collect(),
        ephemeral: true,
        workdir: config.workdir,
        shell: config.shell,
        entrypoint: config.entrypoint,
        hostname: config.hostname,
        user: config.user,
        log_level: config.log_level.map(log_level_to_cloud),
        scripts: config.scripts,
        max_duration_secs: config.policy.max_duration_secs,
        idle_timeout_secs: config.policy.idle_timeout_secs,
    })
}

fn reject_cloud_deferred(
    present: bool,
    feature: &'static str,
    available_in_phase: &'static str,
) -> MicrosandboxResult<()> {
    if present {
        return Err(unsupported(feature, available_in_phase));
    }
    Ok(())
}

fn unsupported(feature: &'static str, available_in_phase: &'static str) -> MicrosandboxError {
    MicrosandboxError::Unsupported {
        feature: feature.into(),
        available_in_phase: available_in_phase.into(),
    }
}

fn log_level_to_cloud(level: microsandbox_runtime::logging::LogLevel) -> String {
    match level {
        microsandbox_runtime::logging::LogLevel::Error => "error",
        microsandbox_runtime::logging::LogLevel::Warn => "warn",
        microsandbox_runtime::logging::LogLevel::Info => "info",
        microsandbox_runtime::logging::LogLevel::Debug => "debug",
        microsandbox_runtime::logging::LogLevel::Trace => "trace",
    }
    .to_string()
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::SandboxBuilder;

    #[tokio::test]
    async fn cloud_create_request_maps_common_fields() {
        let config = SandboxBuilder::new("agent-1")
            .image("python:3.12")
            .cpus(2)
            .memory(1024)
            .env("A", "B")
            .workdir("/app")
            .shell("/bin/bash")
            .entrypoint(["python", "-u"])
            .build()
            .await
            .unwrap();

        let req = cloud_create_request_from_config(config).unwrap();

        assert_eq!(req.name, "agent-1");
        assert_eq!(req.image, "python:3.12");
        assert_eq!(req.vcpus, 2);
        assert_eq!(req.memory_mib, 1024);
        assert_eq!(req.env["A"], "B");
        assert_eq!(req.workdir.as_deref(), Some("/app"));
        assert_eq!(req.shell.as_deref(), Some("/bin/bash"));
        assert_eq!(
            req.entrypoint,
            Some(vec!["python".to_string(), "-u".to_string()])
        );
    }

    #[tokio::test]
    async fn cloud_create_request_rejects_disk_image_rootfs() {
        let config = SandboxConfig {
            name: "agent-1".into(),
            image: RootfsSource::DiskImage {
                path: "rootfs.img".into(),
                format: crate::sandbox::DiskImageFormat::Raw,
                fstype: None,
            },
            ..Default::default()
        };

        let err = cloud_create_request_from_config(config).unwrap_err();
        assert!(matches!(err, MicrosandboxError::Unsupported { .. }));
    }
}
