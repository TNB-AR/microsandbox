//! Integration test for bind mounts whose source directory is owned by
//! another user.
//!
//! Reproducer for the bug fixed in this PR: `PassthroughFs::build()` in
//! strict mode probes xattr support by writing
//! `setxattr user.containers._probe` on the source directory. On Linux,
//! writing a `user.*` xattr requires owning the file or holding
//! CAP_SYS_ADMIN, so binding any directory the calling user doesn't own
//! (`/tmp`, `/var`, `/etc`, …) failed up front with EPERM. The fix
//! turns strict off for the user-bind-mount path in
//! `crates/runtime/lib/vm.rs`.
//!
//! Pre-fix expectation (vm.rs unchanged): create-sandbox errored with
//!   io error: mount <tag>: Operation not permitted (os error 1)
//!
//! Post-fix expectation: sandbox boots cleanly; bind contents are
//! visible inside the guest; readonly is enforced at the kernel level.

use microsandbox::Sandbox;
use test_utils::msb_test;

const IMAGE: &str = "mirror.gcr.io/library/alpine";

async fn cleanup(name: &str) {
    if let Ok(mut h) = Sandbox::get(name).await {
        let _ = h.kill().await;
        let _ = h.remove().await;
    }
}

/// Bind `/tmp` (root-owned on every standard Linux) readonly into the
/// guest. With strict-xattr enabled on user mounts, this would error
/// out during sandbox start because the calling user can't write
/// `user.*` xattrs to a directory they don't own.
#[msb_test]
async fn bind_mount_of_foreign_owned_directory_succeeds() {
    let name = "bind-foreign-owned";
    cleanup(name).await;

    let sb = Sandbox::builder(name)
        .image(IMAGE)
        .cpus(1)
        .memory(256)
        .replace()
        .volume("/host-tmp", |m| m.bind("/tmp").readonly())
        .create()
        .await
        .expect("create sandbox with bind of /tmp");

    // The mount point should exist and be enumerable.
    let ls = sb
        .shell("ls -1 /host-tmp >/dev/null 2>&1; echo $?")
        .await
        .expect("ls /host-tmp");
    assert_eq!(
        ls.stdout().expect("utf8").trim(),
        "0",
        "expected ls /host-tmp to succeed; got {ls:?}"
    );

    // Readonly is enforced by the guest kernel: touch should fail with
    // "Read-only file system".
    let write = sb
        .shell("touch /host-tmp/probe-write 2>&1; true")
        .await
        .expect("touch attempt");
    let combined = format!(
        "{}{}",
        write.stdout().unwrap_or_default(),
        write.stderr().unwrap_or_default()
    );
    assert!(
        combined.to_lowercase().contains("read-only"),
        "expected read-only error, got: {combined:?}"
    );

    sb.stop_and_wait().await.expect("stop");
    let _ = Sandbox::remove(name).await;
}
