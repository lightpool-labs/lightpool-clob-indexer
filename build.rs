// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn git_commit(repo_root: &PathBuf) -> String {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(repo_root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|commit| !commit.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));

    println!("cargo:rerun-if-changed=src/main.rs");
    println!("cargo:rerun-if-changed=build/package.sh");
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join(".git/HEAD").display()
    );

    if env::var("PROFILE").ok().as_deref() != Some("release") {
        return;
    }

    if env::var("LIGHTPOOL_SKIP_PACKAGE").is_ok() {
        return;
    }

    let script = manifest_dir.join("build/package.sh");
    let git_commit = git_commit(&manifest_dir);

    if !script.exists() {
        panic!("packaging script not found: {}", script.display());
    }

    let target_dir = env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
        manifest_dir
            .join("target")
            .canonicalize()
            .unwrap_or_else(|_| manifest_dir.join("target"))
            .to_string_lossy()
            .into_owned()
    });

    let log_path = format!("{target_dir}/lightpool-clob-index-package.log");
    let wrapper = format!("nohup bash {script:?} >>{log_path:?} 2>&1 &");

    Command::new("bash")
        .arg("-c")
        .arg(wrapper)
        .env(
            "CARGO_PKG_VERSION",
            env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION"),
        )
        .env("CARGO_TARGET_DIR", &target_dir)
        .env(
            "CARGO_CFG_TARGET_OS",
            env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS"),
        )
        .env(
            "CARGO_CFG_TARGET_ARCH",
            env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH"),
        )
        .env("LIGHTPOOL_GIT_COMMIT", git_commit)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to spawn packaging script: {err}"));

    eprintln!("packaging: started in background, see {log_path}");
}
