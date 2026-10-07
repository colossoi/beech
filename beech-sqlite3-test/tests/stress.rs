use std::{
    fs,
    process::Command,
    time::{Duration, Instant},
};

#[test]
fn stress_runs_differential_workload_and_cleans_up() {
    let parent = beech_disk::Workspace::new().unwrap();
    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_sqlite-stress"))
        .args([
            "--duration-secs",
            "3",
            "--initial-rows",
            "10",
            "--tables",
            "3",
            "--max-disk-mib",
            "64",
            "--seed",
            "42",
            "--directory",
        ])
        .arg(parent.path())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("initialized table=2"), "{stdout}");
    assert!(stdout.contains("verified_steps="), "{stdout}");
    assert!(stdout.contains("stopped: duration"), "{stdout}");
    assert!(start.elapsed() < Duration::from_secs(15));
    assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
}

#[test]
fn tiny_disk_budget_interrupts_setup_and_cleans_up() {
    let parent = beech_disk::Workspace::new().unwrap();
    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_sqlite-stress"))
        .args(["--duration-secs", "60", "--max-disk-mib", "1", "--directory"])
        .arg(parent.path())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("stopped: disk budget"), "{stdout}");
    // A sample can catch a write beyond the budget: that must be reported as failure.
    if !output.status.success() {
        assert!(String::from_utf8_lossy(&output.stderr).contains("disk budget exceeded"));
    }
    assert!(start.elapsed() < Duration::from_secs(15));
    assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
}

#[test]
fn keep_preserves_only_the_isolated_run_directory() {
    let parent = beech_disk::Workspace::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sqlite-stress"))
        .args([
            "--duration-secs",
            "1",
            "--initial-rows",
            "2",
            "--keep",
            "--directory",
        ])
        .arg(parent.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let dirs = fs::read_dir(parent.path()).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(dirs.len(), 1);
    let run = dirs[0].path();
    assert!(run.join("stress.sqlite").exists());
    assert!(run.join("repo/root").exists());
    assert!(!run.join("repo-0").exists());
    let repository =
        beech_core::storage::Repository::new(beech_core::storage::FileStore::new(run.join("repo")));
    let root = beech_core::Id::from_hex(fs::read_to_string(run.join("repo/root")).unwrap().trim()).unwrap();
    let repository = std::sync::Arc::new(repository);
    let snapshot = repository.snapshot(root).unwrap();
    assert_eq!(snapshot.transaction().tables().len(), 4);
    for table in 0..4 {
        assert!(snapshot.table(&format!("data{table}")).is_ok());
    }
}
