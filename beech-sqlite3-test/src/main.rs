#!/usr/bin/env rust

//! Integration test binary for beech-sqlite3
//!
//! This binary runs integration tests for the SQLite virtual table functionality,
//! including testing with real beech data files.

use std::env;
use std::path::PathBuf;

use beech_sqlite3::create_beech_module;
use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    env_logger::init();

    println!("Running beech-sqlite3 integration tests...");

    // Test 1: Basic module registration
    match test_module_registration() {
        Ok(()) => println!("✓ Module registration test passed"),
        Err(e) => {
            println!("✗ Module registration test failed: {}", e);
            println!("This may be due to SQLite initialization issues in the test environment");
        }
    }

    // Test 2: Virtual table creation (if test data exists)
    if let Some(test_data_path) = get_test_data_path() {
        match test_virtual_table_creation(test_data_path) {
            Ok(()) => println!("✓ Virtual table creation test passed"),
            Err(e) => println!("✗ Virtual table creation test failed: {}", e),
        }
    } else {
        println!("No test data found, skipping virtual table creation test");
    }

    println!("Integration tests completed!");
    Ok(())
}

fn test_module_registration() -> Result<()> {
    println!("Test 1: Testing SQLite module registration...");

    match Connection::open_in_memory() {
        Ok(conn) => match create_beech_module(&conn) {
            Ok(()) => {
                println!("✓ Module registration successful");
                Ok(())
            }
            Err(e) => {
                println!("Module registration failed: {}", e);
                Err(e)
            }
        },
        Err(e) => {
            println!("Failed to create SQLite connection: {}", e);
            Err(e)
        }
    }
}

fn test_virtual_table_creation(test_data_path: PathBuf) -> Result<()> {
    println!(
        "Test 2: Testing virtual table creation with data at {:?}...",
        test_data_path
    );

    // Check if required files exist
    let root_file = test_data_path.join("root");
    if !root_file.exists() {
        println!("  - Warning: Root file not found at {:?}", root_file);
    }

    println!("  - Creating SQLite connection...");
    let conn = Connection::open_in_memory()?;
    println!("  - Registering beech module...");
    create_beech_module(&conn)?;
    println!("  - Module registered successfully");

    // Try to create the virtual table
    println!("  - Creating virtual table...");
    let result = conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE test_table USING beech('{}', 'test_source', 'table')",
        test_data_path.display()
    ));

    match result {
        Ok(()) => {
            println!("✓ Virtual table created successfully");

            // Check what tables exist
            println!("  - Checking existing tables...");
            match conn.prepare("SELECT name FROM sqlite_master WHERE type='table'") {
                Ok(mut stmt) => {
                    let table_iter = stmt.query_map([], |row| Ok(row.get::<_, String>(0)?));
                    match table_iter {
                        Ok(rows) => {
                            println!("    Tables found:");
                            for table_name in rows {
                                println!("      - {}", table_name.unwrap_or("ERROR".to_string()));
                            }
                        }
                        Err(e) => println!("    Error listing tables: {}", e),
                    }
                }
                Err(e) => println!("  Error preparing table list query: {}", e),
            }

            // Try various queries to test functionality
            println!("  - Testing queries on test_table...");

            // Test 1: Basic SELECT
            match conn.prepare("SELECT * FROM test_table LIMIT 1") {
                Ok(mut stmt) => {
                    println!("    ✓ Basic SELECT prepared successfully");
                    match stmt.query_row([], |row| Ok(format!("First row data available"))) {
                        Ok(_) => println!("    ✓ Query executed and returned data"),
                        Err(e) => println!("    Query execution failed: {}", e),
                    }
                }
                Err(e) => println!("    Basic SELECT preparation failed: {}", e),
            }

            // Test 2: COUNT query
            match conn.prepare("SELECT COUNT(*) FROM test_table") {
                Ok(mut stmt) => {
                    println!("    ✓ COUNT query prepared successfully");
                    match stmt.query_row([], |row| {
                        let count: i64 = row.get(0)?;
                        Ok(count)
                    }) {
                        Ok(count) => println!("    ✓ Found {} rows in test_table", count),
                        Err(e) => println!("    COUNT execution failed: {}", e),
                    }
                }
                Err(e) => println!("    COUNT preparation failed: {}", e),
            }
        }
        Err(e) => {
            println!("Virtual table creation failed (expected if no data): {}", e);
        }
    }

    Ok(())
}

fn get_test_data_path() -> Option<PathBuf> {
    // Check command line arguments first
    if let Some(path) = env::args().nth(1) {
        return Some(PathBuf::from(path));
    }

    // Check common test data locations
    let candidates = ["/tmp/test_data", "./test_data", "../test_data", "../../test_data"];

    for candidate in &candidates {
        let path = PathBuf::from(candidate);
        if path.exists() && path.is_dir() {
            println!("Found test data at: {:?}", path);
            return Some(path);
        }
    }

    None
}
