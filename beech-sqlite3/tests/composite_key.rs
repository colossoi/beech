mod common;
use common::*;

// BUG: WHERE a = 2 AND b = 1 on a two-part key returns the first row
// matching the prefix (b=0) instead of the exact match (b=1).
#[test]
#[ignore]
fn two_part_key_round_trip() {
    let rows: Vec<_> = (0..5)
        .flat_map(|a| {
            (0..4).map(move |b| {
                let row_id = (a * 10 + b) as i64;
                (row_id, two_part_record(a, b, a * 100 + b))
            })
        })
        .collect();
    let tmp = make_test_tree(rows, vec![0, 1], "t");
    let conn = setup_vtab(tmp.path(), "t", "tt");

    // Full scan should return all 20 rows.
    let count: i64 = conn.query_row("SELECT count(*) FROM tt", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 20);

    // Point lookup on first key part.
    let mut stmt = conn.prepare("SELECT a, b, v FROM tt WHERE a = 3").unwrap();
    let results: Vec<(i32, i32, i32)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 4);
    for (a, b, v) in &results {
        assert_eq!(*a, 3);
        assert_eq!(*v, 3 * 100 + b);
    }

    // Both key parts.
    let (a, b, v): (i32, i32, i32) = conn
        .query_row("SELECT a, b, v FROM tt WHERE a = 2 AND b = 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(a, 2);
    assert_eq!(b, 1);
    assert_eq!(v, 201);
}
