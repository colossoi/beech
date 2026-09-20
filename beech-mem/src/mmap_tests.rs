#![allow(unused_imports)]
use super::*;
use std::io::Write;

#[test]
fn reads_back_what_was_written() {
    let mut tmp = beech_disk::NamedTempFile::new().unwrap();
    let payload = b"the quick brown fox jumps over the lazy dog";
    tmp.write_all(payload).unwrap();
    tmp.flush().unwrap();
    let file = std::fs::File::open(tmp.path()).unwrap();
    let buf = MappedBuffer::from_file(&file).unwrap();
    let bytes: &[u8] = AsRef::<[u8]>::as_ref(&*buf);
    assert_eq!(bytes, payload);
    assert_eq!(buf.len(), payload.len());
}
