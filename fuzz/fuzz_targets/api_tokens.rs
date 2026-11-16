#![no_main]
//! Request tokens: the native/compat `next` cursor, the GraphQL page and block cursors and
//! the Relay node id. Property: never panics; Err or a well-formed value (R-P5P9-25: cursors
//! carry no checksum, so a mutated valid token may be another valid one).

use kdx_api::Cursor;
use kdx_graphql::connection::{BlockCursor, PageCursor};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    if let Ok(c) = Cursor::decode(&s) {
        // A decoded token re-encodes to one that decodes to the same cursor.
        let again = Cursor::decode(&c.encode()).expect("encoded cursor decodes");
        assert_eq!(again.encode(), c.encode());
    }
    if let Ok(c) = BlockCursor::decode(&s) {
        assert_eq!(BlockCursor::decode(&c.encode()).unwrap(), c);
    }
    if let Ok(c) = PageCursor::decode(&s) {
        let _ = PageCursor::decode(&c.encode()).expect("encoded cursor decodes");
    }
    if let Ok(k) = kdx_graphql::ids::decode(&s) {
        let _ = kdx_graphql::ids::decode(&kdx_graphql::ids::encode(&k)).expect("id round trip");
    }
});
