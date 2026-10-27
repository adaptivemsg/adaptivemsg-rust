//! The Go runtime's interop_golden_test.go encodes the same messages and
//! checks them against the same bytes, so a change on either side that breaks
//! the other shows up in both test suites.

use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::__private::serde_bytes::ByteBuf;
use crate::codec::CodecID;
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::codec_registry::codec_by_id;
use crate::message::{Message, MessageDecode};
use crate::raw_message::{decode_raw_as, RawMessage};

const GOLDEN_COMPACT: &str =
    "97b2616d2e696e7465726f702e4d657373616765a16ec40301020392a161a162c092a174c3cb3fe0000000000000";
const GOLDEN_MAP: &str = "82a474797065b2616d2e696e7465726f702e4d657373616765a46461746186a46e616d65a16ea464617461c403010203a47461677392a161a162a46e6f7465c0a5696e6e657282a474657874a174a4666c6167c3a5726174696fcb3fe0000000000000";

const GOLDEN_BLOB_COMPACT: &str =
    "95af616d2e696e7465726f702e426c6f62c4020102c4010492c40103c40081a16bc4020506";
const GOLDEN_BLOB_MAP: &str = "82a474797065af616d2e696e7465726f702e426c6f62a46461746184a464617461c4020102a56d61796265c40104a66368756e6b7392c40103c400a762795f6e616d6581a16bc4020506";

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct InteropInner {
    text: String,
    flag: bool,
}

#[crate::message(name = "interop.Message")]
#[derive(Debug, PartialEq)]
struct InteropMessage {
    name: String,
    #[serde(with = "crate::__private::serde_bytes")]
    data: Vec<u8>,
    tags: Vec<String>,
    note: Option<String>,
    inner: InteropInner,
    ratio: f64,
}

fn golden_message() -> InteropMessage {
    InteropMessage {
        name: "n".into(),
        data: vec![1, 2, 3],
        tags: vec!["a".into(), "b".into()],
        note: None,
        inner: InteropInner {
            text: "t".into(),
            flag: true,
        },
        ratio: 0.5,
    }
}

/// The byte fields in the form amgen-go generates them.
#[crate::message(name = "interop.Blob")]
#[derive(Debug, PartialEq)]
struct InteropBlob {
    #[serde(with = "crate::__private::serde_bytes")]
    data: Vec<u8>,
    #[serde(with = "crate::__private::serde_bytes")]
    maybe: Option<Vec<u8>>,
    chunks: Vec<ByteBuf>,
    by_name: BTreeMap<String, ByteBuf>,
}

fn golden_blob() -> InteropBlob {
    InteropBlob {
        data: vec![1, 2],
        maybe: Some(vec![4]),
        chunks: vec![ByteBuf::from(vec![3]), ByteBuf::new()],
        by_name: BTreeMap::from([("k".to_string(), ByteBuf::from(vec![5, 6]))]),
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex digit"))
        .collect()
}

fn check_golden<T>(want: T, compact: &str, map: &str)
where
    T: Message + MessageDecode + PartialEq + Debug + 'static,
{
    let cases: [(CodecID, &str); 2] = [(CodecMsgpackCompact, compact), (CodecMsgpackMap, map)];
    for (codec_id, golden) in cases {
        let codec = codec_by_id(codec_id).expect("codec registered");
        let payload = codec.encode(&want).expect("encode");
        assert_eq!(
            to_hex(&payload),
            golden,
            "{} encoding changed",
            codec_id.name()
        );

        let envelope = codec
            .decode_envelope(&from_hex(golden))
            .expect("decode envelope");
        let raw = RawMessage {
            wire: envelope.wire,
            codec,
            body: envelope.body,
        };
        let decoded: T = decode_raw_as(raw).expect("decode");
        assert_eq!(decoded, want, "{} decoding", codec_id.name());
    }
}

#[test]
fn interop_golden_encoding() {
    check_golden(golden_message(), GOLDEN_COMPACT, GOLDEN_MAP);
}

#[test]
fn interop_golden_byte_fields() {
    check_golden(golden_blob(), GOLDEN_BLOB_COMPACT, GOLDEN_BLOB_MAP);
}
