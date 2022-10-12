use std::borrow::Cow;

use std::mem::size_of;
use std::str;

use charabia::{Language, Script};

pub struct ScriptLanguageCodec;

impl<'a> heed::BytesDecode<'a> for ScriptLanguageCodec {
    type DItem = (Script, Language);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let footer_len = size_of::<u32>();

        if bytes.len() < footer_len {
            return None;
        }

        let (script, bytes) = bytes.split_at(bytes.len() - footer_len);
        let script = str::from_utf8(script).ok()?;
        let script_name = Script::from_name(script);
        let lan = str::from_utf8(bytes).ok()?;
        let lan_name = Language::from_name(lan);

        Some((script_name, lan_name))
    }
}

impl<'a> heed::BytesEncode<'a> for ScriptLanguageCodec {
    type EItem = (Script, Language);

    fn bytes_encode((script, lan): &Self::EItem) -> Option<Cow<[u8]>> {
        let script_name = script.name();
        let lan_name = lan.name();

        let mut bytes = Vec::with_capacity(script_name.len() + lan_name.len());
        bytes.extend_from_slice(script_name.as_bytes());
        bytes.extend_from_slice(lan_name.as_bytes());

        Some(Cow::Owned(bytes))
    }
}
