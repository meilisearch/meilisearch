use std::borrow::Cow;
use std::str;

use charabia::{Language, Script};

pub struct ScriptLanguageCodec;

impl<'a> heed::BytesDecode<'a> for ScriptLanguageCodec {
    type DItem = (Script, Language);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let sep = bytes.iter().position(|b| *b == 0)?;
        let (s_bytes, l_bytes) = bytes.split_at(sep);
        let script = str::from_utf8(s_bytes).ok()?;
        let script_name = Script::from_name(script);
        let lan = str::from_utf8(l_bytes).ok()?;
        let lan_name = Language::from_name(lan);

        Some((script_name, lan_name))
    }
}

impl<'a> heed::BytesEncode<'a> for ScriptLanguageCodec {
    type EItem = (Script, Language);

    fn bytes_encode((script, lan): &Self::EItem) -> Option<Cow<[u8]>> {
        let script_name = script.name().as_bytes();
        let lan_name = lan.name().as_bytes();

        let mut bytes = Vec::with_capacity(script_name.len() + lan_name.len() + 1);
        bytes.extend_from_slice(script_name);
        bytes.push(0);
        bytes.extend_from_slice(lan_name);

        Some(Cow::Owned(bytes))
    }
}
