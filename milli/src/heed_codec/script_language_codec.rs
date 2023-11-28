use std::borrow::Cow;
use std::ffi::CStr;
use std::str;

use charabia::{Language, Script};
use heed::BoxedError;

pub struct ScriptLanguageCodec;

impl<'a> heed::BytesDecode<'a> for ScriptLanguageCodec {
    type DItem = (Script, Language);

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let cstr = CStr::from_bytes_until_nul(bytes)?;
        let script = cstr.to_str()?;
        let script_name = Script::from_name(script);
        // skip '\0' byte between the two strings.
        let lan = str::from_utf8(&bytes[script.len() + 1..])?;
        let lan_name = Language::from_name(lan);

        Ok((script_name, lan_name))
    }
}

impl<'a> heed::BytesEncode<'a> for ScriptLanguageCodec {
    type EItem = (Script, Language);

    fn bytes_encode((script, lan): &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let script_name = script.name().as_bytes();
        let lan_name = lan.name().as_bytes();

        let mut bytes = Vec::with_capacity(script_name.len() + lan_name.len() + 1);
        bytes.extend_from_slice(script_name);
        bytes.push(0);
        bytes.extend_from_slice(lan_name);

        Ok(Cow::Owned(bytes))
    }
}
