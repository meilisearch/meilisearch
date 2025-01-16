use deserr::Deserr;
use milli::LocalizedAttributesRule;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Deserr, Serialize, Deserialize, ToSchema)]
#[deserr(rename_all = camelCase)]
#[serde(rename_all = "camelCase")]
pub struct LocalizedAttributesRuleView {
    pub attribute_patterns: Vec<String>,
    pub locales: Vec<Locale>,
}

impl From<LocalizedAttributesRule> for LocalizedAttributesRuleView {
    fn from(rule: LocalizedAttributesRule) -> Self {
        Self {
            attribute_patterns: rule.attribute_patterns,
            locales: rule.locales.into_iter().map(|l| l.into()).collect(),
        }
    }
}

impl From<LocalizedAttributesRuleView> for LocalizedAttributesRule {
    fn from(view: LocalizedAttributesRuleView) -> Self {
        Self {
            attribute_patterns: view.attribute_patterns,
            locales: view.locales.into_iter().map(|l| l.into()).collect(),
        }
    }
}

/// Generate a Locale enum and its From and Into implementations for milli::tokenizer::Language.
///
/// this enum implements `Deserr` in order to be used in the API.
macro_rules! make_locale {
    ($(($iso_639_1:ident, $iso_639_1_str:expr) => ($iso_639_3:ident, $iso_639_3_str:expr),)+) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Deserr, Serialize, Deserialize, Ord, PartialOrd, ToSchema)]
        #[deserr(rename_all = camelCase)]
        #[serde(rename_all = "camelCase")]
        pub enum Locale {
            $($iso_639_1,)+
            $($iso_639_3,)+
            Cmn,
        }

        impl From<milli::tokenizer::Language> for Locale {
            fn from(other: milli::tokenizer::Language) -> Locale {
                match other {
                    $(milli::tokenizer::Language::$iso_639_3 => Locale::$iso_639_3,)+
                    milli::tokenizer::Language::Cmn => Locale::Cmn,
                }
            }
        }

        impl From<Locale> for milli::tokenizer::Language {
            fn from(other: Locale) -> milli::tokenizer::Language {
                match other {
                    $(Locale::$iso_639_1 => milli::tokenizer::Language::$iso_639_3,)+
                    $(Locale::$iso_639_3 => milli::tokenizer::Language::$iso_639_3,)+
                    Locale::Cmn => milli::tokenizer::Language::Cmn,
                }
            }
        }

        impl std::str::FromStr for Locale {
            type Err = LocaleFormatError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let locale = match s {
                    $($iso_639_1_str => Locale::$iso_639_1,)+
                    $($iso_639_3_str => Locale::$iso_639_3,)+
                    "cmn" => Locale::Cmn,
                    _ => return Err(LocaleFormatError { invalid_locale: s.to_string() }),
                };

                Ok(locale)
            }
        }

        #[derive(Debug)]
        pub struct LocaleFormatError {
            pub invalid_locale: String,
        }

        impl std::fmt::Display for LocaleFormatError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut valid_locales = [$($iso_639_1_str),+,$($iso_639_3_str),+,"cmn"];
                valid_locales.sort_by(|left, right| left.len().cmp(&right.len()).then(left.cmp(right)));
                write!(f, "Unsupported locale `{}`, expected one of {}", self.invalid_locale, valid_locales.join(", "))
            }
        }

        impl std::error::Error for LocaleFormatError {}
    };
}

make_locale!(
    (Af, "af") => (Afr, "afr"),
    (Ak, "ak") => (Aka, "aka"),
    (Am, "am") => (Amh, "amh"),
    (Ar, "ar") => (Ara, "ara"),
    (Az, "az") => (Aze, "aze"),
    (Be, "be") => (Bel, "bel"),
    (Bn, "bn") => (Ben, "ben"),
    (Bg, "bg") => (Bul, "bul"),
    (Ca, "ca") => (Cat, "cat"),
    (Cs, "cs") => (Ces, "ces"),
    (Da, "da") => (Dan, "dan"),
    (De, "de") => (Deu, "deu"),
    (El, "el") => (Ell, "ell"),
    (En, "en") => (Eng, "eng"),
    (Eo, "eo") => (Epo, "epo"),
    (Et, "et") => (Est, "est"),
    (Fi, "fi") => (Fin, "fin"),
    (Fr, "fr") => (Fra, "fra"),
    (Gu, "gu") => (Guj, "guj"),
    (He, "he") => (Heb, "heb"),
    (Hi, "hi") => (Hin, "hin"),
    (Hr, "hr") => (Hrv, "hrv"),
    (Hu, "hu") => (Hun, "hun"),
    (Hy, "hy") => (Hye, "hye"),
    (Id, "id") => (Ind, "ind"),
    (It, "it") => (Ita, "ita"),
    (Jv, "jv") => (Jav, "jav"),
    (Ja, "ja") => (Jpn, "jpn"),
    (Kn, "kn") => (Kan, "kan"),
    (Ka, "ka") => (Kat, "kat"),
    (Km, "km") => (Khm, "khm"),
    (Ko, "ko") => (Kor, "kor"),
    (La, "la") => (Lat, "lat"),
    (Lv, "lv") => (Lav, "lav"),
    (Lt, "lt") => (Lit, "lit"),
    (Ml, "ml") => (Mal, "mal"),
    (Mr, "mr") => (Mar, "mar"),
    (Mk, "mk") => (Mkd, "mkd"),
    (My, "my") => (Mya, "mya"),
    (Ne, "ne") => (Nep, "nep"),
    (Nl, "nl") => (Nld, "nld"),
    (Nb, "nb") => (Nob, "nob"),
    (Or, "or") => (Ori, "ori"),
    (Pa, "pa") => (Pan, "pan"),
    (Fa, "fa") => (Pes, "pes"),
    (Pl, "pl") => (Pol, "pol"),
    (Pt, "pt") => (Por, "por"),
    (Ro, "ro") => (Ron, "ron"),
    (Ru, "ru") => (Rus, "rus"),
    (Si, "si") => (Sin, "sin"),
    (Sk, "sk") => (Slk, "slk"),
    (Sl, "sl") => (Slv, "slv"),
    (Sn, "sn") => (Sna, "sna"),
    (Es, "es") => (Spa, "spa"),
    (Sr, "sr") => (Srp, "srp"),
    (Sv, "sv") => (Swe, "swe"),
    (Ta, "ta") => (Tam, "tam"),
    (Te, "te") => (Tel, "tel"),
    (Tl, "tl") => (Tgl, "tgl"),
    (Th, "th") => (Tha, "tha"),
    (Tk, "tk") => (Tuk, "tuk"),
    (Tr, "tr") => (Tur, "tur"),
    (Uk, "uk") => (Ukr, "ukr"),
    (Ur, "ur") => (Urd, "urd"),
    (Uz, "uz") => (Uzb, "uzb"),
    (Vi, "vi") => (Vie, "vie"),
    (Yi, "yi") => (Yid, "yid"),
    (Zh, "zh") => (Zho, "zho"),
    (Zu, "zu") => (Zul, "zul"),
);
