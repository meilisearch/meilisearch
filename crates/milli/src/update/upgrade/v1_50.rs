use charabia::TokenizerBuilder;
use heed::types::Str;
use heed::RwTxn;

use super::{UpgradeIndex, UpgradeParams};
use crate::{index::Synonyms, update::settings::normalize, Index, Result};
