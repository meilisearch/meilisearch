mod errors;

use once_cell::sync::Lazy;

use crate::common::{Server, Value};
use crate::json;

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "id": "287947",
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "title": "Captain Marvel",
            "id": "299537",
            "_vectors": { "manual": [1, 2, 54] },
        },
        {
            "title": "Escape Room",
            "id": "522681",
            "_vectors": { "manual": [10, -23, 32] },
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "id": "166428",
            "_vectors": { "manual": [-100, 231, 32] },
        },
        {
            "title": "Gläss",
            "id": "450465",
            "_vectors": { "manual": [-100, 340, 90] },
        }
    ])
});
