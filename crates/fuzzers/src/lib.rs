use arbitrary::Arbitrary;
use serde_json::{json, Value};

#[derive(Debug, Arbitrary)]
pub enum Document {
    One,
    Two,
    Three,
    Four,
    Five,
    Six,
}

impl Document {
    pub fn to_d(&self) -> Value {
        match self {
            Self::One => json!({ "id": 0, "doggo": "bernese" }),
            Self::Two => json!({ "id": 0, "doggo": "golden" }),
            Self::Three => json!({ "id": 0, "catto": "jorts" }),
            Self::Four => json!({ "id": 1, "doggo": "bernese" }),
            Self::Five => json!({ "id": 1, "doggo": "golden" }),
            Self::Six => json!({ "id": 1, "catto": "jorts" }),
        }
    }
}

#[derive(Debug, Arbitrary)]
pub enum DocId {
    Zero,
    One,
}

impl DocId {
    pub fn to_s(&self) -> String {
        match self {
            Self::Zero => "0".to_string(),
            Self::One => "1".to_string(),
        }
    }
}

#[derive(Debug, Arbitrary)]
pub enum Operation {
    AddDoc(Document),
    DeleteDoc(DocId),
}
