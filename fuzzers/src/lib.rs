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
            Document::One => json!({ "id": 0, "doggo": "bernese" }),
            Document::Two => json!({ "id": 0, "doggo": "golden" }),
            Document::Three => json!({ "id": 0, "catto": "jorts" }),
            Document::Four => json!({ "id": 1, "doggo": "bernese" }),
            Document::Five => json!({ "id": 1, "doggo": "golden" }),
            Document::Six => json!({ "id": 1, "catto": "jorts" }),
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
            DocId::Zero => "0".to_string(),
            DocId::One => "1".to_string(),
        }
    }
}

#[derive(Debug, Arbitrary)]
pub enum Operation {
    AddDoc(Document),
    DeleteDoc(DocId),
}
