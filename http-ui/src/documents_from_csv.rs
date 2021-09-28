use std::io::{Read, Result as IoResult};
use std::num::ParseFloatError;

use serde_json::{Map, Value};

enum AllowedType {
    String,
    Number,
}

fn parse_csv_header(header: &str) -> (String, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name.to_string(), AllowedType::String),
            "number" => (field_name.to_string(), AllowedType::Number),
            // we may return an error in this case.
            _otherwise => (header.to_string(), AllowedType::String),
        },
        None => (header.to_string(), AllowedType::String),
    }
}

pub struct CSVDocumentDeserializer<R>
where
    R: Read,
{
    documents: csv::StringRecordsIntoIter<R>,
    headers: Vec<(String, AllowedType)>,
}

impl<R: Read> CSVDocumentDeserializer<R> {
    pub fn from_reader(reader: R) -> IoResult<Self> {
        let mut records = csv::Reader::from_reader(reader);

        let headers = records.headers()?.into_iter().map(parse_csv_header).collect();

        Ok(Self { documents: records.into_records(), headers })
    }
}

impl<R: Read> Iterator for CSVDocumentDeserializer<R> {
    type Item = anyhow::Result<Map<String, Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let csv_document = self.documents.next()?;

        match csv_document {
            Ok(csv_document) => {
                let mut document = Map::new();

                for ((field_name, field_type), value) in
                    self.headers.iter().zip(csv_document.into_iter())
                {
                    let parsed_value: Result<Value, ParseFloatError> = match field_type {
                        AllowedType::Number => {
                            value.parse::<f64>().map(Value::from).map_err(Into::into)
                        }
                        AllowedType::String => Ok(Value::String(value.to_string())),
                    };

                    match parsed_value {
                        Ok(value) => drop(document.insert(field_name.to_string(), value)),
                        Err(_e) => {
                            return Some(Err(anyhow::anyhow!(
                                "Value '{}' is not a valid number",
                                value
                            )))
                        }
                    }
                }

                Some(Ok(document))
            }
            Err(e) => Some(Err(anyhow::anyhow!("Error parsing csv document: {}", e))),
        }
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;

    #[test]
    fn simple_csv_document() {
        let documents = r#"city,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn coma_in_field() {
        let documents = r#"city,country,pop
"Boston","United, States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United, States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn quote_in_field() {
        let documents = r#"city,country,pop
"Boston","United"" States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United\" States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn integer_in_field() {
        let documents = r#"city,country,pop:number
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910.0,
            })
        );
    }

    #[test]
    fn float_in_field() {
        let documents = r#"city,country,pop:number
"Boston","United States","4628910.01""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city": "Boston",
                "country": "United States",
                "pop": 4628910.01,
            })
        );
    }

    #[test]
    fn several_double_dot_in_header() {
        let documents = r#"city:love:string,country:state,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city:love": "Boston",
                "country:state": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn ending_by_double_dot_in_header() {
        let documents = r#"city:,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn starting_by_double_dot_in_header() {
        let documents = r#":city,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                ":city": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn starting_by_double_dot_in_header2() {
        let documents = r#":string,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn double_double_dot_in_header() {
        let documents = r#"city::string,country,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert_eq!(
            Value::Object(csv_iter.next().unwrap().unwrap()),
            json!({
                "city:": "Boston",
                "country": "United States",
                "pop": "4628910",
            })
        );
    }

    #[test]
    fn bad_type_in_header() {
        let documents = r#"city,country:number,pop
"Boston","United States","4628910""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert!(csv_iter.next().unwrap().is_err());
    }

    #[test]
    fn bad_column_count1() {
        let documents = r#"city,country,pop
"Boston","United States","4628910", "too much""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert!(csv_iter.next().unwrap().is_err());
    }

    #[test]
    fn bad_column_count2() {
        let documents = r#"city,country,pop
"Boston","United States""#;

        let mut csv_iter = CSVDocumentDeserializer::from_reader(documents.as_bytes()).unwrap();

        assert!(csv_iter.next().unwrap().is_err());
    }
}
