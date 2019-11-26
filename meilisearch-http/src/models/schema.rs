use std::collections::HashSet;

use indexmap::IndexMap;
use meilisearch_schema::{Schema, SchemaBuilder, SchemaProps};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FieldProperties {
    Identifier,
    Indexed,
    Displayed,
    Ranked,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaBody(IndexMap<String, HashSet<FieldProperties>>);

impl From<Schema> for SchemaBody {
    fn from(value: Schema) -> SchemaBody {
        let mut map = IndexMap::new();
        for (name, _attr, props) in value.iter() {
            let old_properties = map.entry(name.to_owned()).or_insert(HashSet::new());
            if props.is_indexed() {
                old_properties.insert(FieldProperties::Indexed);
            }
            if props.is_displayed() {
                old_properties.insert(FieldProperties::Displayed);
            }
            if props.is_ranked() {
                old_properties.insert(FieldProperties::Ranked);
            }
        }
        let old_properties = map
            .entry(value.identifier_name().to_string())
            .or_insert(HashSet::new());
        old_properties.insert(FieldProperties::Identifier);
        old_properties.insert(FieldProperties::Displayed);
        SchemaBody(map)
    }
}

impl Into<Schema> for SchemaBody {
    fn into(self) -> Schema {
        let mut identifier = "documentId".to_string();
        let mut attributes = IndexMap::new();
        for (field, properties) in self.0 {
            let mut indexed = false;
            let mut displayed = false;
            let mut ranked = false;
            for property in properties {
                match property {
                    FieldProperties::Indexed => indexed = true,
                    FieldProperties::Displayed => displayed = true,
                    FieldProperties::Ranked => ranked = true,
                    FieldProperties::Identifier => identifier = field.clone(),
                }
            }
            attributes.insert(
                field,
                SchemaProps {
                    indexed,
                    displayed,
                    ranked,
                },
            );
        }

        let mut builder = SchemaBuilder::with_identifier(identifier);
        for (field, props) in attributes {
            builder.new_attribute(field, props);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_body_conversion() {
        let schema_body = r#"
        {
            "id": ["identifier", "indexed", "displayed"],
            "title": ["indexed", "displayed"],
            "date": ["displayed"]
        }
        "#;

        let schema_builder = r#"
        {
            "identifier": "id",
            "attributes": {
                "id": {
                    "indexed": true,
                    "displayed": true
                },
                "title": {
                    "indexed": true,
                    "displayed": true
                },
                "date": {
                    "displayed": true
                }
            }
        }
        "#;

        let schema_body: SchemaBody = serde_json::from_str(schema_body).unwrap();
        let schema_builder: SchemaBuilder = serde_json::from_str(schema_builder).unwrap();

        let schema_from_body: Schema = schema_body.into();
        let schema_from_builder: Schema = schema_builder.build();

        assert_eq!(schema_from_body, schema_from_builder);
    }
}
