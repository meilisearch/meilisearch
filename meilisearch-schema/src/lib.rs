mod error;
mod fields_map;
mod schema;

pub use error::{Error, SResult};
pub use fields_map::{FieldsMap, FieldId};
pub use schema::{Schema, IndexedPos};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Default, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct SchemaAttr(pub u16);

impl SchemaAttr {
    pub const fn new(value: u16) -> SchemaAttr {
        SchemaAttr(value)
    }

    pub const fn min() -> SchemaAttr {
        SchemaAttr(u16::min_value())
    }

    pub const fn max() -> SchemaAttr {
        SchemaAttr(u16::max_value())
    }

    pub fn next(self) -> SResult<SchemaAttr> {
        self.0.checked_add(1).map(SchemaAttr).ok_or(Error::MaxFieldsLimitExceeded)
    }

    pub fn prev(self) -> SResult<SchemaAttr> {
        self.0.checked_sub(1).map(SchemaAttr).ok_or(Error::MaxFieldsLimitExceeded)
    }
}

impl From<u16> for SchemaAttr {
    fn from(value: u16) -> SchemaAttr {
        SchemaAttr(value)
    }
}

impl Into<u16> for SchemaAttr {
    fn into(self) -> u16 {
        self.0
    }
}



// use std::collections::{BTreeMap, HashMap};
// use std::ops::BitOr;
// use std::sync::Arc;
// use std::{fmt, u16};

// use indexmap::IndexMap;
// use serde::{Deserialize, Serialize};

// pub const DISPLAYED: SchemaProps = SchemaProps {
//     displayed: true,
//     indexed: false,
//     ranked: false,
// };
// pub const INDEXED: SchemaProps = SchemaProps {
//     displayed: false,
//     indexed: true,
//     ranked: false,
// };
// pub const RANKED: SchemaProps = SchemaProps {
//     displayed: false,
//     indexed: false,
//     ranked: true,
// };

// #[derive(Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub struct SchemaProps {
//     #[serde(default)]
//     pub displayed: bool,

//     #[serde(default)]
//     pub indexed: bool,

//     #[serde(default)]
//     pub ranked: bool,
// }

// impl SchemaProps {
//     pub fn is_displayed(self) -> bool {
//         self.displayed
//     }

//     pub fn is_indexed(self) -> bool {
//         self.indexed
//     }

//     pub fn is_ranked(self) -> bool {
//         self.ranked
//     }
// }

// impl BitOr for SchemaProps {
//     type Output = Self;

//     fn bitor(self, other: Self) -> Self::Output {
//         SchemaProps {
//             displayed: self.displayed | other.displayed,
//             indexed: self.indexed | other.indexed,
//             ranked: self.ranked | other.ranked,
//         }
//     }
// }

// impl fmt::Debug for SchemaProps {
//     #[allow(non_camel_case_types)]
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         #[derive(Debug)]
//         struct DISPLAYED;

//         #[derive(Debug)]
//         struct INDEXED;

//         #[derive(Debug)]
//         struct RANKED;

//         let mut debug_set = f.debug_set();

//         if self.displayed {
//             debug_set.entry(&DISPLAYED);
//         }

//         if self.indexed {
//             debug_set.entry(&INDEXED);
//         }

//         if self.ranked {
//             debug_set.entry(&RANKED);
//         }

//         debug_set.finish()
//     }
// }

// #[derive(Serialize, Deserialize)]
// pub struct SchemaBuilder {
//     identifier: String,
//     attributes: IndexMap<String, SchemaProps>,
// }

// impl SchemaBuilder {

//     pub fn with_identifier<S: Into<String>>(name: S) -> SchemaBuilder {
//         SchemaBuilder {
//             identifier: name.into(),
//             attributes: IndexMap::new(),
//         }
//     }

//     pub fn new_attribute<S: Into<String>>(&mut self, name: S, props: SchemaProps) -> SchemaAttr {
//         let len = self.attributes.len();
//         if self.attributes.insert(name.into(), props).is_some() {
//             panic!("Field already inserted.")
//         }
//         SchemaAttr(len as u16)
//     }

//     pub fn build(self) -> Schema {
//         let mut attrs = HashMap::new();
//         let mut props = Vec::new();

//         for (i, (name, prop)) in self.attributes.into_iter().enumerate() {
//             attrs.insert(name.clone(), SchemaAttr(i as u16));
//             props.push((name, prop));
//         }

//         let identifier = self.identifier;
//         Schema {
//             inner: Arc::new(InnerSchema {
//                 identifier,
//                 attrs,
//                 props,
//             }),
//         }
//     }
// }

// #[derive(Clone, PartialEq, Eq)]
// pub struct Schema {
//     inner: Arc<InnerSchema>,
// }

// #[derive(Clone, PartialEq, Eq)]
// struct InnerSchema {
//     identifier: (String, u16),
//     attrs: HashMap<String, SchemaAttr>,
//     props: Vec<(String, SchemaProps)>,
// }

// impl Schema {
//     pub fn to_builder(&self) -> SchemaBuilder {
//         let identifier = self.inner.identifier.clone();
//         let attributes = self.attributes_ordered();
//         SchemaBuilder {
//             identifier,
//             attributes,
//         }
//     }

//     fn attributes_ordered(&self) -> IndexMap<String, SchemaProps> {
//         let mut ordered = BTreeMap::new();
//         for (name, attr) in &self.inner.attrs {
//             let (_, props) = self.inner.props[attr.0 as usize];
//             ordered.insert(attr.0, (name, props));
//         }

//         let mut attributes = IndexMap::with_capacity(ordered.len());
//         for (_, (name, props)) in ordered {
//             attributes.insert(name.clone(), props);
//         }

//         attributes
//     }

//     pub fn number_of_attributes(&self) -> usize {
//         self.inner.attrs.len()
//     }

//     pub fn props(&self, attr: SchemaAttr) -> SchemaProps {
//         let (_, props) = self.inner.props[attr.0 as usize];
//         props
//     }

//     pub fn identifier_name(&self) -> &str {
//         &self.inner.identifier
//     }

//     pub fn attribute<S: AsRef<str>>(&self, name: S) -> Option<SchemaAttr> {
//         self.inner.attrs.get(name.as_ref()).cloned()
//     }

//     pub fn attribute_name(&self, attr: SchemaAttr) -> &str {
//         let (name, _) = &self.inner.props[attr.0 as usize];
//         name
//     }

//     pub fn into_iter<'a>(&'a self) -> impl Iterator<Item = (String, SchemaProps)> + 'a {
//         self.inner.props.clone().into_iter()
//     }

//     pub fn iter<'a>(&'a self) -> impl Iterator<Item = (&str, SchemaAttr, SchemaProps)> + 'a {
//         self.inner.props.iter().map(move |(name, prop)| {
//             let attr = self.inner.attrs.get(name).unwrap();
//             (name.as_str(), *attr, *prop)
//         })
//     }
// }

// impl Serialize for Schema {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::ser::Serializer,
//     {
//         self.to_builder().serialize(serializer)
//     }
// }

// impl<'de> Deserialize<'de> for Schema {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::de::Deserializer<'de>,
//     {
//         let builder = SchemaBuilder::deserialize(deserializer)?;
//         Ok(builder.build())
//     }
// }

// impl fmt::Debug for Schema {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         let builder = self.to_builder();
//         f.debug_struct("Schema")
//             .field("identifier", &builder.identifier)
//             .field("attributes", &builder.attributes)
//             .finish()
//     }
// }

// #[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
// pub struct SchemaAttr(pub u16);

// impl SchemaAttr {
//     pub const fn new(value: u16) -> SchemaAttr {
//         SchemaAttr(value)
//     }

//     pub const fn min() -> SchemaAttr {
//         SchemaAttr(u16::min_value())
//     }

//     pub const fn max() -> SchemaAttr {
//         SchemaAttr(u16::max_value())
//     }

//     pub fn next(self) -> Option<SchemaAttr> {
//         self.0.checked_add(1).map(SchemaAttr)
//     }

//     pub fn prev(self) -> Option<SchemaAttr> {
//         self.0.checked_sub(1).map(SchemaAttr)
//     }
// }

// impl fmt::Display for SchemaAttr {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         self.0.fmt(f)
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub enum Diff {
//     IdentChange {
//         old: String,
//         new: String,
//     },
//     AttrMove {
//         name: String,
//         old: usize,
//         new: usize,
//     },
//     AttrPropsChange {
//         name: String,
//         old: SchemaProps,
//         new: SchemaProps,
//     },
//     NewAttr {
//         name: String,
//         pos: usize,
//         props: SchemaProps,
//     },
//     RemovedAttr {
//         name: String,
//     },
// }

// pub fn diff(old: &Schema, new: &Schema) -> Vec<Diff> {
//     use Diff::{AttrMove, AttrPropsChange, IdentChange, NewAttr, RemovedAttr};

//     let mut differences = Vec::new();
//     let old = old.to_builder();
//     let new = new.to_builder();

//     // check if the old identifier differs from the new one
//     if old.identifier != new.identifier {
//         let old = old.identifier;
//         let new = new.identifier;
//         differences.push(IdentChange { old, new });
//     }

//     // compare all old attributes positions
//     // and properties with the new ones
//     for (pos, (name, props)) in old.attributes.iter().enumerate() {
//         match new.attributes.get_full(name) {
//             Some((npos, _, nprops)) => {
//                 if pos != npos {
//                     let name = name.clone();
//                     differences.push(AttrMove {
//                         name,
//                         old: pos,
//                         new: npos,
//                     });
//                 }
//                 if props != nprops {
//                     let name = name.clone();
//                     differences.push(AttrPropsChange {
//                         name,
//                         old: *props,
//                         new: *nprops,
//                     });
//                 }
//             }
//             None => differences.push(RemovedAttr { name: name.clone() }),
//         }
//     }

//     // retrieve all attributes that
//     // were not present in the old schema
//     for (pos, (name, props)) in new.attributes.iter().enumerate() {
//         if !old.attributes.contains_key(name) {
//             let name = name.clone();
//             differences.push(NewAttr {
//                 name,
//                 pos,
//                 props: *props,
//             });
//         }
//     }

//     differences
// }


// // The diff_transposition return the transpotion matrix to apply during the documents rewrite process.
// // e.g.
// // old_schema: ["id", "title", "description", "tags", "date"]
// // new_schema: ["title", "tags", "id", "new", "position","description"]
// // diff_transposition: [Some(2), Some(0), Some(5), Some(1), None]
// //
// // - attribute 0 (id) become attribute 2
// // - attribute 1 (title) become attribute 0
// // - attribute 2 (description) become attribute 5
// // - attribute 3 (tags) become attribute 1
// // - attribute 4 (date) is deleted
// pub fn diff_transposition(old: &Schema, new: &Schema) -> Vec<Option<u16>> {
//     let old = old.to_builder();
//     let new = new.to_builder();

//     let old_attributes: Vec<&str> = old.attributes.iter().map(|(key, _)| key.as_str()).collect();
//     let new_attributes: Vec<&str> = new.attributes.iter().map(|(key, _)| key.as_str()).collect();

//     let mut transpotition = Vec::new();

//     for (_pos, attr) in old_attributes.iter().enumerate() {
//         if let Some(npos) = new_attributes[..].iter().position(|x| x == attr) {
//             transpotition.push(Some(npos as u16));
//         } else {
//             transpotition.push(None);
//         }
//     }

//     transpotition
// }

// pub fn generate_schema(identifier: String, indexed: Vec<String>, displayed: Vec<String>, ranked: Vec<String>) -> Schema {
//     let mut map = IndexMap::new();

//     for item in indexed.iter() {
//         map.entry(item).or_insert(SchemaProps::default()).indexed = true;
//     }
//     for item in ranked.iter() {
//         map.entry(item).or_insert(SchemaProps::default()).ranked = true;
//     }
//     for item in displayed.iter() {
//         map.entry(item).or_insert(SchemaProps::default()).displayed = true;
//     }
//     let id = identifier.clone();
//     map.entry(&id).or_insert(SchemaProps::default());

//     let mut builder = SchemaBuilder::with_identifier(identifier);

//     for (key, value) in map {
//         builder.new_attribute(key, value);
//     }

//     builder.build()
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::error::Error;

//     #[test]
//     fn difference() {
//         use Diff::{AttrMove, AttrPropsChange, IdentChange, NewAttr, RemovedAttr};

//         let mut builder = SchemaBuilder::with_identifier("id");
//         builder.new_attribute("alpha", DISPLAYED);
//         builder.new_attribute("beta", DISPLAYED | INDEXED);
//         builder.new_attribute("gamma", INDEXED);
//         builder.new_attribute("omega", INDEXED);
//         let old = builder.build();

//         let mut builder = SchemaBuilder::with_identifier("kiki");
//         builder.new_attribute("beta", DISPLAYED | INDEXED);
//         builder.new_attribute("alpha", DISPLAYED | INDEXED);
//         builder.new_attribute("delta", RANKED);
//         builder.new_attribute("gamma", DISPLAYED);
//         let new = builder.build();

//         let differences = diff(&old, &new);
//         let expected = &[
//             IdentChange {
//                 old: format!("id"),
//                 new: format!("kiki"),
//             },
//             AttrMove {
//                 name: format!("alpha"),
//                 old: 0,
//                 new: 1,
//             },
//             AttrPropsChange {
//                 name: format!("alpha"),
//                 old: DISPLAYED,
//                 new: DISPLAYED | INDEXED,
//             },
//             AttrMove {
//                 name: format!("beta"),
//                 old: 1,
//                 new: 0,
//             },
//             AttrMove {
//                 name: format!("gamma"),
//                 old: 2,
//                 new: 3,
//             },
//             AttrPropsChange {
//                 name: format!("gamma"),
//                 old: INDEXED,
//                 new: DISPLAYED,
//             },
//             RemovedAttr {
//                 name: format!("omega"),
//             },
//             NewAttr {
//                 name: format!("delta"),
//                 pos: 2,
//                 props: RANKED,
//             },
//         ];

//         assert_eq!(&differences, expected)
//     }

//     #[test]
//     fn serialize_deserialize() -> bincode::Result<()> {
//         let mut builder = SchemaBuilder::with_identifier("id");
//         builder.new_attribute("alpha", DISPLAYED);
//         builder.new_attribute("beta", DISPLAYED | INDEXED);
//         builder.new_attribute("gamma", INDEXED);
//         let schema = builder.build();

//         let mut buffer = Vec::new();
//         bincode::serialize_into(&mut buffer, &schema)?;
//         let schema2 = bincode::deserialize_from(buffer.as_slice())?;

//         assert_eq!(schema, schema2);

//         Ok(())
//     }

//     #[test]
//     fn serialize_deserialize_toml() -> Result<(), Box<dyn Error>> {
//         let mut builder = SchemaBuilder::with_identifier("id");
//         builder.new_attribute("alpha", DISPLAYED);
//         builder.new_attribute("beta", DISPLAYED | INDEXED);
//         builder.new_attribute("gamma", INDEXED);
//         let schema = builder.build();

//         let buffer = toml::to_vec(&schema)?;
//         let schema2 = toml::from_slice(buffer.as_slice())?;

//         assert_eq!(schema, schema2);

//         let data = r#"
//             identifier = "id"

//             [attributes."alpha"]
//             displayed = true

//             [attributes."beta"]
//             displayed = true
//             indexed = true

//             [attributes."gamma"]
//             indexed = true
//         "#;
//         let schema2 = toml::from_str(data)?;
//         assert_eq!(schema, schema2);

//         Ok(())
//     }

//     #[test]
//     fn serialize_deserialize_json() -> Result<(), Box<dyn Error>> {
//         let mut builder = SchemaBuilder::with_identifier("id");
//         builder.new_attribute("alpha", DISPLAYED);
//         builder.new_attribute("beta", DISPLAYED | INDEXED);
//         builder.new_attribute("gamma", INDEXED);
//         let schema = builder.build();

//         let buffer = serde_json::to_vec(&schema)?;
//         let schema2 = serde_json::from_slice(buffer.as_slice())?;

//         assert_eq!(schema, schema2);

//         let data = r#"
//             {
//                 "identifier": "id",
//                 "attributes": {
//                     "alpha": {
//                         "displayed": true
//                     },
//                     "beta": {
//                         "displayed": true,
//                         "indexed": true
//                     },
//                     "gamma": {
//                         "indexed": true
//                     }
//                 }
//             }"#;
//         let schema2 = serde_json::from_str(data)?;
//         assert_eq!(schema, schema2);

//         Ok(())
//     }

//     #[test]
//     fn debug_output() {
//         use std::fmt::Write as _;

//         let mut builder = SchemaBuilder::with_identifier("id");
//         builder.new_attribute("alpha", DISPLAYED);
//         builder.new_attribute("beta", DISPLAYED | INDEXED);
//         builder.new_attribute("gamma", INDEXED);
//         let schema = builder.build();

//         let mut output = String::new();
//         let _ = write!(&mut output, "{:#?}", schema);

//         let expected = r#"Schema {
//     identifier: "id",
//     attributes: {
//         "alpha": {
//             DISPLAYED,
//         },
//         "beta": {
//             DISPLAYED,
//             INDEXED,
//         },
//         "gamma": {
//             INDEXED,
//         },
//     },
// }"#;

//         assert_eq!(output, expected);

//         let mut output = String::new();
//         let _ = write!(&mut output, "{:?}", schema);

//         let expected = r#"Schema { identifier: "id", attributes: {"alpha": {DISPLAYED}, "beta": {DISPLAYED, INDEXED}, "gamma": {INDEXED}} }"#;

//         assert_eq!(output, expected);
//     }
// }
