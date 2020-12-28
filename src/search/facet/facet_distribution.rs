use std::collections::{HashSet, HashMap};
use std::fmt;
use std::ops::Bound::Unbounded;

use roaring::RoaringBitmap;
use serde_json::Value;

use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetValueStringCodec, FacetLevelValueF64Codec, FacetLevelValueI64Codec};
use crate::search::facet::FacetRange;
use crate::{Index, FieldId};

pub struct FacetDistribution<'a> {
    facets: Option<HashSet<String>>,
    candidates: Option<RoaringBitmap>,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> FacetDistribution<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> FacetDistribution<'a> {
        FacetDistribution { facets: None, candidates: None, rtxn, index }
    }

    pub fn candidates(&mut self, candidates: RoaringBitmap) -> &mut Self {
        self.candidates = Some(candidates);
        self
    }

    pub fn facets<I: IntoIterator<Item=A>, A: AsRef<str>>(&mut self, names: I) -> &mut Self {
        self.facets = Some(names.into_iter().map(|s| s.as_ref().to_string()).collect());
        self
    }

    fn facet_values(&self, field_id: FieldId, field_type: FacetType) -> heed::Result<Vec<Value>> {
        let db = self.index.facet_field_id_value_docids;
        let iter = match field_type {
            FacetType::String => {
                let iter = db
                    .prefix_iter(&self.rtxn, &[field_id])?
                    .remap_key_type::<FacetValueStringCodec>()
                    .map(|r| r.map(|((_, v), docids)| (Value::from(v), docids)));
                Box::new(iter) as Box::<dyn Iterator<Item=_>>
            },
            FacetType::Integer => {
                let db = db.remap_key_type::<FacetLevelValueI64Codec>();
                let range = FacetRange::<i64, _>::new(
                    self.rtxn, db, field_id, 0, Unbounded, Unbounded,
                )?;
                Box::new(range.map(|r| r.map(|((_, _, v, _), docids)| (Value::from(v), docids))))
            },
            FacetType::Float => {
                let db = db.remap_key_type::<FacetLevelValueF64Codec>();
                let range = FacetRange::<f64, _>::new(
                    self.rtxn, db, field_id, 0, Unbounded, Unbounded,
                )?;
                Box::new(range.map(|r| r.map(|((_, _, v, _), docids)| (Value::from(v), docids))))
            },
        };

        let mut facet_values = Vec::new();
        for result in iter {
            let (value, docids) = result?;
            match &self.candidates {
                Some(candidates) => if !docids.is_disjoint(candidates) {
                    facet_values.push(value);
                },
                None => facet_values.push(value),
            }
        }
        Ok(facet_values)
    }

    pub fn execute(&self) -> heed::Result<HashMap<String, Vec<Value>>> {
        let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let faceted_fields = self.index.faceted_fields(self.rtxn)?;
        let fields_ids: Vec<_> = match &self.facets {
            Some(names) => {
                names.iter().filter_map(|n| {
                    let id = fields_ids_map.id(n)?;
                    faceted_fields.get(&id).cloned().map(|t| (id, t))
                }).collect()
            },
            None => faceted_fields.iter().map(|(id, t)| (*id, *t)).collect(),
        };

        let mut facets_values = HashMap::new();
        for (fid, ftype) in fields_ids {
            let facet_name = fields_ids_map.name(fid).unwrap();
            let values = self.facet_values(fid, ftype)?;
            facets_values.insert(facet_name.to_string(), values);
        }

        Ok(facets_values)
    }
}

impl fmt::Debug for FacetDistribution<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let FacetDistribution { facets, candidates, rtxn: _, index: _ } = self;
        f.debug_struct("FacetDistribution")
            .field("facets", facets)
            .field("candidates", candidates)
            .finish()
    }
}
