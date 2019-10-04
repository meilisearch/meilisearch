#[derive(Copy, Clone)]
pub struct Synonyms {
    pub(crate) synonyms: rkv::SingleStore,
}

impl Synonyms {
    pub fn synonyms_fst(
        &self,
        reader: &impl rkv::Readable,
    ) -> Result<fst::Set, rkv::StoreError>
    {
        Ok(fst::Set::default())
    }

    pub fn alternatives_to(
        &self,
        reader: &impl rkv::Readable,
        word: &[u8],
    ) -> Result<Option<fst::Set>, rkv::StoreError>
    {
        unimplemented!()
    }
}
