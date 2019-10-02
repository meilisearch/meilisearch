pub struct Synonyms {
    pub(crate) main: rkv::SingleStore,
    pub(crate) synonyms: rkv::SingleStore,
}

impl Synonyms {
    pub fn synonyms_fst<T: rkv::Readable>(
        &self,
        reader: &T,
    ) -> Result<fst::Set, rkv::StoreError>
    {
        Ok(fst::Set::default())
    }

    pub fn alternatives_to<T: rkv::Readable>(
        &self,
        reader: &T,
        word: &[u8],
    ) -> Result<Option<fst::Set>, rkv::StoreError>
    {
        unimplemented!()
    }
}
