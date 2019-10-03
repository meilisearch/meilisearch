use crate::update::Update;

#[derive(Copy, Clone)]
pub struct Updates {
    pub(crate) updates: rkv::SingleStore,
}

impl Updates {
    pub fn push_back(
        &self,
        writer: &mut rkv::Writer,
        update: &Update,
    ) -> Result<u64, rkv::StoreError>
    {
        // let update = rmp_serde::to_vec_named(&addition)?;
        unimplemented!()
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
