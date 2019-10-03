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

        // WARN could not retrieve the last key/data entry of a tree...
        // self.updates.get(writer, )?;

        unimplemented!()
    }

    pub fn pop_back(
        &self,
        writer: &mut rkv::Writer,
    ) -> Result<Option<(u64, Update)>, rkv::StoreError>
    {
        unimplemented!()
    }
}
