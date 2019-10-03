use crate::update::UpdateResult;

#[derive(Copy, Clone)]
pub struct UpdatesResults {
    pub(crate) updates_results: rkv::SingleStore,
}

impl UpdatesResults {
    pub fn put_update_result(
        &self,
        writer: &mut rkv::Writer,
        update_id: u64,
        update_result: &UpdateResult,
    ) -> Result<(), rkv::StoreError>
    {
        // let update = rmp_serde::to_vec_named(&addition)?;

        // WARN could not retrieve the last key/data entry of a tree...
        // self.updates.get(writer, )?;

        unimplemented!()
    }

    pub fn update_result<T: rkv::Readable>(
        reader: &T,
        update_id: u64,
    ) -> Result<Option<UpdateResult>, rkv::StoreError>
    {
        unimplemented!()
    }
}
