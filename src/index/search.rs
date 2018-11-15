use crate::rank::Document;

pub trait Search {
    fn search(&self, text: &str) -> Vec<Document>;
}
