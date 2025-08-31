#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter<'a> {
    /// Regular filter condition
    Condition(FilterCondition<'a>),
    /// Sub-object filter that applies conditions to the same sub-object
    SubObject(SubObjectFilter<'a>),
}

impl<'a> Filter<'a> {
    /// Execute the filter and return matching document IDs
    pub fn execute(&self, index: &Index) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        match self {
            Filter::Condition(condition) => condition.execute(index),
            Filter::SubObject(sub_object) => sub_object.execute(index),
        }
    }
}

impl<'a> fmt::Display for Filter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Filter::Condition(condition) => write!(f, "{}", condition),
            Filter::SubObject(sub_object) => write!(f, "{}", sub_object),
        }
    }
}
