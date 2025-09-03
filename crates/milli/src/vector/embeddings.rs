/// One or multiple embeddings stored consecutively in a flat vector.
#[derive(Debug, PartialEq)]
pub struct Embeddings<F> {
    data: Vec<F>,
    dimension: usize,
}

impl<F> Embeddings<F> {
    /// Declares an empty  vector of embeddings of the specified dimensions.
    pub fn new(dimension: usize) -> Self {
        Self { data: Default::default(), dimension }
    }

    /// Declares a vector of embeddings containing a single element.
    ///
    /// The dimension is inferred from the length of the passed embedding.
    pub fn from_single_embedding(embedding: Vec<F>) -> Self {
        Self { dimension: embedding.len(), data: embedding }
    }

    /// Declares a vector of embeddings from its components.
    ///
    /// `data.len()` must be a multiple of `dimension`, otherwise an error is returned.
    pub fn from_inner(data: Vec<F>, dimension: usize) -> Result<Self, Vec<F>> {
        let mut this = Self::new(dimension);
        this.append(data)?;
        Ok(this)
    }

    /// Returns the number of embeddings in this vector of embeddings.
    pub fn embedding_count(&self) -> usize {
        self.data.len() / self.dimension
    }

    /// Dimension of a single embedding.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Deconstructs self into the inner flat vector.
    pub fn into_inner(self) -> Vec<F> {
        self.data
    }

    /// A reference to the inner flat vector.
    pub fn as_inner(&self) -> &[F] {
        &self.data
    }

    /// Iterates over the embeddings contained in the flat vector.
    pub fn iter(&self) -> impl Iterator<Item = &'_ [F]> + '_ {
        self.data.as_slice().chunks_exact(self.dimension)
    }

    /// Push an embedding at the end of the embeddings.
    ///
    /// If `embedding.len() != self.dimension`, then the push operation fails.
    pub fn push(&mut self, mut embedding: Vec<F>) -> Result<(), Vec<F>> {
        if embedding.len() != self.dimension {
            return Err(embedding);
        }
        self.data.append(&mut embedding);
        Ok(())
    }

    /// Append a flat vector of embeddings at the end of the embeddings.
    ///
    /// If `embeddings.len() % self.dimension != 0`, then the append operation fails.
    pub fn append(&mut self, mut embeddings: Vec<F>) -> Result<(), Vec<F>> {
        if embeddings.len() % self.dimension != 0 {
            return Err(embeddings);
        }
        self.data.append(&mut embeddings);
        Ok(())
    }
}
