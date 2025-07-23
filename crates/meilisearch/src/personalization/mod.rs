use crate::search::{Personalize, SearchResult};
use cohere_rust::{
    api::rerank::{ReRankModel, ReRankRequest},
    Cohere,
};
use meilisearch_types::error::ResponseError;
use tracing::{debug, error, info};

pub struct PersonalizationService {
    cohere: Option<Cohere>,
}

impl PersonalizationService {
    pub fn new(api_key: Option<String>) -> Self {
        let cohere = api_key.map(|key| Cohere::new("https://api.cohere.ai", key));

        if cohere.is_some() {
            info!("Personalization service initialized with Cohere API");
        } else {
            debug!("Personalization service initialized without Cohere API key");
        }

        Self { cohere }
    }

    pub async fn rerank_search_results(
        &self,
        search_result: SearchResult,
        personalize: Option<&Personalize>,
        query: Option<&str>,
    ) -> Result<SearchResult, ResponseError> {
        // If no API key, return original results
        let Some(cohere) = &self.cohere else { return Ok(search_result) };

        // Extract user context from personalization
        let user_context = personalize.and_then(|p| p.user_context.as_deref());

        // Build the prompt by merging query and user context
        let prompt = match (query, user_context) {
            (Some(q), Some(uc)) => format!("User Context: {}\nQuery: {}", uc, q),
            (Some(q), None) => q.to_string(),
            (None, Some(uc)) => format!("User Context: {}", uc),
            (None, None) => return Ok(search_result),
        };

        // Extract documents for reranking
        let documents: Vec<String> = search_result
            .hits
            .iter()
            .map(|hit| {
                // Convert the document to a string representation for reranking
                serde_json::to_string(&hit.document).unwrap_or_else(|_| "{}".to_string())
            })
            .collect();

        if documents.is_empty() {
            return Ok(search_result);
        }

        // Prepare the rerank request
        let rerank_request = ReRankRequest {
            query: &prompt,
            documents: &documents,
            model: ReRankModel::EnglishV3, // Use the default and more recent model
            top_n: None,
            max_chunks_per_doc: None,
        };

        // Call Cohere's rerank API
        match cohere.rerank(&rerank_request).await {
            Ok(rerank_response) => {
                debug!("Cohere rerank successful, reordering {} results", search_result.hits.len());

                // Create a mapping from original index to new rank
                let reranked_indices: Vec<usize> =
                    rerank_response.iter().map(|result| result.index as usize).collect();

                // Reorder the hits based on Cohere's reranking
                let mut reranked_hits = search_result.hits.clone();
                for (new_index, original_index) in reranked_indices.iter().enumerate() {
                    if *original_index < reranked_hits.len() {
                        reranked_hits.swap(new_index, *original_index);
                    }
                }

                Ok(SearchResult { hits: reranked_hits, ..search_result })
            }
            Err(e) => {
                error!("Cohere rerank failed with model EnglishV3: {}", e);
                // Return original results on error
                Ok(search_result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{HitsInfo, SearchHit};

    #[tokio::test]
    async fn test_personalization_service_without_api_key() {
        let service = PersonalizationService::new(None);
        let personalize = Personalize { user_context: Some("test user".to_string()) };

        let search_result = SearchResult {
            hits: vec![SearchHit {
                document: serde_json::Map::new(),
                formatted: serde_json::Map::new(),
                matches_position: None,
                ranking_score: Some(1.0),
                ranking_score_details: None,
            }],
            query: "test".to_string(),
            processing_time_ms: 10,
            hits_info: HitsInfo::OffsetLimit { limit: 1, offset: 0, estimated_total_hits: 1 },
            facet_distribution: None,
            facet_stats: None,
            semantic_hit_count: None,
            degraded: false,
            used_negative_operator: false,
        };

        let result = service
            .rerank_search_results(search_result.clone(), Some(&personalize), Some("test"))
            .await;
        assert!(result.is_ok());

        // Should return original results when no API key is provided
        let reranked_result = result.unwrap();
        assert_eq!(reranked_result.hits.len(), search_result.hits.len());
    }

    #[tokio::test]
    async fn test_personalization_service_with_user_context_only() {
        let service = PersonalizationService::new(Some("fake_key".to_string()));
        let personalize = Personalize { user_context: Some("test user".to_string()) };

        let search_result = SearchResult {
            hits: vec![SearchHit {
                document: serde_json::Map::new(),
                formatted: serde_json::Map::new(),
                matches_position: None,
                ranking_score: Some(1.0),
                ranking_score_details: None,
            }],
            query: "test".to_string(),
            processing_time_ms: 10,
            hits_info: HitsInfo::OffsetLimit { limit: 1, offset: 0, estimated_total_hits: 1 },
            facet_distribution: None,
            facet_stats: None,
            semantic_hit_count: None,
            degraded: false,
            used_negative_operator: false,
        };

        let result =
            service.rerank_search_results(search_result.clone(), Some(&personalize), None).await;
        assert!(result.is_ok());

        // Should attempt reranking with user context only
        let reranked_result = result.unwrap();
        assert_eq!(reranked_result.hits.len(), search_result.hits.len());
    }

    #[tokio::test]
    async fn test_personalization_service_with_query_only() {
        let service = PersonalizationService::new(Some("fake_key".to_string()));

        let search_result = SearchResult {
            hits: vec![SearchHit {
                document: serde_json::Map::new(),
                formatted: serde_json::Map::new(),
                matches_position: None,
                ranking_score: Some(1.0),
                ranking_score_details: None,
            }],
            query: "test".to_string(),
            processing_time_ms: 10,
            hits_info: HitsInfo::OffsetLimit { limit: 1, offset: 0, estimated_total_hits: 1 },
            facet_distribution: None,
            facet_stats: None,
            semantic_hit_count: None,
            degraded: false,
            used_negative_operator: false,
        };

        let result = service.rerank_search_results(search_result.clone(), None, Some("test")).await;
        assert!(result.is_ok());

        // Should attempt reranking with query only
        let reranked_result = result.unwrap();
        assert_eq!(reranked_result.hits.len(), search_result.hits.len());
    }

    #[tokio::test]
    async fn test_personalization_service_both_none() {
        let service = PersonalizationService::new(Some("fake_key".to_string()));

        let search_result = SearchResult {
            hits: vec![SearchHit {
                document: serde_json::Map::new(),
                formatted: serde_json::Map::new(),
                matches_position: None,
                ranking_score: Some(1.0),
                ranking_score_details: None,
            }],
            query: "test".to_string(),
            processing_time_ms: 10,
            hits_info: HitsInfo::OffsetLimit { limit: 1, offset: 0, estimated_total_hits: 1 },
            facet_distribution: None,
            facet_stats: None,
            semantic_hit_count: None,
            degraded: false,
            used_negative_operator: false,
        };

        let result = service.rerank_search_results(search_result.clone(), None, None).await;
        assert!(result.is_ok());

        // Should return original results when both query and user_context are None
        let reranked_result = result.unwrap();
        assert_eq!(reranked_result.hits.len(), search_result.hits.len());
    }
}
