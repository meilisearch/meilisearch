#[cfg(test)]
mod tests {
    use deserr::Deserr;
    use meili_snap::snapshot;
    use meilisearch_types::deserr::DeserrQueryParamError;
    use meilisearch_types::error::{Code, ResponseError};

    use crate::routes::tasks::{TaskDeletionOrCancelationQuery, TasksFilterQuery};

    fn deserr_query_params<T>(j: &str) -> Result<T, ResponseError>
    where
        T: Deserr<DeserrQueryParamError>,
    {
        let value = serde_urlencoded::from_str::<serde_json::Value>(j)
            .map_err(|e| ResponseError::from_msg(e.to_string(), Code::BadRequest))?;

        match deserr::deserialize::<_, _, DeserrQueryParamError>(value) {
            Ok(data) => Ok(data),
            Err(e) => Err(ResponseError::from(e)),
        }
    }

    #[test]
    fn deserialize_task_filter_dates() {
        {
            let params = "afterEnqueuedAt=2021-12-03&beforeEnqueuedAt=2021-12-03&afterStartedAt=2021-12-03&beforeStartedAt=2021-12-03&afterFinishedAt=2021-12-03&beforeFinishedAt=2021-12-03";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();

            snapshot!(format!("{:?}", query.after_enqueued_at), @"Other(2021-12-04 0:00:00.0 +00:00:00)");
            snapshot!(format!("{:?}", query.before_enqueued_at), @"Other(2021-12-03 0:00:00.0 +00:00:00)");
            snapshot!(format!("{:?}", query.after_started_at), @"Other(2021-12-04 0:00:00.0 +00:00:00)");
            snapshot!(format!("{:?}", query.before_started_at), @"Other(2021-12-03 0:00:00.0 +00:00:00)");
            snapshot!(format!("{:?}", query.after_finished_at), @"Other(2021-12-04 0:00:00.0 +00:00:00)");
            snapshot!(format!("{:?}", query.before_finished_at), @"Other(2021-12-03 0:00:00.0 +00:00:00)");
        }
        {
            let params =
                "afterEnqueuedAt=2021-12-03T23:45:23Z&beforeEnqueuedAt=2021-12-03T23:45:23Z";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at), @"Other(2021-12-03 23:45:23.0 +00:00:00)");
            snapshot!(format!("{:?}", query.before_enqueued_at), @"Other(2021-12-03 23:45:23.0 +00:00:00)");
        }
        {
            let params = "afterEnqueuedAt=1997-11-12T09:55:06-06:20";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at), @"Other(1997-11-12 9:55:06.0 -06:20:00)");
        }
        {
            let params = "afterEnqueuedAt=1997-11-12T09:55:06%2B00:00";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at), @"Other(1997-11-12 9:55:06.0 +00:00:00)");
        }
        {
            let params = "afterEnqueuedAt=1997-11-12T09:55:06.200000300Z";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at), @"Other(1997-11-12 9:55:06.2000003 +00:00:00)");
        }
        {
            // Stars are allowed in date fields as well
            let params = "afterEnqueuedAt=*&beforeStartedAt=*&afterFinishedAt=*&beforeFinishedAt=*&afterStartedAt=*&beforeEnqueuedAt=*";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query), @"TaskDeletionOrCancelationQuery { uids: None, batch_uids: None, canceled_by: None, types: None, statuses: None, index_uids: None, after_enqueued_at: Star, before_enqueued_at: Star, after_started_at: Star, before_started_at: Star, after_finished_at: Star, before_finished_at: Star }");
        }
        {
            let params = "afterFinishedAt=2021";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `afterFinishedAt`: `2021` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
              "code": "invalid_task_after_finished_at",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_after_finished_at"
            }
            "###);
        }
        {
            let params = "beforeFinishedAt=2021";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `beforeFinishedAt`: `2021` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
              "code": "invalid_task_before_finished_at",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_before_finished_at"
            }
            "###);
        }
        {
            let params = "afterEnqueuedAt=2021-12";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `afterEnqueuedAt`: `2021-12` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
              "code": "invalid_task_after_enqueued_at",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_after_enqueued_at"
            }
            "###);
        }

        {
            let params = "beforeEnqueuedAt=2021-12-03T23";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `beforeEnqueuedAt`: `2021-12-03T23` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
              "code": "invalid_task_before_enqueued_at",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_before_enqueued_at"
            }
            "###);
        }
        {
            let params = "afterStartedAt=2021-12-03T23:45";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `afterStartedAt`: `2021-12-03T23:45` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
              "code": "invalid_task_after_started_at",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_after_started_at"
            }
            "###);
        }
        {
            let params = "beforeStartedAt=2021-12-03T23:45";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `beforeStartedAt`: `2021-12-03T23:45` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
              "code": "invalid_task_before_started_at",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_before_started_at"
            }
            "###);
        }
    }

    #[test]
    fn deserialize_task_filter_uids() {
        {
            let params = "uids=78,1,12,73";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.uids), @"List([78, 1, 12, 73])");
        }
        {
            let params = "uids=1";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.uids), @"List([1])");
        }
        {
            let params = "uids=cat,*,dog";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `uids[0]`: could not parse `cat` as a positive integer",
              "code": "invalid_task_uids",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_uids"
            }
            "###);
        }
        {
            let params = "uids=78,hello,world";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `uids[1]`: could not parse `hello` as a positive integer",
              "code": "invalid_task_uids",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_uids"
            }
            "###);
        }
        {
            let params = "uids=cat";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `uids`: could not parse `cat` as a positive integer",
              "code": "invalid_task_uids",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_uids"
            }
            "###);
        }
    }

    #[test]
    fn deserialize_task_filter_status() {
        {
            let params = "statuses=succeeded,failed,enqueued,processing,canceled";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.statuses), @"List([Succeeded, Failed, Enqueued, Processing, Canceled])");
        }
        {
            let params = "statuses=enqueued";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.statuses), @"List([Enqueued])");
        }
        {
            let params = "statuses=finished";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `statuses`: `finished` is not a valid task status. Available statuses are `enqueued`, `processing`, `succeeded`, `failed`, `canceled`.",
              "code": "invalid_task_statuses",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_statuses"
            }
            "###);
        }
    }
    #[test]
    fn deserialize_task_filter_types() {
        {
            let params = "types=documentAdditionOrUpdate,documentDeletion,settingsUpdate,indexCreation,indexDeletion,indexUpdate,indexSwap,taskCancelation,taskDeletion,dumpCreation,snapshotCreation";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.types), @"List([DocumentAdditionOrUpdate, DocumentDeletion, SettingsUpdate, IndexCreation, IndexDeletion, IndexUpdate, IndexSwap, TaskCancelation, TaskDeletion, DumpCreation, SnapshotCreation])");
        }
        {
            let params = "types=settingsUpdate";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.types), @"List([SettingsUpdate])");
        }
        {
            let params = "types=createIndex";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `types`: `createIndex` is not a valid task type. Available types are `documentAdditionOrUpdate`, `documentEdition`, `documentDeletion`, `settingsUpdate`, `indexCreation`, `indexDeletion`, `indexUpdate`, `indexSwap`, `taskCancelation`, `taskDeletion`, `dumpCreation`, `snapshotCreation`, `export`, `upgradeDatabase`, `indexCompaction`.",
              "code": "invalid_task_types",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_types"
            }
            "###);
        }
    }
    #[test]
    fn deserialize_task_filter_index_uids() {
        {
            let params = "indexUids=toto,tata-78";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.index_uids), @r###"List([IndexUid("toto"), IndexUid("tata-78")])"###);
        }
        {
            let params = "indexUids=index_a";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.index_uids), @r###"List([IndexUid("index_a")])"###);
        }
        {
            let params = "indexUids=1,hé";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `indexUids[1]`: `hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
              "code": "invalid_index_uid",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
            }
            "###);
        }
        {
            let params = "indexUids=hé";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `indexUids`: `hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
              "code": "invalid_index_uid",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
            }
            "###);
        }
    }

    #[test]
    fn deserialize_task_filter_general() {
        {
            let params = "from=12&limit=15&indexUids=toto,tata-78&statuses=succeeded,enqueued&afterEnqueuedAt=2012-04-23&uids=1,2,3";
            let query = deserr_query_params::<TasksFilterQuery>(params).unwrap();
            snapshot!(format!("{:?}", query), @r###"TasksFilterQuery { limit: Param(15), from: Some(Param(12)), reverse: None, batch_uids: None, uids: List([1, 2, 3]), canceled_by: None, types: None, statuses: List([Succeeded, Enqueued]), index_uids: List([IndexUid("toto"), IndexUid("tata-78")]), after_enqueued_at: Other(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None }"###);
        }
        {
            // Stars should translate to `None` in the query
            // Verify value of the default limit
            let params = "indexUids=*&statuses=succeeded,*&afterEnqueuedAt=2012-04-23&uids=1,2,3";
            let query = deserr_query_params::<TasksFilterQuery>(params).unwrap();
            snapshot!(format!("{:?}", query), @"TasksFilterQuery { limit: Param(20), from: None, reverse: None, batch_uids: None, uids: List([1, 2, 3]), canceled_by: None, types: None, statuses: Star, index_uids: Star, after_enqueued_at: Other(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None }");
        }
        {
            // Stars should also translate to `None` in task deletion/cancelation queries
            let params = "indexUids=*&statuses=succeeded,*&afterEnqueuedAt=2012-04-23&uids=1,2,3";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query), @"TaskDeletionOrCancelationQuery { uids: List([1, 2, 3]), batch_uids: None, canceled_by: None, types: None, statuses: Star, index_uids: Star, after_enqueued_at: Other(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None }");
        }
        {
            // Star in from not allowed
            let params = "uids=*&from=*";
            let err = deserr_query_params::<TasksFilterQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Invalid value in parameter `from`: could not parse `*` as a positive integer",
              "code": "invalid_task_from",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_task_from"
            }
            "###);
        }
        {
            // From not allowed in task deletion/cancelation queries
            let params = "from=12";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Unknown parameter `from`: expected one of `uids`, `batchUids`, `canceledBy`, `types`, `statuses`, `indexUids`, `afterEnqueuedAt`, `beforeEnqueuedAt`, `afterStartedAt`, `beforeStartedAt`, `afterFinishedAt`, `beforeFinishedAt`",
              "code": "bad_request",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#bad_request"
            }
            "###);
        }
        {
            // Limit not allowed in task deletion/cancelation queries
            let params = "limit=12";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(meili_snap::json_string!(err), @r###"
            {
              "message": "Unknown parameter `limit`: expected one of `uids`, `batchUids`, `canceledBy`, `types`, `statuses`, `indexUids`, `afterEnqueuedAt`, `beforeEnqueuedAt`, `afterStartedAt`, `beforeStartedAt`, `afterFinishedAt`, `beforeFinishedAt`",
              "code": "bad_request",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#bad_request"
            }
            "###);
        }
    }

    #[test]
    fn deserialize_task_delete_or_cancel_empty() {
        {
            let params = "";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            assert!(query.is_empty());
        }
        {
            let params = "statuses=*";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            assert!(!query.is_empty());
            snapshot!(format!("{query:?}"), @"TaskDeletionOrCancelationQuery { uids: None, batch_uids: None, canceled_by: None, types: None, statuses: Star, index_uids: None, after_enqueued_at: None, before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None }");
        }
    }
}
