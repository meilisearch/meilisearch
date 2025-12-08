// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;

use actix_web::web::Data;
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use futures::TryStreamExt;
use index_scheduler::{IndexScheduler, Query, RoFeatures};
use itertools::{EitherOrBoth, Itertools};
use meilisearch_auth::AuthFilter;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::features::RuntimeTogglableFeatures;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::network::{Network as DbNetwork, Remote as DbRemote};
use meilisearch_types::tasks::network::{headers, NetworkTopologyChange, Origin, TaskNetwork};
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;

use super::{merge_networks, Network, PatchNetworkAnalytics, Remote};
use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::proxy::{self, proxy, Body, ProxyError};
use crate::routes::tasks::AllTasks;
use crate::routes::SummarizedTaskView;

pub async fn patch_network(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    new_network: AwebJson<Network, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    match (
        proxy::origin_from_req(&req)?,
        proxy::import_data_from_req(&req)?,
        proxy::import_metadata_from_req(&req)?,
    ) {
        (Some(origin), None, None) => {
            patch_network_with_origin(index_scheduler, new_network, req, origin, analytics).await
        }
        (None, None, None) => {
            patch_network_without_origin(index_scheduler, new_network, req, analytics).await
        }
        (Some(origin), Some(import_data), Some(metadata)) => {
            if metadata.index_count == 0 {
                tokio::task::spawn_blocking(move || {
                    index_scheduler.network_no_index_for_remote(import_data.remote_name, origin)
                })
                .await
                .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;
                Ok(HttpResponse::Ok().finish())
            } else {
                Err(MeilisearchHttpError::InvalidHeaderValue {
                    header_name: headers::PROXY_IMPORT_INDEX_COUNT_HEADER,
                    msg: format!("Expected 0 indexes, got `{}`", metadata.index_count),
                }
                .into())
            }
        }
        (origin, import_data, metadata) => {
            Err(MeilisearchHttpError::InconsistentTaskNetworkHeaders {
                is_missing_origin: origin.is_none(),
                is_missing_import: import_data.is_none(),
                is_missing_import_metadata: metadata.is_none(),
            }
            .into())
        }
    }
}

async fn patch_network_without_origin(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    new_network: AwebJson<Network, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let new_network = new_network.0;
    let old_network = index_scheduler.network();
    debug!(parameters = ?new_network, "Patch network");

    if !matches!(new_network.previous_remotes, Setting::NotSet) {
        return Err(MeilisearchHttpError::UnexpectedNetworkPreviousRemotes.into());
    }

    let merged_network = merge_networks(old_network.clone(), new_network)?;

    // When a network task must be created, perform some sanity checks against common errors:
    // - missing experimental feature on an host from the network
    // - a network task is already enqueued
    //
    // These checks are by no mean perfect (they are not atomic since the network is involved), but they should
    // help preventing a bad situation.
    if merged_network.leader.is_some() {
        let query = Query {
            statuses: Some(vec![
                meilisearch_types::tasks::Status::Enqueued,
                meilisearch_types::tasks::Status::Processing,
            ]),
            types: Some(vec![meilisearch_types::tasks::Kind::NetworkTopologyChange]),
            ..Default::default()
        };

        let filters = AuthFilter::default();
        let (tasks, _) = index_scheduler.get_task_ids_from_authorized_indexes(&query, &filters)?;

        if let Some(first) = tasks.min() {
            return Err(MeilisearchHttpError::UnprocessedNetworkTask {
                remote: None,
                task_uid: first,
            }
            .into());
        }

        futures::stream::iter(
            old_network
                .remotes
                .iter()
                .merge_join_by(merged_network.remotes.iter(), |(left, _), (right, _)| {
                    left.cmp(right)
                })
                .map(|eob| -> Result<_, ResponseError> {
                    Ok(async move {
                        let (remote_name, remote, allow_unreachable) = match eob {
                            EitherOrBoth::Both(_, (remote_name, remote))
                            | EitherOrBoth::Right((remote_name, remote)) => {
                                (remote_name, remote, false)
                            }
                            EitherOrBoth::Left((remote_name, remote)) => {
                                (remote_name, remote, true)
                            }
                        };
                        {
                            // 1. check that the experimental feature is enabled
                            let remote_features: RuntimeTogglableFeatures = match proxy::send_request(
                                "/experimental-features",
                                reqwest::Method::GET,
                                None,
                                Body::none(),
                                remote_name,
                                remote,
                            )
                            .await {
                                Ok(remote_features) => remote_features,
                                Err(ProxyError::Timeout | ProxyError::CouldNotSendRequest(_)) if allow_unreachable => {
                                    return Ok(())
                                },
                                Err(err) => return Err(err.as_response_error()),
                            };
                            let remote_features =
                                RoFeatures::from_runtime_features(remote_features);
                            remote_features
                                .check_network("receiving a proxied network task")
                                .map_err(|error| MeilisearchHttpError::RemoteIndexScheduler {
                                    remote: remote_name.to_owned(),
                                    error,
                                })?;

                            // 2. check whether there are any unfinished network task
                            let network_tasks: AllTasks = match proxy::send_request(
                        "/tasks?types=networkTopologyChange&statuses=enqueued,processing&limit=1",
                                reqwest::Method::GET,
                                None,
                                Body::none(),
                                remote_name,
                                remote).await {
                                    Ok(network_tasks) => network_tasks,
                                Err(ProxyError::Timeout | ProxyError::CouldNotSendRequest(_)) if allow_unreachable => {
                                    return Ok(())
                                },
                                Err(err) => return Err(err.as_response_error()),
                                };

                            if let [first, ..] = network_tasks.results.as_slice() {
                                return Err(ResponseError::from(
                                    MeilisearchHttpError::UnprocessedNetworkTask {
                                        remote: Some(remote_name.to_owned()),
                                        task_uid: first.uid,
                                    },
                                ));
                            }
                        }

                        Ok(())
                    })
                }),
        )
        .try_buffer_unordered(40)
        .try_collect::<()>()
        .await?;
    }

    index_scheduler.put_network(merged_network.clone())?;

    analytics.publish(
        PatchNetworkAnalytics {
            network_size: merged_network.remotes.len(),
            network_has_self: merged_network.local.is_some(),
        },
        &req,
    );

    if merged_network.leader.is_some() {
        let network_topology_change =
            NetworkTopologyChange::new(old_network.clone(), merged_network.clone());
        let task = KindWithContent::NetworkTopologyChange(network_topology_change);
        let mut task = {
            let index_scheduler = index_scheduler.clone();
            tokio::task::spawn_blocking(move || {
                index_scheduler.register_with_custom_metadata(
                    task,
                    None,
                    None,
                    false,
                    Some(TaskNetwork::Remotes {
                        remote_tasks: Default::default(),
                        network_version: merged_network.version,
                    }),
                )
            })
            .await??
        };

        let mut proxied_network = Network {
            remotes: Setting::Set(to_settings_remotes(&merged_network.remotes)),
            local: Setting::NotSet,
            leader: Setting::some_or_not_set(merged_network.leader.clone()),
            previous_remotes: Setting::Set(to_settings_remotes(&old_network.remotes)),
        };
        let mut deleted_network = old_network;

        let deleted_remotes = &mut deleted_network.remotes;
        deleted_remotes.retain(|node, _| !merged_network.remotes.contains_key(node));

        // proxy network change to the remaining remotes.
        let updated_task = proxy(
            &index_scheduler,
            None,
            &req,
            task.network.take().unwrap(), // set in register
            merged_network,
            Body::generated(proxied_network.clone(), |name, _remote, network| {
                network.local = Setting::Set(name.to_string());
            }),
            &task,
        )
        .await?;
        // unwrap: network was set by `proxy`
        let task_network = updated_task.network.unwrap();

        proxied_network.previous_remotes = Setting::NotSet;

        if deleted_network.leader.is_some() {
            // proxy network change to the deleted remotes
            proxy(
                &index_scheduler,
                None,
                &req,
                task_network,
                deleted_network,
                Body::generated(proxied_network.clone(), |_name, _remote, network| {
                    network.local = Setting::Reset;
                }),
                &task,
            )
            .await?;
        }

        let task: SummarizedTaskView = task.into();
        debug!("returns: {:?}", task);
        Ok(HttpResponse::Accepted().json(task))
    } else {
        Ok(HttpResponse::Ok().json(merged_network))
    }
}

async fn patch_network_with_origin(
    index_scheduler: GuardedData<ActionPolicy<{ actions::NETWORK_UPDATE }>, Data<IndexScheduler>>,
    merged_network: AwebJson<Network, DeserrJsonError>,
    req: HttpRequest,
    origin: Origin,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let merged_network = merged_network.into_inner();
    debug!(parameters = ?merged_network, ?origin, "Patch network");
    let mut remotes = BTreeMap::new();
    let mut old_network = index_scheduler.network();

    for (name, remote) in merged_network.remotes.set().into_iter().flat_map(|x| x.into_iter()) {
        let Some(remote) = remote else { continue };
        let remote = remote.try_into_db_node(&name)?;
        remotes.insert(name, remote);
    }
    let mut previous_remotes = BTreeMap::new();
    for (name, remote) in
        merged_network.previous_remotes.set().into_iter().flat_map(|x| x.into_iter())
    {
        let Some(remote) = remote else {
            continue;
        };
        let remote = remote.try_into_db_node(&name)?;
        previous_remotes.insert(name, remote);
    }

    old_network.remotes = previous_remotes;

    let new_network = DbNetwork {
        local: merged_network.local.set(),
        remotes,
        leader: merged_network.leader.set(),
        version: origin.network_version,
    };
    index_scheduler.put_network(new_network.clone())?;

    analytics.publish(
        PatchNetworkAnalytics {
            network_size: new_network.remotes.len(),
            network_has_self: new_network.local.is_some(),
        },
        &req,
    );

    let network_topology_change = NetworkTopologyChange::new(old_network, new_network);
    let task = KindWithContent::NetworkTopologyChange(network_topology_change);
    let task = {
        let index_scheduler = index_scheduler.clone();
        tokio::task::spawn_blocking(move || {
            index_scheduler.register_with_custom_metadata(
                task,
                None,
                None,
                false,
                Some(TaskNetwork::Origin { origin }),
            )
        })
        .await??
    };

    let task: SummarizedTaskView = task.into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

fn to_settings_remotes(
    db_remotes: &BTreeMap<String, DbRemote>,
) -> BTreeMap<String, Option<Remote>> {
    db_remotes
        .iter()
        .map(|(name, remote)| {
            (
                name.clone(),
                Some(Remote {
                    url: Setting::Set(remote.url.clone()),
                    search_api_key: Setting::some_or_not_set(remote.search_api_key.clone()),
                    write_api_key: Setting::some_or_not_set(remote.write_api_key.clone()),
                }),
            )
        })
        .collect()
}
