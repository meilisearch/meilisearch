use std::{
    collections::{BTreeMap, HashMap},
    io::Read as _,
    path::Path,
    sync::Arc,
};

use anyhow::{bail, Context as _};
use milli::vector::json_template::JsonTemplate;
use serde::{Deserialize, Serialize};

use crate::{
    common::{
        assets::{self, fetch_asset, Asset, AssetFormat},
        client::Client,
        command::{run_commands, Command},
        instance::Binary,
        process::{self, delete_db, start_meili},
    },
    relevancy::RelevancyArgs,
};

/// A relevancy workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RelevancyWorkload {
    pub name: String,
    pub extra_cli_args: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub assets: BTreeMap<String, Asset>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub precommands: Vec<Command>,
    pub query_template: QueryTemplate,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryTemplate {
    query_set_asset: String,
    template: serde_json::Value,
}

pub async fn execute(
    assets_client: &Client,
    meili_client: &Arc<Client>,
    master_key: Option<&str>,
    workload: RelevancyWorkload,
    args: &RelevancyArgs,
    binary_path: Option<&Path>,
) -> anyhow::Result<()> {
    assets::fetch_assets(assets_client, &workload.assets, &args.common.asset_folder).await?;
    tracing::info!(workload = workload.name, "Successful workload");

    execute_run(meili_client, master_key, &workload, args, binary_path).await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)] // not best code quality, but this is a benchmark runner
#[tracing::instrument(skip(meili_client, workload, master_key, args), fields(workload = %workload.name))]
async fn execute_run(
    meili_client: &Arc<Client>,
    master_key: Option<&str>,
    workload: &RelevancyWorkload,
    args: &RelevancyArgs,
    binary_path: Option<&Path>,
) -> anyhow::Result<()> {
    delete_db().await;

    let assets = Arc::new(workload.assets.clone());
    let asset_folder = args.common.asset_folder.clone().leak();

    let binary = match binary_path {
        Some(binary_path) => Binary {
            description: Some("initial binary for the workload".into()),
            source: crate::common::instance::BinarySource::Path(binary_path.to_owned()),
            extra_cli_args: workload.extra_cli_args.clone(),
        },
        None => Binary {
            description: Some("initial binary for the workloard".into()),
            source: crate::common::instance::BinarySource::Build {
                edition: crate::common::instance::Edition::Community,
            },
            extra_cli_args: workload.extra_cli_args.clone(),
        },
    };

    let meilisearch =
        start_meili(meili_client, master_key, &binary, &args.common.asset_folder).await?;

    run_commands(
        meili_client,
        &workload.precommands,
        0,
        &assets,
        asset_folder,
        &mut HashMap::new(),
        false,
    )
    .await?;

    run_query_template(meili_client, &workload.query_template, &assets, asset_folder).await?;

    process::kill_meili(meilisearch).await;

    Ok(())
}

async fn run_query_template(
    meili_client: &Arc<Client>,
    query_template: &QueryTemplate,
    assets: &Arc<BTreeMap<String, Asset>>,
    asset_folder: &'static str,
) -> anyhow::Result<()> {
    let template = JsonTemplate::new(query_template.template.clone())
        .map_err(|err| anyhow::anyhow!(err.parsing_error("template")))?;
    let (query_set, format) = {
        let name = &query_template.query_set_asset;
        let context = || format!("while getting body from asset '{name}'");
        let (mut file, format) = fetch_asset(&name, assets, asset_folder).with_context(context)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).with_context(context)?;
        (buf, AssetFormat::auto_detect(name))
    };

    let query_set: QuerySet = match format {
        assets::AssetFormat::Json => serde_json::from_slice(&query_set)
            .context("while trying to deserialize querySet as a JSON object")?,
        assets::AssetFormat::NdJson => {
            let de =
                serde_json::StreamDeserializer::new(serde_json::de::SliceRead::new(&query_set));
            let queries: Result<Vec<QueryEntry>, _> = de.collect();
            QuerySet(queries.context("while deserializeing querySet as a NDJSON")?)
        }
        assets::AssetFormat::Raw | assets::AssetFormat::Auto => {
            bail!("unsupported querySet format")
        }
    };

    Ok(())
}

#[derive(Deserialize)]
struct QuerySet(Vec<QueryEntry>);

#[derive(Deserialize)]
struct QueryEntry {
    id: String,
    query: String,
    description: Option<String>,
    hits: Vec<Hit>,
}

#[derive(Deserialize)]
struct Hit {
    id: String,
    score: Option<f64>,
}
