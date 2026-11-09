use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::io::Read as _;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context as _};
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use milli::vector::json_template::JsonTemplate;
use serde::{Deserialize, Serialize};

use crate::common::assets::{self, fetch_asset, Asset, AssetFormat};
use crate::common::client::Client;
use crate::common::command::{run_commands, Command};
use crate::common::instance::Binary;
use crate::common::process::{self, delete_temporary_db, start_meili};
use crate::relevancy::RelevancyArgs;

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
    primary_key: String,
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
    tracing::info!(workload = workload.name, "Workload done and reports written");

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
    let db_path = delete_temporary_db(args.common.reuse_db.as_deref()).await?;

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

    let report_folder = &args.report_folder;
    std::fs::create_dir_all(report_folder)
        .with_context(|| format!("could not create report directory at {report_folder}"))?;

    let meilisearch =
        start_meili(meili_client, master_key, &binary, &args.common.asset_folder, &db_path).await?;

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

    run_query_template(
        meili_client,
        &workload.query_template,
        &assets,
        asset_folder,
        &Path::new(&args.report_folder),
        &workload.name,
    )
    .await?;

    process::kill_meili(meilisearch).await;

    Ok(())
}

async fn run_query_template(
    meili_client: &Arc<Client>,
    query_template: &QueryTemplate,
    assets: &Arc<BTreeMap<String, Asset>>,
    asset_folder: &'static str,
    report_folder: &Path,
    workload_name: &str,
) -> anyhow::Result<()> {
    let template = JsonTemplate::new(query_template.template.clone())
        .map_err(|err| anyhow::anyhow!(err.parsing_error("template")))?;
    let (query_set, format) = {
        let name = &query_template.query_set_asset;
        let context = || format!("while getting body from asset '{name}'");
        let (mut file, _format) = fetch_asset(name, assets, asset_folder).with_context(context)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).with_context(context)?;
        (buf, AssetFormat::auto_detect(name))
    };

    let QuerySet(query_set) = match format {
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

    async fn send_query(
        meili_client: Arc<Client>,
        query_entry: QueryEntry,
        template: Arc<JsonTemplate>,
        primary_key: Arc<String>,
        assets: Arc<BTreeMap<String, Asset>>,
        asset_folder: &'static str,
    ) -> anyhow::Result<RelevancyResult> {
        let mut query: serde_json::Value = template
            .render_search(Some(query_entry.query.as_str()), None)
            .map_err(|err| anyhow::anyhow!(err.rendering_error("template")))?;

        query["offset"] = 0.into();
        query["limit"] = 50.into();

        let ideal_hits = query_entry.ideal_hits();

        let ips_at_1 = query_entry.ideal_precision_score_for(1);
        let irs_at_1 = query_entry.ideal_recall_for(1);

        let ips_at_5 = query_entry.ideal_precision_score_for(5);
        let irs_at_5 = query_entry.ideal_recall_for(5);

        let ips_at_20 = query_entry.ideal_precision_score_for(20);
        let irs_at_20 = query_entry.ideal_recall_for(20);

        let ips_at_50 = query_entry.ideal_precision_score_for(50);
        let irs_at_50 = query_entry.ideal_recall_for(50);

        let command = Command {
            description: query_entry.description,
            route: "/indexes/relevancy/search".to_string(),
            method: crate::common::client::Method::Post,
            body: crate::common::command::Body::Inline { inline: query },
            expected_status: Some(200),
            expected_response: None,
            register: Default::default(),
            api_key_variable: None,
            synchronous: crate::common::command::SyncMode::WaitForResponse,
        };
        let Some((response, _code)) = crate::common::command::run(
            &meili_client,
            &command,
            0,
            &assets,
            Default::default(),
            asset_folder,
            true,
        )
        .await?
        else {
            anyhow::bail!("missing response for query `{}`", query_entry.id);
        };

        let hits = response
            .as_object()
            .context("response must be an object")?
            .get("hits")
            .context("`hits` must be present")?
            .as_array()
            .context("`hits` must be an array")?;

        let hits: anyhow::Result<Vec<_>> = hits
            .iter()
            .map(|hit| {
                hit.as_object()
                    .context("`hit` must be an object")?
                    .get(primary_key.as_str())
                    .context(
                        "the primary key must be present, check the declared pk in your workload",
                    )
                    .and_then(|pk| pk.as_str().context("docid must be a string"))
            })
            .collect();
        let hits = hits?;

        let score_sum = std::cell::Cell::new(0.0);
        let mut it = hits.iter().map(|hit| {
            score_sum.update(|score_sum| {
                let score = ideal_hits.get(*hit).copied().unwrap_or(0.0);
                if query_entry.id == "42" {
                    dbg!(hit);
                    dbg!(score);
                }
                score_sum + score
            })
        });

        it.next();

        // hits len are always considered to be always 1 so we don't divide by 0
        let hits_len = usize::max(1, hits.len());

        let ps_at_1: f64 = score_sum.get() / usize::min(1, hits_len) as f64;
        let rs_at_1: f64 = score_sum.get();

        it.nth(3);

        let ps_at_5: f64 = score_sum.get() / usize::min(hits_len, 5) as f64;
        let rs_at_5: f64 = score_sum.get();

        if query_entry.id == "42" {
            dbg!(hits.len());
            dbg!(ideal_hits.len());
            dbg!(ips_at_5);
            dbg!(ps_at_5);
        }

        it.nth(14);

        let ps_at_20: f64 = score_sum.get() / usize::min(hits_len, 20) as f64;
        let rs_at_20: f64 = score_sum.get();

        it.nth(29);

        let ps_at_50: f64 = score_sum.get() / usize::min(hits_len, 50) as f64;
        let rs_at_50: f64 = score_sum.get();

        Ok(RelevancyResult {
            q_id: query_entry.id,
            precision_at_1: ps_at_1 / ips_at_1,
            precision_at_5: ps_at_5 / ips_at_5,
            precision_at_20: ps_at_20 / ips_at_20,
            precision_at_50: ps_at_50 / ips_at_50,
            recall_at_1: rs_at_1 / irs_at_1,
            recall_at_5: rs_at_5 / irs_at_5,
            recall_at_20: rs_at_20 / irs_at_20,
            recall_at_50: rs_at_50 / irs_at_50,
        })
    }
    let template = Arc::new(template);
    let primary_key = query_template.primary_key.clone();
    let primary_key = Arc::new(primary_key);

    let mut unordered: FuturesUnordered<_> = query_set
        .into_iter()
        .map({
            move |query_entry| {
                tokio::spawn(send_query(
                    meili_client.clone(),
                    query_entry,
                    template.clone(),
                    primary_key.clone(),
                    assets.clone(),
                    asset_folder,
                ))
            }
        })
        .collect();

    let mut relevancy_results = Vec::new();
    let report_csv_file = report_folder.join(workload_name).with_extension("csv");
    let mut csv_writer = csv::Writer::from_path(report_csv_file)?;
    while let Some(result) = unordered.next().await {
        let result = result.unwrap()?;
        csv_writer.serialize(&result)?;
        relevancy_results.push(result);
    }
    csv_writer.flush()?;

    plot(report_folder, workload_name, relevancy_results)?;

    Ok(())
}

fn plot(
    report_folder: &Path,
    workload_name: &str,
    relevancy_results: Vec<RelevancyResult>,
) -> anyhow::Result<()> {
    use plotters::prelude::*;

    let report_file = report_folder.join(workload_name).with_extension("png");
    let root = BitMapBackend::new(&report_file, (1000, 850)).into_drawing_area();
    root.fill(&WHITE)?;

    let quartiles_precision_at_1 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.precision_at_1).collect::<Vec<_>>());
    let quartiles_recall_at_1 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.recall_at_1).collect::<Vec<_>>());

    let quartiles_precision_at_5 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.precision_at_5).collect::<Vec<_>>());
    let quartiles_recall_at_5 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.recall_at_5).collect::<Vec<_>>());

    let quartiles_precision_at_20 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.precision_at_20).collect::<Vec<_>>());
    let quartiles_recall_at_20 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.recall_at_20).collect::<Vec<_>>());

    let quartiles_precision_at_50 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.precision_at_50).collect::<Vec<_>>());
    let quartiles_recall_at_50 =
        Quartiles::new(&relevancy_results.iter().map(|rr| rr.recall_at_50).collect::<Vec<_>>());

    let axis = [
        "precision@1",
        "recall@1",
        "precision@5",
        "recall@5",
        "precision@20",
        "recall@20",
        "precision@50",
        "recall@50",
    ];

    let mut chart = ChartBuilder::on(&root)
        .margin(20)
        .x_label_area_size(40)
        .y_label_area_size(40)
        .caption(format!(r#""{workload_name}" Relevancy Boxplot"#), ("sans-serif", 20))
        .build_cartesian_2d(axis[..].into_segmented(), 0_f32..1.0_f32)?;

    chart.configure_mesh().light_line_style(WHITE).draw()?;
    chart.draw_series(vec![
        Boxplot::new_vertical(SegmentValue::CenterOf(&"precision@1"), &quartiles_precision_at_1)
            .style(&BLUE),
        Boxplot::new_vertical(SegmentValue::CenterOf(&"recall@1"), &quartiles_recall_at_1)
            .style(&RED),
        Boxplot::new_vertical(SegmentValue::CenterOf(&"precision@5"), &quartiles_precision_at_5)
            .style(&BLUE),
        Boxplot::new_vertical(SegmentValue::CenterOf(&"recall@5"), &quartiles_recall_at_5)
            .style(&RED),
        Boxplot::new_vertical(SegmentValue::CenterOf(&"precision@20"), &quartiles_precision_at_20)
            .style(&BLUE),
        Boxplot::new_vertical(SegmentValue::CenterOf(&"recall@20"), &quartiles_recall_at_20)
            .style(&RED),
        Boxplot::new_vertical(SegmentValue::CenterOf(&"precision@50"), &quartiles_precision_at_50)
            .style(&BLUE),
        Boxplot::new_vertical(SegmentValue::CenterOf(&"recall@50"), &quartiles_recall_at_50)
            .style(&RED),
    ])?;

    root.present()
        .with_context(|| format!("while trying to plot into `{}`", report_file.display()))?;

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

impl QueryEntry {
    /// Returns the best possible precision score for a query of `limit` documents.
    fn ideal_precision_score_for(&self, limit: usize) -> f64 {
        assert!(
            self.hits.is_sorted_by_key(|hit| Reverse(hit.score)),
            "hits must be sorted by descending score"
        );

        self.hits.iter().take(limit).map(|h| h.score).sum::<f64>()
            / usize::max(1, usize::min(self.hits.len(), limit)) as f64
    }

    /// Returns the best possible recall score for a query of `limit` documents.
    fn ideal_recall_for(&self, limit: usize) -> f64 {
        assert!(
            self.hits.is_sorted_by_key(|hit| Reverse(hit.score)),
            "hits must be sorted by descending score"
        );

        self.hits.iter().take(limit).map(|h| h.score).sum::<f64>()
    }

    fn ideal_hits(&self) -> HashMap<String, f64> {
        self.hits.iter().map(|hit| (hit.id.clone(), hit.score)).collect()
    }
}

#[derive(Deserialize)]
struct Hit {
    id: String,
    #[serde(default = "Hit::default_score")]
    score: f64,
}

impl Hit {
    fn default_score() -> f64 {
        1.0
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RelevancyResult {
    q_id: String,
    #[serde(rename = "precision@1")]
    precision_at_1: f64,
    #[serde(rename = "precision@5")]
    precision_at_5: f64,
    #[serde(rename = "precision@20")]
    precision_at_20: f64,
    #[serde(rename = "precision@50")]
    precision_at_50: f64,
    #[serde(rename = "recall@1")]
    recall_at_1: f64,
    #[serde(rename = "recall@5")]
    recall_at_5: f64,
    #[serde(rename = "recall@20")]
    recall_at_20: f64,
    #[serde(rename = "recall@50")]
    recall_at_50: f64,
}
