use std::io::Write;

use milli::vector::settings::{
    EmbedderSource, EmbeddingSettings, FieldStatus, MetaEmbeddingSetting, NestingContext,
    ReindexOutcome,
};

pub trait Formatter {
    fn begin_document(&mut self);
    fn end_document(&mut self);

    fn begin_header(&mut self);
    fn put_source_header(&mut self, source: EmbedderSource);
    fn end_header(&mut self);

    fn begin_setting(
        &mut self,
        setting: MetaEmbeddingSetting,
        description: &'static str,
        kind: &'static str,
        reindex_outcome: ReindexOutcome,
        default_value: &'static str,
    );
    fn end_setting(&mut self, setting: MetaEmbeddingSetting);

    fn put_setting_status(
        &mut self,
        source: EmbedderSource,
        field_status_by_nesting_context: FieldStatusByNestingContext,
    );
}

pub struct GitHubMdFormatter<W> {
    w: W,
}

impl<W: Write> GitHubMdFormatter<W> {
    pub fn new(w: W) -> Self {
        Self { w }
    }
}

impl<W: Write> Formatter for GitHubMdFormatter<W> {
    fn begin_document(&mut self) {
        let s = r#"

<table>
<tbody>
        "#;
        write!(self.w, "{s}").unwrap()
    }

    fn end_document(&mut self) {
        write!(
            self.w,
            r#"
</tbody>
</table>
        "#
        )
        .unwrap()
    }

    fn begin_header(&mut self) {
        write!(
            self.w,
            r#"
<thead>
<tr>
<th>Setting</th>
<th>Description</th>
<th>Type</th>
<th>Default Value</th>
<th>Regenerate on Change</th>
<th colspan="6">Availability for source</th>
</tr>
<tr>
<th colspan="5"></th>
            "#
        )
        .unwrap()
    }
    fn put_source_header(&mut self, source: EmbedderSource) {
        write!(
            self.w,
            r#"
<th>

{source}

</th>
        "#
        )
        .unwrap()
    }
    fn end_header(&mut self) {
        write!(
            self.w,
            r#"
</tr>
</thead>
        "#
        )
        .unwrap()
    }

    fn begin_setting(
        &mut self,
        setting: MetaEmbeddingSetting,
        description: &'static str,
        kind: &'static str,
        reindex_outcome: ReindexOutcome,
        default_value: &'static str,
    ) {
        let name = setting.name();
        let reindex_outcome = match reindex_outcome {
            ReindexOutcome::AlwaysReindex => "🏗️ Always",
            ReindexOutcome::NeverReindex => "🌱 Never",
            ReindexOutcome::ReindexSometimes(sometimes) => sometimes,
        };
        write!(
            self.w,
            r#"
<tr>
<td>

`{name}`

</td>
<td>

{description}

</td>
<td>

{kind}

</td>
<td>

{default_value}

</td>
<td>

{reindex_outcome}

</td>
        "#
        )
        .unwrap()
    }

    fn end_setting(&mut self, _setting: MetaEmbeddingSetting) {
        write!(
            self.w,
            r#"


</tr>
        "#
        )
        .unwrap()
    }

    fn put_setting_status(
        &mut self,
        _source: EmbedderSource,
        field_status_by_nesting_context: FieldStatusByNestingContext,
    ) {
        let field_status = match field_status_by_nesting_context {
            FieldStatusByNestingContext::Invariant(field_status) => {
                format_field_status(field_status).to_string()
            }
            FieldStatusByNestingContext::Variant(variant_field_status_by_nesting_context) => {
                format!(
                    r#"
- Usually, {}
- When used in `searchEmbedder` in a `composite` embedder, {}
- When used in `indexingEmbedder` in a `composite` embedder, {}
                "#,
                    format_field_status(variant_field_status_by_nesting_context.not_nested),
                    format_field_status(variant_field_status_by_nesting_context.search),
                    format_field_status(variant_field_status_by_nesting_context.index)
                )
            }
        };
        write!(
            self.w,
            r#"
<td>

{field_status}

</td>
        "#
        )
        .unwrap();
    }
}

fn format_field_status(field_status: FieldStatus) -> &'static str {
    match field_status {
        FieldStatus::Mandatory => "🔐 **Mandatory**",
        FieldStatus::Allowed => "✅ Allowed",
        FieldStatus::Disallowed => "🚫 Disallowed",
    }
}

pub struct GitHubMdAvailabilityFormatter<W>(pub GitHubMdFormatter<W>);
impl<W: Write> Formatter for GitHubMdAvailabilityFormatter<W> {
    fn begin_document(&mut self) {
        write!(self.0.w, "## Availability of the settings depending on the selected source\n\n")
            .unwrap();
        self.0.begin_document();
    }

    fn end_document(&mut self) {
        self.0.end_document();
    }

    fn begin_header(&mut self) {
        write!(
            self.0.w,
            r#"
<thead>
<tr>
<th>Setting</th>
            "#
        )
        .unwrap()
    }

    fn put_source_header(&mut self, source: EmbedderSource) {
        self.0.put_source_header(source);
    }

    fn end_header(&mut self) {
        self.0.end_header();
    }

    fn begin_setting(
        &mut self,
        setting: MetaEmbeddingSetting,
        _description: &'static str,
        _kind: &'static str,
        _reindex_outcome: ReindexOutcome,
        _default_value: &'static str,
    ) {
        if setting == MetaEmbeddingSetting::Source {
            return;
        }
        let name = setting.name();
        write!(
            self.0.w,
            r#"
<tr>
<td>

`{name}`

</td>
        "#
        )
        .unwrap()
    }

    fn end_setting(&mut self, setting: MetaEmbeddingSetting) {
        if setting == MetaEmbeddingSetting::Source {
            return;
        }
        self.0.end_setting(setting);
    }

    fn put_setting_status(
        &mut self,
        source: EmbedderSource,
        field_status_by_nesting_context: FieldStatusByNestingContext,
    ) {
        self.0.put_setting_status(source, field_status_by_nesting_context);
    }
}

pub struct GitHubMdBasicFormatter<W>(pub GitHubMdFormatter<W>);
impl<W: Write> Formatter for GitHubMdBasicFormatter<W> {
    fn begin_document(&mut self) {
        write!(self.0.w, "## List of the embedder settings\n\n").unwrap();
        self.0.begin_document();
    }

    fn end_document(&mut self) {
        self.0.end_document();
    }

    fn begin_header(&mut self) {
        write!(
            self.0.w,
            r#"
<thead>
<tr>
<th>Setting</th>
<th>Description</th>
<th>Type</th>
<th>Default Value</th>
<th>Regenerate on Change</th>
            "#
        )
        .unwrap()
    }

    fn put_source_header(&mut self, _source: EmbedderSource) {}

    fn end_header(&mut self) {
        self.0.end_header();
    }

    fn begin_setting(
        &mut self,
        setting: MetaEmbeddingSetting,
        description: &'static str,
        kind: &'static str,
        reindex_outcome: ReindexOutcome,
        default_value: &'static str,
    ) {
        self.0.begin_setting(setting, description, kind, reindex_outcome, default_value);
    }

    fn end_setting(&mut self, setting: MetaEmbeddingSetting) {
        self.0.end_setting(setting);
    }

    fn put_setting_status(
        &mut self,
        _source: EmbedderSource,
        _field_status_by_nesting_context: FieldStatusByNestingContext,
    ) {
    }
}

pub enum FieldStatusByNestingContext {
    Invariant(FieldStatus),
    Variant(VariantFieldStatusByNestingContext),
}

pub struct VariantFieldStatusByNestingContext {
    not_nested: FieldStatus,
    search: FieldStatus,
    index: FieldStatus,
}

fn format_settings(mut fmt: impl Formatter) {
    #![allow(unused_labels)] // the labels are used as documentation
    fmt.begin_document();
    fmt.begin_header();
    for source in enum_iterator::all::<EmbedderSource>() {
        fmt.put_source_header(source);
    }
    fmt.end_header();
    'setting: for setting in enum_iterator::all::<MetaEmbeddingSetting>() {
        let description = setting.description();
        let kind = setting.kind();
        let reindex_outcome = setting.reindex_outcome();
        let default_value = setting.default_value();
        fmt.begin_setting(setting, description, kind, reindex_outcome, default_value);

        'source: for source in enum_iterator::all::<EmbedderSource>() {
            if setting == MetaEmbeddingSetting::Source {
                break 'source;
            }
            let mut field_status = VariantFieldStatusByNestingContext {
                not_nested: FieldStatus::Disallowed,
                search: FieldStatus::Disallowed,
                index: FieldStatus::Disallowed,
            };
            'nesting: for nesting_context in enum_iterator::all::<NestingContext>() {
                let status = EmbeddingSettings::field_status(source, setting, nesting_context);

                match nesting_context {
                    NestingContext::NotNested => {
                        field_status.not_nested = status;
                    }
                    NestingContext::Search => {
                        field_status.search = status;
                    }
                    NestingContext::Indexing => {
                        field_status.index = status;
                    }
                }
            }
            let field_status_by_nesting_context = if field_status.index == field_status.search
                && field_status.search == field_status.not_nested
            {
                FieldStatusByNestingContext::Invariant(field_status.not_nested)
            } else {
                FieldStatusByNestingContext::Variant(field_status)
            };
            fmt.put_setting_status(source, field_status_by_nesting_context);
        }
        fmt.end_setting(setting);
    }
    fmt.end_document();
}

fn main() {
    let mut std_out = std::io::stdout().lock();

    write!(
        &mut std_out,
        "The tables below have been generated by calling `cargo run --bin embedder_settings`\n\n"
    )
    .unwrap();

    let formatter = GitHubMdFormatter::new(&mut std_out);
    let formatter = GitHubMdBasicFormatter(formatter);
    format_settings(formatter);

    write!(&mut std_out, "\n\n").unwrap();

    let formatter = GitHubMdFormatter::new(&mut std_out);
    let formatter = GitHubMdAvailabilityFormatter(formatter);
    format_settings(formatter);
}
