use serde_json::json;

use crate::analytics::Aggregate;
use crate::routes::indexes::render::RenderQuery;

#[derive(Default)]
pub struct RenderAggregator {
    // requests
    total_received: usize,
    total_succeeded: usize,

    // parameters
    template_inline: bool,
    template_id: bool,
    input_inline: bool,
    input_id: bool,
    input_omitted: bool,
    fields_forced: bool,
    fields_disabled: bool,
}

impl RenderAggregator {
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_query(query: &RenderQuery) -> Self {
        let RenderQuery {
            template,
            input,
        } = query;

        let mut ret = Self::default();

        ret.total_received = 1;

        ret.template_inline = template.inline.is_some();
        ret.template_id = template.id.is_some();
        ret.input_inline = input.as_ref().is_some_and(|i| i.inline.is_some());
        ret.input_id = input.as_ref().is_some_and(|i| i.document_id.is_some());
        ret.input_omitted = input.as_ref().is_none();
        ret.fields_forced = input.as_ref().is_some_and(|i| i.insert_fields.is_some());
        ret.fields_disabled = input.as_ref().is_some_and(|i| i.insert_fields.is_none());

        ret
    }

    pub fn succeed(&mut self) {
        self.total_succeeded += 1;
    }
}

impl Aggregate for RenderAggregator {
    fn event_name(&self) -> &'static str {
        "Documents Rendered"
    }

    fn aggregate(mut self: Box<Self>, new: Box<Self>) -> Box<Self> {
        self.total_received += new.total_received;
        self.total_succeeded += new.total_succeeded;

        self.template_inline |= new.template_inline;
        self.template_id |= new.template_id;
        self.input_inline |= new.input_inline;
        self.input_id |= new.input_id;
        self.input_omitted |= new.input_omitted;
        self.fields_forced |= new.fields_forced;
        self.fields_disabled |= new.fields_disabled;

        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        let Self {
            total_received,
            total_succeeded,
            template_inline,
            template_id,
            input_inline,
            input_id,
            input_omitted,
            fields_forced,
            fields_disabled,
        } = *self;

        json!({
            "requests": {
                "total_received": total_received,
                "total_succeeded": total_succeeded,
                "total_failed": total_received.saturating_sub(total_succeeded) // just to be sure we never panics
            },
            "template": {
                "inline": template_inline,
                "id": template_id,
            },
            "input": {
                "inline": input_inline,
                "id": input_id,
                "omitted": input_omitted,
                "fields_forced": fields_forced,
                "fields_disabled": fields_disabled,
            },
        })
    }
}
