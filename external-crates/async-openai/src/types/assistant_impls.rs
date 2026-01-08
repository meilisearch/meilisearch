use super::{
    AssistantToolCodeInterpreterResources, AssistantToolFileSearchResources,
    AssistantToolResources, AssistantTools, AssistantToolsFileSearch, AssistantToolsFunction,
    CreateAssistantToolFileSearchResources, CreateAssistantToolResources, FunctionObject,
};

impl From<AssistantToolsFileSearch> for AssistantTools {
    fn from(value: AssistantToolsFileSearch) -> Self {
        Self::FileSearch(value)
    }
}

impl From<AssistantToolsFunction> for AssistantTools {
    fn from(value: AssistantToolsFunction) -> Self {
        Self::Function(value)
    }
}

impl From<FunctionObject> for AssistantToolsFunction {
    fn from(value: FunctionObject) -> Self {
        Self { function: value }
    }
}

impl From<FunctionObject> for AssistantTools {
    fn from(value: FunctionObject) -> Self {
        Self::Function(value.into())
    }
}

impl From<CreateAssistantToolFileSearchResources> for CreateAssistantToolResources {
    fn from(value: CreateAssistantToolFileSearchResources) -> Self {
        Self {
            code_interpreter: None,
            file_search: Some(value),
        }
    }
}

impl From<AssistantToolCodeInterpreterResources> for CreateAssistantToolResources {
    fn from(value: AssistantToolCodeInterpreterResources) -> Self {
        Self {
            code_interpreter: Some(value),
            file_search: None,
        }
    }
}

impl From<AssistantToolCodeInterpreterResources> for AssistantToolResources {
    fn from(value: AssistantToolCodeInterpreterResources) -> Self {
        Self {
            code_interpreter: Some(value),
            file_search: None,
        }
    }
}

impl From<AssistantToolFileSearchResources> for AssistantToolResources {
    fn from(value: AssistantToolFileSearchResources) -> Self {
        Self {
            code_interpreter: None,
            file_search: Some(value),
        }
    }
}
