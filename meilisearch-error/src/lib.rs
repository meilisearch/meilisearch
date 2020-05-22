#![allow(dead_code)]

use std::fmt;
use actix_http::http::StatusCode;

pub trait ErrorCode: std::error::Error {
    fn error_code(&self) -> Code;
}

enum ErrorCategory {
   None = 0,
}

pub enum Code {
    BadParameter,
    BadRequest,
    CreateIndex,
    DocumentNotFound,
    IndexNotFound,
    Internal,
    InvalidIndexUid,
    InvalidToken,
    Maintenance,
    MissingAuthorizationHeader,
    MissingHeader,
    NotFound,
    OpenIndex,
    RetrieveDocument,
    SearchDocuments,
    PayloadTooLarge,
    UnsupportedMediaType,
    Other,
}

impl Code {

    /// ascociate a `Code` variant to the actual ErrCode
    fn err_code(&self) -> ErrCode {
        use Code::*;

        match self {
            BadParameter
            | BadRequest
            | CreateIndex
            | InvalidIndexUid
            | OpenIndex
            | RetrieveDocument
            | SearchDocuments => ErrCode::bad_request(false, false, ErrorCategory::None, 0),
            DocumentNotFound
            | IndexNotFound
            | NotFound => ErrCode::not_found(false, false, ErrorCategory::None, 0),
            InvalidToken
            | MissingHeader => ErrCode::unauthorized(false, false, ErrorCategory::None, 0),
            MissingAuthorizationHeader => ErrCode::forbidden(false, false, ErrorCategory::None, 0),
            Internal => ErrCode::internal(false, false, ErrorCategory::None, 0),
            Maintenance =>  ErrCode::service_unavailable(false, false, ErrorCategory::None, 0),
            PayloadTooLarge => ErrCode::payload_too_large(false, false, ErrorCategory::None, 0),
            UnsupportedMediaType => ErrCode::unsupported_media_type(false, false, ErrorCategory::None, 0),
            _ => ErrCode::not_found(false, false, ErrorCategory::None, 0),
        }
    }

    /// return the HTTP status code ascociated with the `Code`
    pub fn http(&self) -> StatusCode {
        self.err_code().http_code
    }

    /// returns internal error code, in the form:
    /// `EPFCNN`
    /// - E: plain letter "E", to mark an error code, future main introduce W for warning
    /// - P: scope of the error, 0 for private, 1 for public. Private are error that make no sense
    /// reporting to the user, they are internal errors, and there is nothing the user can do about
    /// them. they are nonetheless returned, without a message, for assistance purpose.
    /// - F: 0 or 1, report if the error is fatal.
    /// - C: error category, number in 0-9, the category of the error. Categories are still to be determined, input is required.
    /// - NN: The error number, two digits, within C.


    pub fn internal(&self) -> String {
        let ErrCode { public, fatal, category, code, .. } = self.err_code();
        format!("E{}{}{}{}",
            public as u16,
            fatal as u16,
            category as u16,
            code)
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.internal().fmt(f)
    }
}

/// Internal structure providing a convenient way to create error codes
struct ErrCode {
    public: bool,
    fatal: bool,
    http_code: StatusCode,
    category: ErrorCategory,
    code: u16,
}

impl ErrCode {
    fn new(
        public: bool,
        fatal: bool,
        http_code: StatusCode,
        category: ErrorCategory,
        code: u16
    ) -> ErrCode {
        ErrCode {
            public,
            fatal,
            http_code,
            category,
            code,
        }
    }

    pub fn internal(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::INTERNAL_SERVER_ERROR, category, code)
    }

    pub fn bad_request(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::BAD_REQUEST, category, code)
    }

    pub fn unsupported_media_type(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::UNSUPPORTED_MEDIA_TYPE, category, code)
    }

    pub fn payload_too_large(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::PAYLOAD_TOO_LARGE, category, code)
    }

    pub fn service_unavailable(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::SERVICE_UNAVAILABLE, category, code)
    }

    pub fn forbidden(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::FORBIDDEN, category, code)
    }

    pub fn unauthorized(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::UNAUTHORIZED, category, code)
    }

    pub fn not_found(
        public: bool,
        fatal: bool,
        category: ErrorCategory,
        code: u16
        ) -> ErrCode {
        ErrCode::new(public, fatal, StatusCode::NOT_FOUND, category, code)
    }
}
