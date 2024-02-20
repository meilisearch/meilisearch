#![allow(non_snake_case)]
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use actix_web::dev::Payload;
use actix_web::{FromRequest, Handler, HttpRequest};
use pin_project_lite::pin_project;

/// `SeqHandler` is an actix `Handler` that enforces that extractors errors are returned in the
/// same order as they are defined in the wrapped handler. This is needed because, by default, actix
/// resolves the extractors concurrently, whereas we always need the authentication extractor to
/// throw first.
#[derive(Clone)]
pub struct SeqHandler<H>(pub H);

pub struct SeqFromRequest<T>(T);

/// This macro implements `FromRequest` for arbitrary arity handler, except for one, which is
/// useless anyway.
macro_rules! gen_seq {
    ($ty:ident; $($T:ident)+) => {
        pin_project! {
            pub struct $ty<$($T: FromRequest), +> {
                $(
                #[pin]
                $T: ExtractFuture<$T::Future, $T, $T::Error>,
                )+
            }
        }

        impl<$($T: FromRequest), +> Future for $ty<$($T),+> {
            type Output = Result<SeqFromRequest<($($T),+)>, actix_web::Error>;

            fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
                let mut this = self.project();

                let mut count_fut = 0;
                let mut count_finished = 0;

                $(
                count_fut += 1;
                match this.$T.as_mut().project() {
                    ExtractProj::Future { fut } => match fut.poll(cx) {
                        Poll::Ready(Ok(output)) => {
                            count_finished += 1;
                            let _ = this
                                .$T
                                .as_mut()
                                .project_replace(ExtractFuture::Done { output });
                        }
                        Poll::Ready(Err(error)) => {
                            count_finished += 1;
                            let _ = this
                                .$T
                                .as_mut()
                                .project_replace(ExtractFuture::Error { error });
                        }
                        Poll::Pending => (),
                    },
                    ExtractProj::Done { .. } => count_finished += 1,
                    ExtractProj::Error { .. } => {
                        // short circuit if all previous are finished and we had an error.
                        if count_finished == count_fut {
                            match this.$T.project_replace(ExtractFuture::Empty) {
                                ExtractReplaceProj::Error { error } => {
                                    return Poll::Ready(Err(error.into()))
                                }
                                _ => unreachable!("Invalid future state"),
                            }
                        } else {
                            count_finished += 1;
                        }
                    }
                    ExtractProj::Empty => unreachable!("From request polled after being finished. {}", stringify!($T)),
                }
                )+

                if count_fut == count_finished {
                    let result = (
                        $(
                            match this.$T.project_replace(ExtractFuture::Empty) {
                                ExtractReplaceProj::Done { output } => output,
                                ExtractReplaceProj::Error { error } => return Poll::Ready(Err(error.into())),
                                _ => unreachable!("Invalid future state"),
                            },
                        )+
                    );

                    Poll::Ready(Ok(SeqFromRequest(result)))
                } else {
                    Poll::Pending
                }
            }
        }

        impl<$($T: FromRequest,)+> FromRequest for SeqFromRequest<($($T,)+)> {
            type Error = actix_web::Error;

            type Future = $ty<$($T),+>;

            fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
                $ty {
                $(
                    $T: ExtractFuture::Future {
                        fut: $T::from_request(req, payload),
                    },
                )+
                }
            }
        }

    impl<Han, $($T: FromRequest),+> Handler<SeqFromRequest<($($T),+)>> for SeqHandler<Han>
    where
        Han: Handler<($($T),+)>,
    {
        type Output = Han::Output;
        type Future = Han::Future;

        fn call(&self, args: SeqFromRequest<($($T),+)>) -> Self::Future {
            self.0.call(args.0)
        }
    }
    };
}

// Not working for a single argument, but then, it is not really necessary.
// gen_seq! { SeqFromRequestFut1; A }
gen_seq! { SeqFromRequestFut2; A B }
gen_seq! { SeqFromRequestFut3; A B C }
gen_seq! { SeqFromRequestFut4; A B C D }
gen_seq! { SeqFromRequestFut5; A B C D E }
gen_seq! { SeqFromRequestFut6; A B C D E F }
gen_seq! { SeqFromRequestFut7; A B C D E F G }

pin_project! {
    #[project = ExtractProj]
    #[project_replace = ExtractReplaceProj]
    enum ExtractFuture<Fut, Res, Err> {
        Future {
            #[pin]
            fut: Fut,
        },
        Done {
            output: Res,
        },
        Error {
            error: Err,
        },
        Empty,
    }
}
