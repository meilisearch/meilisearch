use std::rc::Rc;

use criterion::{criterion_group, criterion_main};
use milli::tokenizer::TokenizerBuilder;
use milli::{FormatOptions, MatcherBuilder, MatchingWord, MatchingWords};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct Conf<'a> {
    name: &'a str,
    text: &'a str,
    matching_words: MatcherBuilder<'a, Vec<u8>>,
}

fn bench_formatting(c: &mut criterion::Criterion) {
    #[rustfmt::skip]
    let confs = &[
    	Conf {
    		name: "'the door d'",
			text: r#"He used to do the door sounds in "Star Trek" with his mouth, phssst, phssst. The MD-11 passenger and cargo doors also tend to behave like electromagnetic apertures, because the doors do not have continuous electrical contact with the door frames around the door perimeter. But Theodor said that the doors don't work."#,
			matching_words: MatcherBuilder::new(MatchingWords::new(vec![
	            (vec![Rc::new(MatchingWord::new("t".to_string(), 0, false).unwrap()), Rc::new(MatchingWord::new("he".to_string(), 0, false).unwrap())], vec![0]),
	            (vec![Rc::new(MatchingWord::new("the".to_string(), 0, false).unwrap())], vec![0]),
	            (vec![Rc::new(MatchingWord::new("door".to_string(), 1, false).unwrap())], vec![1]),
	            (vec![Rc::new(MatchingWord::new("do".to_string(), 0, false).unwrap()), Rc::new(MatchingWord::new("or".to_string(), 0, false).unwrap())], vec![0]),
	            (vec![Rc::new(MatchingWord::new("thedoor".to_string(), 1, false).unwrap())], vec![0, 1]),
	            (vec![Rc::new(MatchingWord::new("d".to_string(), 0, true).unwrap())], vec![2]),
	            (vec![Rc::new(MatchingWord::new("thedoord".to_string(), 1, true).unwrap())], vec![0, 1, 2]),
	            (vec![Rc::new(MatchingWord::new("doord".to_string(), 1, true).unwrap())], vec![1, 2]),
        	]
            ), TokenizerBuilder::default().build()),
		},
    ];

    let format_options = &[
        FormatOptions { highlight: false, crop: None },
        FormatOptions { highlight: true, crop: None },
        FormatOptions { highlight: false, crop: Some(10) },
        FormatOptions { highlight: true, crop: Some(10) },
        FormatOptions { highlight: false, crop: Some(20) },
        FormatOptions { highlight: true, crop: Some(20) },
    ];

    for option in format_options {
        let highlight = if option.highlight { "highlight" } else { "no-highlight" };

        let name = match option.crop {
            Some(size) => format!("{}-crop({})", highlight, size),
            None => format!("{}-no-crop", highlight),
        };

        let mut group = c.benchmark_group(&name);
        for conf in confs {
            group.bench_function(conf.name, |b| {
                b.iter(|| {
                    let mut matcher = conf.matching_words.build(conf.text);
                    matcher.format(*option);
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_formatting);
criterion_main!(benches);
