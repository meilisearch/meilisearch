mod datasets_paths;
mod utils;

use criterion::{criterion_group, criterion_main};
use milli::update::Settings;
use milli::FilterableAttributesRule;
use utils::Conf;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn base_conf(builder: &mut Settings) {
    let displayed_fields =
        ["id", "title", "album", "artist", "genre", "country", "released", "duration"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    builder.set_displayed_fields(displayed_fields);

    let searchable_fields = ["title", "album", "artist"].iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);

    let faceted_fields = ["released-timestamp", "duration-float", "genre", "country", "artist"]
        .iter()
        .map(|s| FilterableAttributesRule::Field(s.to_string()))
        .collect();
    builder.set_filterable_fields(faceted_fields);
}

#[rustfmt::skip]
const BASE_CONF: Conf = Conf {
    dataset: datasets_paths::SMOL_SONGS,
    queries: &[
        "john ",             // 9097
        "david ",            // 4794
        "charles ",          // 1957
        "david bowie ",      // 1200
        "michael jackson ",  // 600
        "thelonious monk ",  // 303
        "charles mingus ",   // 142
        "marcus miller ",    // 60
        "tamo ",             // 13
        "Notstandskomitee ", // 4
    ],
    configure: base_conf,
    primary_key: Some("id"),
    ..Conf::BASE
};

fn bench_songs(c: &mut criterion::Criterion) {
    let default_criterion: Vec<String> =
        milli::default_criteria().iter().map(|criteria| criteria.to_string()).collect();
    let default_criterion = default_criterion.iter().map(|s| s.as_str());
    let asc_default: Vec<&str> =
        std::iter::once("released-timestamp:asc").chain(default_criterion.clone()).collect();
    let desc_default: Vec<&str> =
        std::iter::once("released-timestamp:desc").chain(default_criterion.clone()).collect();

    let basic_with_quote: Vec<String> = BASE_CONF
        .queries
        .iter()
        .map(|s| {
            s.trim().split(' ').map(|s| format!(r#""{}""#, s)).collect::<Vec<String>>().join(" ")
        })
        .collect();
    let basic_with_quote: &[&str] =
        &basic_with_quote.iter().map(|s| s.as_str()).collect::<Vec<&str>>();

    #[rustfmt::skip]
    let confs = &[
        /* first we bench each criterion alone */
        utils::Conf {
            group_name: "proximity",
            queries: &[
                "black saint sinner lady ",
                "les dangeureuses 1960 ",
                "The Disneyland Sing-Along Chorus ",
                "Under Great Northern Lights ",
                "7000 Danses Un Jour Dans Notre Vie ",
            ],
            criterion: Some(&["proximity"]),
            optional_words: false,
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "typo",
            queries: &[
                "mongus ",
                "thelonius monk ",
                "Disnaylande ",
                "the white striper ",
                "indochie ",
                "indochien ",
                "klub des loopers ",
                "fear of the duck ",
                "michel depech ",
                "stromal ",
                "dire straights ",
                "Arethla Franklin ",
            ],
            criterion: Some(&["typo"]),
            optional_words: false,
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "words",
            queries: &[
                "the black saint and the sinner lady and the good doggo ", // four words to pop
                "les liaisons dangeureuses 1793 ",                         // one word to pop
                "The Disneyland Children's Sing-Alone song ",              // two words to pop
                "seven nation mummy ",                                     // one word to pop
                "7000 Danses / Le Baiser / je me trompe de mots ",         // four words to pop
                "Bring Your Daughter To The Slaughter but now this is not part of the title ", // nine words to pop
                "whathavenotnsuchforth and a good amount of words to pop to match the first one ", // 13
            ],
            criterion: Some(&["words"]),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "asc",
            criterion: Some(&["released-timestamp:desc"]),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "desc",
            criterion: Some(&["released-timestamp:desc"]),
            ..BASE_CONF
        },

        /* then we bench the asc and desc criterion on top of the default criterion */
        utils::Conf {
            group_name: "asc + default",
            criterion: Some(&asc_default[..]),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "desc + default",
            criterion: Some(&desc_default[..]),
            ..BASE_CONF
        },

        /* we bench the filters with the default request */
        utils::Conf {
            group_name: "basic filter: <=",
            filter: Some("released-timestamp <= 946728000"), // year 2000
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "basic filter: TO",
            filter: Some("released-timestamp 946728000 TO 1262347200"), // year 2000 to 2010
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "big filter",
            filter: Some("released-timestamp != 1262347200 AND (NOT (released-timestamp = 946728000)) AND (duration-float = 1 OR (duration-float 1.1 TO 1.5 AND released-timestamp > 315576000))"),
            ..BASE_CONF
        },

        /* the we bench some global / normal search with all the default criterion in the default
         * order */
        utils::Conf {
            group_name: "basic placeholder",
            queries: &[""],
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "basic without quote",
            queries: &BASE_CONF
                .queries
                .iter()
                .map(|s| s.trim()) // we remove the space at the end of each request
                .collect::<Vec<&str>>(),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "basic with quote",
            queries: basic_with_quote,
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "prefix search",
            queries: &[
                "s", // 500k+ results
                "a", //
                "b", //
                "i", //
                "x", // only 7k results
            ],
            ..BASE_CONF
        },
    ];

    utils::run_benches(c, confs);
}

criterion_group!(benches, bench_songs);
criterion_main!(benches);
