/*!
This module tests the Proximity ranking rule:

1. A proximity of >7 always has the same cost.

2. Phrase terms can be in sprximity to other terms via their start and end words,
   but we need to make sure that the phrase exists in the document that meets this
   proximity condition. This is especially relevant with split words and synonyms.

3. An ngram has the same sprximity cost as its component words being consecutive.
   e.g. `sunflower` equivalent to `sun flower`.

4. The prefix databases can be used to find the sprximity between two words, but
   they store fewer sprximities than the regular word sprximity DB.
*/

use std::collections::BTreeMap;

use crate::index::tests::TempIndex;
use crate::search::new::tests::collect_field_values;
use crate::{Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_simple_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Words, Criterion::Proximity]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "the very quick dark brown and smart fox did jump over the terribly lazy and small dog"
            },
            {
                "id": 1,
                "text": "the. quick brown fox jumps over the lazy. dog"
            },
            {
                "id": 2,
                "text": "the quick brown fox jumps over the lazy. dog"
            },
            {
                "id": 3,
                "text": "dog the quick brown fox jumps over the lazy"
            },
            {
                "id": 4,
                "text": "the quickbrown fox jumps over the lazy dog"
            },
            {
                "id": 5,
                "text": "brown quick fox jumps over the lazy dog"
            },
            {
                "id": 6,
                "text": "the really quick brown fox jumps over the very lazy dog"
            },
            {
                "id": 7,
                "text": "the really quick brown fox jumps over the lazy dog"
            },
            {
                "id": 8,
                "text": "the quick brown fox jumps over the lazy"
            },
            {
                "id": 9,
                "text": "the quack brown fox jumps over the lazy"
            },
            {
                "id": 9,
                "text": "the quack brown fox jumps over the lazy dog"
            },
            {
                "id": 10,
                "text": "the quick brown fox jumps over the lazy dog"
            }
        ]))
        .unwrap();
    index
}

fn create_edge_cases_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Words, Criterion::Proximity]);
        })
        .unwrap();

    index.add_documents(documents!([
        {
            // This document will insert "s" in the prefix database
            "id": 0,
            "text": "
            saa sab sac sae saf sag sah sai saj sak sal sam san sao sap saq sar sasa sat sau sav saw sax say saz
            sba sbb sbc sbe sbf sbg sbh sbi sbj sbk sbl sbm sbn sbo sbp sbq sbr sbsb sbt sbu sbv sbw sbx sby sbz
            sca scb scc sce scf scg sch sci scj sck scl scm scn sco scp scq scr scsc sct scu scv scw scx scy scz
            sda sdb sdc sde sdf sdg sdh sdi sdj sdk sdl sdm sdn sdo sdp sdq sdr sdsd sdt sdu sdv sdw sdx sdy sdz
            sea seb sec see sef seg seh sei sej sek sel sem sen seo sep seq ser sese set seu sev sew sex sey sez
            sfa sfb sfc sfe sff sfg sfh sfi sfj sfk sfl sfm sfn sfo sfp sfq sfr sfsf sft sfu sfv sfw sfx sfy sfz
            sga sgb sgc sge sgf sgg sgh sgi sgj sgk sgl sgm sgn sgo sgp sgq sgr sgsg sgt sgu sgv sgw sgx sgy sgz
            ska skb skc ske skf skg skh ski skj skk skl skm skn sko skp skq skr sksk skt sku skv skw skx sky skz
            sla slb slc sle slf slg slh sli slj slk sll slm sln slo slp slq slr slsl slt slu slv slw slx sly slz
            sma smb smc sme smf smg smh smi smj smk sml smm smn smo smp smq smr smsm smt smu smv smw smx smy smz
            sna snb snc sne snf sng snh sni snj snk snl snm snn sno snp snq snr snsn snt snu snv snw snx sny snz
            soa sob soc soe sof sog soh soi soj sok sol som son soo sop soq sor soso sot sou sov sow sox soy soz
            spa spb spc spe spf spg sph spi spj spk spl spm spn spo spp spq spr spsp spt spu spv spw spx spy spz
            sqa sqb sqc sqe sqf sqg sqh sqi sqj sqk sql sqm sqn sqo sqp sqq sqr sqsq sqt squ sqv sqw sqx sqy sqz
            sra srb src sre srf srg srh sri srj srk srl srm srn sro srp srq srr srsr srt sru srv srw srx sry srz
            ssa ssb ssc sse ssf ssg ssh ssi ssj ssk ssl ssm ssn sso ssp ssq ssr ssss sst ssu ssv ssw ssx ssy ssz
            sta stb stc ste stf stg sth sti stj stk stl stm stn sto stp stq str stst stt stu stv stw stx sty stz
            "
        },
        // The next 5 documents lay out a trap with the split word, phrase search, or synonym `sun flower`.
        // If the search query is "sunflower", the split word "Sun Flower" will match some documents.
        // The next 5 documents lay out a trap with the split word, phrase search, or synonym `sun flower`.
        // If the search query is "sunflower", the split word "Sun Flower" will match some documents.
        // If the query is `sunflower wilting`, then we should make sure that
        // the proximity condition `flower wilting: sprx N` also comes with the condition
        // `sun wilting: sprx N+1`, but this is not the exact condition we use for now.
        // We only check that the phrase `sun flower` exists and `flower wilting: sprx N`, which
        // is better than nothing but not the best.
        {
            "id": 1,
            "text": "Sun Flower sounds like the title of a painting, maybe about a plant wilting under the heat."
        },
        {
            "id": 2,
            "text": "Sun Flower sounds like the title of a painting, maybe about a flower wilting under the heat."
        },
        {
            "id": 3,
            // This document matches the query `sunflower wilting`, but the sprximity condition
            // This document matches the query `sunflower wilting`, but the sprximity condition
            // between `sunflower` and `wilting` cannot be through the split-word `Sun Flower`
            // which would reduce to only `flower` and `wilting` being in sprximity.
            "text": "A flower wilting under the sun, unlike a sunflower"
        },
        {
            // This should be the best document for `sunflower wilting`
            "id": 4,
            "text": "sun flower wilting under the heat"
        },
        {
            // This is also the best document for `sunflower wilting`
            "id": 5,
            "text": "sunflower wilting under the heat"
        },
        {
            // Prox MAX between `best` and `s` prefix
            "id": 6,
            "text": "this is the best meal I have ever had in such a beautiful summer day"
        },
        {
            // Prox 5 between `best` and `s` prefix
            "id": 7,
            "text": "this is the best cooked meal of the summer"
        },
        {
            // Prox 4 between `best` and `s` prefix
            "id": 8,
            "text": "this is the best meal of the summer"
        },
        {
            // Prox 3 between `best` and `s` prefix
            "id": 9,
            "text": "this is the best meal of summer"
        },
        {
            // Prox 1 between `best` and `s` prefix
            "id": 10,
            "text": "this is the best summer meal"
        },
        {
            // Reverse Prox 3 between `best` and `s` prefix
            "id": 11,
            "text": "summer x y best"
        },
        {
            // Reverse Prox 2 between `best` and `s` prefix
            "id": 12,
            "text": "summer x best"
        },
        {
            // Reverse Prox 1 between `best` and `s` prefix
            "id": 13,
            "text": "summer best"
        },
        {
            // This document will insert "win" in the prefix database
            "id": 14,
            "text": "
            winaa winab winac winae winaf winag winah winai winaj winak winal winam winan winao winap winaq winar winasa winat winau winav winaw winax winay winaz
            winba winbb winbc winbe winbf winbg winbh winbi winbj winbk winbl winbm winbn winbo winbp winbq winbr winbsb winbt winbu winbv winbw winbx winby winbz
            winca wincb wincc wince wincf wincg winch winci wincj winck wincl wincm wincn winco wincp wincq wincr wincsc winct wincu wincv wincw wincx wincy wincz
            winda windb windc winde windf windg windh windi windj windk windl windm windn windo windp windq windr windsd windt windu windv windw windx windy windz
            winea wineb winec winee winef wineg wineh winei winej winek winel winem winen wineo winep wineq winer winese winet wineu winev winew winex winey winez
            winfa winfb winfc winfe winff winfg winfh winfi winfj winfk winfl winfm winfn winfo winfp winfq winfr winfsf winft winfu winfv winfw winfx winfy winfz
            winga wingb wingc winge wingf wingg wingh wingi wingj wingk wingl wingm wingn wingo wingp wingq wingr wingsg wingt wingu wingv wingw wingx wingy wingz
            winka winkb winkc winke winkf winkg winkh winki winkj winkk winkl winkm winkn winko winkp winkq winkr winksk winkt winku winkv winkw winkx winky winkz
            winla winlb winlc winle winlf winlg winlh winli winlj winlk winll winlm winln winlo winlp winlq winlr winlsl winlt winlu winlv winlw winlx winly winlz
            winma winmb winmc winme winmf winmg winmh winmi winmj winmk winml winmm winmn winmo winmp winmq winmr winmsm winmt winmu winmv winmw winmx winmy winmz
            winna winnb winnc winne winnf winng winnh winni winnj winnk winnl winnm winnn winno winnp winnq winnr winnsn winnt winnu winnv winnw winnx winny winnz
            winoa winob winoc winoe winof winog winoh winoi winoj winok winol winom winon winoo winop winoq winor winoso winot winou winov winow winox winoy winoz
            winpa winpb winpc winpe winpf winpg winph winpi winpj winpk winpl winpm winpn winpo winpp winpq winpr winpsp winpt winpu winpv winpw winpx winpy winpz
            winqa winqb winqc winqe winqf winqg winqh winqi winqj winqk winql winqm winqn winqo winqp winqq winqr winqsq winqt winqu winqv winqw winqx winqy winqz
            winra winrb winrc winre winrf winrg winrh winri winrj winrk winrl winrm winrn winro winrp winrq winrr winrsr winrt winru winrv winrw winrx winry winrz
            winsa winsb winsc winse winsf winsg winsh winsi winsj winsk winsl winsm winsn winso winsp winsq winsr winsss winst winsu winsv winsw winsx winsy winsz
            winta wintb wintc winte wintf wintg winth winti wintj wintk wintl wintm wintn winto wintp wintq wintr wintst wintt wintu wintv wintw wintx winty wintz
            "
        },
        {
            // Prox MAX between `best` and `win` prefix
            "id": 15,
            "text": "this is the best meal I have ever had in such a beautiful winter day"
        },
        {
            // Prox 5 between `best` and `win` prefix
            "id": 16,
            "text": "this is the best cooked meal of the winter"
        },
        {
            // Prox 4 between `best` and `win` prefix
            "id": 17,
            "text": "this is the best meal of the winter"
        },
        {
            // Prox 3 between `best` and `win` prefix
            "id": 18,
            "text": "this is the best meal of winter"
        },
        {
            // Prox 1 between `best` and `win` prefix
            "id": 19,
            "text": "this is the best winter meal"
        },
        {
            // Reverse Prox 3 between `best` and `win` prefix
            "id": 20,
            "text": "winter x y best"
        },
        {
            // Reverse Prox 2 between `best` and `win` prefix
            "id": 21,
            "text": "winter x best"
        },
        {
            // Reverse Prox 1 between `best` and `win` prefix
            "id": 22,
            "text": "winter best"
        },
    ])).unwrap();
    index
}

#[test]
fn test_proximity_simple() {
    let index = create_simple_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[9, 10, 4, 7, 6, 2, 3, 5, 1, 0]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quack brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quickbrown fox jumps over the lazy dog\"",
        "\"the really quick brown fox jumps over the lazy dog\"",
        "\"the really quick brown fox jumps over the very lazy dog\"",
        "\"the quick brown fox jumps over the lazy. dog\"",
        "\"dog the quick brown fox jumps over the lazy\"",
        "\"brown quick fox jumps over the lazy dog\"",
        "\"the. quick brown fox jumps over the lazy. dog\"",
        "\"the very quick dark brown and smart fox did jump over the terribly lazy and small dog\"",
    ]
    "###);
}

#[test]
fn test_proximity_split_word() {
    let index = create_edge_cases_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("sunflower wilting");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 4, 5, 1, 3]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // "2" and "4" should be swapped ideally
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"Sun Flower sounds like the title of a painting, maybe about a flower wilting under the heat.\"",
        "\"sun flower wilting under the heat\"",
        "\"sunflower wilting under the heat\"",
        "\"Sun Flower sounds like the title of a painting, maybe about a plant wilting under the heat.\"",
        "\"A flower wilting under the sun, unlike a sunflower\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("\"sun flower\" wilting");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 4, 1]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // "2" and "4" should be swapped ideally
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"Sun Flower sounds like the title of a painting, maybe about a flower wilting under the heat.\"",
        "\"sun flower wilting under the heat\"",
        "\"Sun Flower sounds like the title of a painting, maybe about a plant wilting under the heat.\"",
    ]
    "###);
    drop(txn);

    index
        .update_settings(|s| {
            let mut syns = BTreeMap::new();
            syns.insert("xyz".to_owned(), vec!["sun flower".to_owned()]);
            s.set_synonyms(syns);
        })
        .unwrap();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("xyz wilting");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 4, 1]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // "2" and "4" should be swapped ideally
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"Sun Flower sounds like the title of a painting, maybe about a flower wilting under the heat.\"",
        "\"sun flower wilting under the heat\"",
        "\"Sun Flower sounds like the title of a painting, maybe about a plant wilting under the heat.\"",
    ]
    "###);
}

#[test]
fn test_proximity_prefix_db() {
    let index = create_edge_cases_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("best s");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[10, 9, 6, 7, 8, 11, 12, 13, 15]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    // This test illustrates the loss of precision from using the prefix DB
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"this is the best summer meal\"",
        "\"this is the best meal of summer\"",
        "\"this is the best meal I have ever had in such a beautiful summer day\"",
        "\"this is the best cooked meal of the summer\"",
        "\"this is the best meal of the summer\"",
        "\"summer x y best\"",
        "\"summer x best\"",
        "\"summer best\"",
        "\"this is the best meal I have ever had in such a beautiful winter day\"",
    ]
    "###);

    // Difference when using the `su` prefix, which is not in the prefix DB
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("best su");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[10, 13, 9, 12, 6, 7, 8, 11, 15]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"this is the best summer meal\"",
        "\"summer best\"",
        "\"this is the best meal of summer\"",
        "\"summer x best\"",
        "\"this is the best meal I have ever had in such a beautiful summer day\"",
        "\"this is the best cooked meal of the summer\"",
        "\"this is the best meal of the summer\"",
        "\"summer x y best\"",
        "\"this is the best meal I have ever had in such a beautiful winter day\"",
    ]
    "###);

    // Note that there is a case where a prefix is in the prefix DB but not in the
    // **proximity** prefix DB. In that case, its sprximity score will always be
    // the maximum. This happens for prefixes that are larger than 2 bytes.

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("best win");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[19, 18, 15, 16, 17, 20, 21, 22]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"this is the best winter meal\"",
        "\"this is the best meal of winter\"",
        "\"this is the best meal I have ever had in such a beautiful winter day\"",
        "\"this is the best cooked meal of the winter\"",
        "\"this is the best meal of the winter\"",
        "\"winter x y best\"",
        "\"winter x best\"",
        "\"winter best\"",
    ]
    "###);

    // Now using `wint`, which is not in the prefix DB:

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("best wint");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[19, 22, 18, 21, 15, 16, 17, 20]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"this is the best winter meal\"",
        "\"winter best\"",
        "\"this is the best meal of winter\"",
        "\"winter x best\"",
        "\"this is the best meal I have ever had in such a beautiful winter day\"",
        "\"this is the best cooked meal of the winter\"",
        "\"this is the best meal of the winter\"",
        "\"winter x y best\"",
    ]
    "###);

    // and using `wi` which is in the prefix DB and proximity prefix DB

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("best wi");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[19, 18, 15, 16, 17, 20, 21, 22]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"this is the best winter meal\"",
        "\"this is the best meal of winter\"",
        "\"this is the best meal I have ever had in such a beautiful winter day\"",
        "\"this is the best cooked meal of the winter\"",
        "\"this is the best meal of the winter\"",
        "\"winter x y best\"",
        "\"winter x best\"",
        "\"winter best\"",
    ]
    "###);
}
