use crate::routes::indexes::search::fix_sort_query_parameters;

#[test]
fn test_fix_sort_query_parameters() {
    let sort = fix_sort_query_parameters("_geoPoint(12, 13):asc");
    assert_eq!(sort, vec!["_geoPoint(12,13):asc".to_string()]);
    let sort = fix_sort_query_parameters("doggo:asc,_geoPoint(12.45,13.56):desc");
    assert_eq!(sort, vec!["doggo:asc".to_string(), "_geoPoint(12.45,13.56):desc".to_string(),]);
    let sort =
        fix_sort_query_parameters("doggo:asc , _geoPoint(12.45, 13.56, 2590352):desc , catto:desc");
    assert_eq!(
        sort,
        vec![
            "doggo:asc".to_string(),
            "_geoPoint(12.45,13.56,2590352):desc".to_string(),
            "catto:desc".to_string(),
        ]
    );
    let sort = fix_sort_query_parameters("doggo:asc , _geoPoint(1, 2), catto:desc");
    // This is ugly but eh, I don't want to write a full parser just for this unused route
    assert_eq!(sort, vec!["doggo:asc".to_string(), "_geoPoint(1,2),catto:desc".to_string(),]);
}
