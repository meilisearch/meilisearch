fn main() {
    let input = std::env::args().nth(1).expect("You must provide a filter to test");

    println!("Trying to execute the following filter:\n{}\n\n", input);

    if let Err(e) = filter_parser::FilterCondition::parse(&input) {
        println!("{}", e.to_string());
    } else {
        println!("✅ Valid filter");
    }
}
