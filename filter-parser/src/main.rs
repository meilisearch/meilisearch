fn main() {
    let input = std::env::args().nth(1).expect("You must provide a filter to test");

    println!("Trying to execute the following filter:\n{}\n", input);

    match filter_parser::FilterCondition::parse(&input) {
        Ok(filter) => {
            println!("✅ Valid filter");
            println!("{:#?}", filter);
        }
        Err(e) => {
            println!("❎ Invalid filter");
            println!("{}", e);
        }
    }
}
