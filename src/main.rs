use core::panic;
use std::str::FromStr;

use chapterhouseqe::lexer::builder;

fn main() {
    println!("Hello, world!");
    let query = "
        select * from bike 
        where id = 42 and value > 90.0";

    let query_val = match String::from_str(query) {
        Ok(v) => v,
        Err(_) => panic!("invalid string"),
    };

    let tokens = builder::lex(query_val);

    println!("tokens from lexer: {:?}", tokens);
}
