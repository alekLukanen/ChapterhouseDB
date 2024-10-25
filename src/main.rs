use chapterhouseqe::lexer::builder;

fn main() {
    println!("Hello, world!");
    let query = "
        select * from bike 
        where id = 42 and value > 90.0 and name = '🥵'";

    let tokens = builder::lex(query);

    println!("tokens from lexer: {:?}", tokens);
}
