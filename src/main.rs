use chapterhouseqe::lexer::lex;

fn main() {
    println!("Hello, world!");
    let query = "
        select * from bike 
        where id = 42 and value > 90.0 and name = '🥵';";

    let tokens = lex::lex(query.to_string());

    println!("tokens from lexer: {:?}", tokens);
}
