use chapterhouseqe::lexer::lex;
use chapterhouseqe::parser::parser;

fn main() {
    let query = "
        select * from bike 
        where id = 42 and value > 90.0 and name = '🥵';";
    let tokens = lex::lex(query.to_string());
    println!("tokens from lexer: {:?}", tokens);

    let query2 = "select * from items.bike;";
    let mut parsi = parser::Parser::new(query2.to_string(), true);
    match parsi.parse() {
        Ok(syntax_tree) => {
            println!("syntax tree:");
            println!("{:?}", syntax_tree);
        }
        Err(err) => {
            parsi.log_debug();
            println!("error: {}", err);
        }
    }
}
