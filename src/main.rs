use chapterhouseqe::lexer::lex;
use chapterhouseqe::parser::parser;

fn main() {
    let query = "
        select * from bike 
        where id = 42 and value > 90.0 and name = '🥵';";
    let tokens = lex::lex(query.to_string());
    println!("tokens from lexer: {:?}", tokens);

    println!("example query1");
    let query1 = "select * from items.bike;";
    println!("query1: {}", query1);
    let mut parsi1 = parser::Parser::new(query1.to_string(), true);
    match parsi1.parse() {
        Ok(syntax_tree) => {
            println!("syntax tree:");
            println!("{:?}", syntax_tree);
        }
        Err(err) => {
            parsi1.log_debug();
            println!("error: {}", err);
        }
    }

    println!("----------------------");

    println!("example query2");
    let query2 = "select * from (select * from bike) as bike_select;";
    println!("query2: {}", query2);
    let mut parsi2 = parser::Parser::new(query2.to_string(), true);
    match parsi2.parse() {
        Ok(syntax_tree) => {
            println!("syntax tree:");
            println!("{:?}", syntax_tree);
        }
        Err(err) => {
            parsi2.log_debug();
            println!("error: {}", err);
        }
    }
}
