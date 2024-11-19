use core::sync;

use chapterhouseqe::ast::ast;
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
            println!("error: {:?}", err);
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
            println!("error: {:?}", err);
        }
    }

    println!("----------------------");

    let query3 = "select * from bike where a + 1 = 2;";
    println!("query3: {}", query3);
    let mut parsi3 = parser::Parser::new(query3.to_string(), true);
    match parsi3.parse() {
        Ok(syntax_tree) => {
            println!("syntax tree:");
            println!("{:?}", syntax_tree);
        }
        Err(err) => {
            parsi3.log_debug();
            println!("error: {:?}", err);
        }
    }

    println!("----------------------");

    let query4 = "select * from bike where 1+2*3+4*4+1 = 2;";
    println!("query4: {}", query3);
    let mut parsi4 = parser::Parser::new(query4.to_string(), true);
    match parsi4.parse() {
        Ok(syntax_tree) => {
            println!("syntax tree:");
            println!("{:?}", syntax_tree);
            match syntax_tree {
                ast::Statement::Select(select) => {
                    print_where_expression_tree(select.where_expression)
                }
                _ => println!("couldn't print tree"),
            }
        }
        Err(err) => {
            parsi4.log_debug();
            println!("error: {:?}", err);
        }
    }
}

fn print_where_expression_tree(expression: Option<ast::Term>) {
    if let Some(tree) = expression {
        let json = serde_json::to_string_pretty(&tree).unwrap();
        println!("{}", json);
    }
}
