use std::vec::Vec;

struct StaticToken {
    token: Token,
    text: String,
}

pub enum Token {
    // keywords
    Select,
    Where,
    From,
    And,
    Or,
    Not,
    Limit,
    Is,
    Null,
    Order,
    By,
    Asc,
    Desc,
    In,
    // symbols
    Star,
    Comma,
    WhiteSpace,
    LessThan,
    GreaterThan,
    Equal,
    Exlamation,
    LeftParenthesis,
    RightParenthesis,
    // data literals
    Number,
    String,
    // user defined
    Identifier,
}

fn match_keyword(token: Token, ms: String, ss: String) -> Option<Token> {
    if ss.to_lowercase() == ss || ss.to_uppercase() == ss {
        if ms.to_lowercase() == ss.to_lowercase() {
            Some(token)
        } else {
            None
        }
    } else {
        None
    }
}

fn token_from_string(s: String) -> Token {
    // match keywords
    let keywords: Vec<StaticToken> = vec![
        StaticToken {
            token: Token::Select,
            text: "select".to_string(),
        },
        StaticToken {
            token: Token::Where,
            text: "where".to_string(),
        },
        StaticToken {
            token: Token::From,
            text: "from".to_string(),
        },
        StaticToken {
            token: Token::And,
            text: "and".to_string(),
        },
        StaticToken {
            token: Token::Or,
            text: "or".to_string(),
        },
        StaticToken {
            token: Token::Not,
            text: "not".to_string(),
        },
        StaticToken {
            token: Token::Limit,
            text: "limit".to_string(),
        },
        StaticToken {
            token: Token::Is,
            text: "is".to_string(),
        },
    ];
}

pub fn lex(query: String) -> Vec<Token> {
    println!("--- lex --- ");
    let mut tokens: Vec<Token> = Vec::new();

    for symbol in query.split(" ") {}

    tokens
}
