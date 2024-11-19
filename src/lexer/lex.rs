use anyhow::{Context, Result};
use std::vec::Vec;
use thiserror::Error;
use unicode_segmentation::UnicodeSegmentation;

#[derive(Error, Debug)]
pub enum TokenizationError {
    #[error("invalid token type: {0}")]
    TypeNotFound(String),
}

trait Tokenizer {
    fn add_next_character(&mut self, c: &str) -> (bool, bool);
    fn to_token(&self) -> Token;
}

struct StaticToken {
    token: Token,
    text: String,
}

impl QuoteType {
    fn to_string(&self) -> String {
        match &self {
            QuoteType::Single => "'".to_string(),
            QuoteType::Double => "\"".to_string(),
            QuoteType::Backtick => "`".to_string(),
        }
    }
}

impl TryFrom<&str> for QuoteType {
    type Error = TokenizationError;

    fn try_from(s: &str) -> Result<QuoteType, Self::Error> {
        match s {
            "'" => Ok(QuoteType::Single),
            "\"" => Ok(QuoteType::Double),
            "`" => Ok(QuoteType::Backtick),
            _ => Err(TokenizationError::TypeNotFound(s.to_string())),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
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
    As,
    Null,
    Order,
    By,
    Asc,
    Desc,
    In,
    True,
    False,
    // symbols
    Star,
    Comma,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    Equal,
    NotEqual,
    LeftParenthesis,
    RightParenthesis,
    Semicolon,
    Period,
    Space,
    Plus,
    Minus,
    ForwardSlash,
    // data literals
    Number(String),
    StringToken(String),
    // user defined
    Identifier(String),
    // not implemented token
    UndefinedTokenType,
}

impl Token {
    pub fn token_types_match(t1: Token, t2: Token) -> bool {
        match (&t1, &t2) {
            (Token::Number(_), Token::Number(_)) => true,
            (Token::StringToken(_), Token::StringToken(_)) => true,
            (Token::Identifier(_), Token::Identifier(_)) => true,
            _ => &t1 == &t2,
        }
    }
    pub fn is_expression_operator(&self) -> bool {
        match self {
            Token::And => true,
            Token::Or => true,
            Token::Not => true,
            Token::Is => true,
            Token::In => true,
            Token::LessThan => true,
            Token::LessThanEqual => true,
            Token::GreaterThan => true,
            Token::GreaterThanEqual => true,
            Token::Equal => true,
            Token::NotEqual => true,
            Token::Plus => true,
            Token::Minus => true,
            Token::Star => true,
            Token::ForwardSlash => true,
            _ => false,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum QuoteType {
    Single,
    Double,
    Backtick,
}

struct QuotedTokenizer {
    quote_type: QuoteType,
    text: Vec<String>,
}

impl Tokenizer for QuotedTokenizer {
    fn add_next_character(&mut self, c: &str) -> (bool, bool) {
        if let Some(last) = self.text.last() {
            if last == "\\" && self.quote_type.to_string() == c {
                self.text.push(c.to_string());
                return (false, true);
            }
        }
        if c == self.quote_type.to_string() {
            (true, true)
        } else {
            self.text.push(c.to_string());
            (false, true)
        }
    }
    fn to_token(&self) -> Token {
        match self.quote_type {
            QuoteType::Single => Token::StringToken(self.text.concat()),
            QuoteType::Double => Token::Identifier(self.text.concat()),
            QuoteType::Backtick => Token::Identifier(self.text.concat()),
        }
    }
}

impl QuotedTokenizer {
    fn new(c: &str) -> Result<QuotedTokenizer> {
        let qt = QuoteType::try_from(c).context("failed to create new QuoteToken")?;
        Ok(QuotedTokenizer {
            quote_type: qt,
            text: Vec::new(),
        })
    }

    fn is_valid_starting_character(c: &str) -> bool {
        match QuoteType::try_from(c) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

struct KeywordTokenizer {
    text: Vec<String>,
}

impl Tokenizer for KeywordTokenizer {
    fn add_next_character(&mut self, c: &str) -> (bool, bool) {
        if let Some(v) = c.chars().next() {
            if v.is_ascii() && (v.is_alphabetic() || v.is_numeric() || c == "_") {
                self.text.push(c.to_string());
                (false, true)
            } else {
                (true, false)
            }
        } else {
            (true, false)
        }
    }
    fn to_token(&self) -> Token {
        if let Some(t) = KeywordTokenizer::token_from_string(self.text.concat()) {
            t
        } else {
            Token::Identifier(self.text.concat())
        }
    }
}

impl KeywordTokenizer {
    fn new(c: &str) -> Result<KeywordTokenizer> {
        Ok(KeywordTokenizer {
            text: vec![c.to_string()],
        })
    }

    fn is_valid_starting_character(c: &str) -> bool {
        if let Some(v) = c.chars().next() {
            v.is_ascii() && (v.is_alphabetic() || c == "_")
        } else {
            false
        }
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

    fn keywords() -> Vec<StaticToken> {
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
            StaticToken {
                token: Token::As,
                text: "as".to_string(),
            },
            StaticToken {
                token: Token::Null,
                text: "null".to_string(),
            },
            StaticToken {
                token: Token::Order,
                text: "order".to_string(),
            },
            StaticToken {
                token: Token::By,
                text: "by".to_string(),
            },
            StaticToken {
                token: Token::Asc,
                text: "asc".to_string(),
            },
            StaticToken {
                token: Token::Desc,
                text: "desc".to_string(),
            },
            StaticToken {
                token: Token::In,
                text: "in".to_string(),
            },
            StaticToken {
                token: Token::True,
                text: "true".to_string(),
            },
            StaticToken {
                token: Token::False,
                text: "false".to_string(),
            },
        ];
        keywords
    }

    fn token_from_string(s: String) -> Option<Token> {
        // match keywords
        for keyword in KeywordTokenizer::keywords() {
            if let Some(token) =
                KeywordTokenizer::match_keyword(keyword.token, keyword.text, s.to_string())
            {
                return Some(token);
            }
        }
        None
    }
}

struct SymbolTokenizer {
    text: Vec<String>,
}

impl Tokenizer for SymbolTokenizer {
    fn add_next_character(&mut self, c: &str) -> (bool, bool) {
        if self.text.len() == 1 {
            if let Some(t) = self.text.last() {
                if (t.to_string() == '>'.to_string() || t.to_string() == '<'.to_string())
                    && c.to_string() == "=".to_string()
                {
                    self.text.push(c.to_string());
                    return (true, true);
                }
            }
        } else {
            return (true, false);
        }
        (true, false)
    }
    fn to_token(&self) -> Token {
        if let Some(t) = SymbolTokenizer::token_from_string(self.text.concat()) {
            t
        } else {
            Token::Identifier(self.text.concat())
        }
    }
}

impl SymbolTokenizer {
    fn new(c: &str) -> Result<SymbolTokenizer> {
        Ok(SymbolTokenizer {
            text: vec![c.to_string()],
        })
    }

    fn is_valid_starting_character(c: &str) -> bool {
        if let Some(v) = c.chars().next() {
            for symbol in SymbolTokenizer::symbols() {
                if symbol.text == v.to_string() {
                    return true;
                }
            }
        }
        false
    }

    fn match_symbol(token: Token, s1: String, s2: String) -> Option<Token> {
        if s1 == s2 {
            Some(token)
        } else {
            None
        }
    }

    fn symbols() -> Vec<StaticToken> {
        let keywords: Vec<StaticToken> = vec![
            StaticToken {
                token: Token::Star,
                text: "*".to_string(),
            },
            StaticToken {
                token: Token::Comma,
                text: ",".to_string(),
            },
            StaticToken {
                token: Token::LessThan,
                text: "<".to_string(),
            },
            StaticToken {
                token: Token::LessThanEqual,
                text: "<=".to_string(),
            },
            StaticToken {
                token: Token::GreaterThan,
                text: ">".to_string(),
            },
            StaticToken {
                token: Token::GreaterThanEqual,
                text: ">=".to_string(),
            },
            StaticToken {
                token: Token::Equal,
                text: "=".to_string(),
            },
            StaticToken {
                token: Token::NotEqual,
                text: "!=".to_string(),
            },
            StaticToken {
                token: Token::LeftParenthesis,
                text: "(".to_string(),
            },
            StaticToken {
                token: Token::RightParenthesis,
                text: ")".to_string(),
            },
            StaticToken {
                token: Token::Semicolon,
                text: ";".to_string(),
            },
            StaticToken {
                token: Token::Period,
                text: ".".to_string(),
            },
            StaticToken {
                token: Token::Plus,
                text: "+".to_string(),
            },
            StaticToken {
                token: Token::Minus,
                text: "-".to_string(),
            },
            StaticToken {
                token: Token::ForwardSlash,
                text: "/".to_string(),
            },
        ];
        keywords
    }

    fn token_from_string(s: String) -> Option<Token> {
        // match keywords
        for keyword in SymbolTokenizer::symbols() {
            if let Some(token) =
                SymbolTokenizer::match_symbol(keyword.token, keyword.text, s.to_string())
            {
                return Some(token);
            }
        }
        None
    }
}

struct NumberTokenizer {
    text: Vec<String>,
}

impl Tokenizer for NumberTokenizer {
    fn add_next_character(&mut self, c: &str) -> (bool, bool) {
        if let Some(t) = c.chars().next() {
            if t.is_ascii() && (t.is_ascii_digit() || c == ".") {
                self.text.push(c.to_string());
                (false, true)
            } else {
                (true, false)
            }
        } else {
            (false, true)
        }
    }
    fn to_token(&self) -> Token {
        Token::Number(self.text.concat())
    }
}

impl NumberTokenizer {
    fn new(c: &str) -> Result<NumberTokenizer> {
        Ok(NumberTokenizer {
            text: vec![c.to_string()],
        })
    }

    fn is_valid_starting_character(c: &str) -> bool {
        if let Some(t) = c.chars().next() {
            t.is_ascii() && (t.is_ascii_digit() || c == ".")
        } else {
            false
        }
    }
}

struct SpaceTokenizer {}

impl Tokenizer for SpaceTokenizer {
    fn add_next_character(&mut self, c: &str) -> (bool, bool) {
        if let Some(t) = c.chars().next() {
            if t.is_whitespace() {
                (false, true)
            } else {
                (true, false)
            }
        } else {
            (false, true)
        }
    }
    fn to_token(&self) -> Token {
        Token::Space
    }
}

impl SpaceTokenizer {
    fn new(_: &str) -> Result<SpaceTokenizer> {
        Ok(SpaceTokenizer {})
    }

    fn is_valid_starting_character(c: &str) -> bool {
        if let Some(t) = c.chars().next() {
            t.is_whitespace()
        } else {
            false
        }
    }
}

pub fn lex(query: String) -> Vec<Token> {
    let mut tokens: Vec<Token> = Vec::new();

    let mut tokenizer: Option<Box<dyn Tokenizer>> = None;
    for symbol in query.as_str().graphemes(true) {
        if let Some(ref mut t) = tokenizer {
            let (done, consumed) = t.add_next_character(symbol);
            if done {
                tokens.push(t.to_token());
                tokenizer = None;
                if consumed {
                    continue;
                }
            } else {
                continue;
            }
        }

        if QuotedTokenizer::is_valid_starting_character(symbol) {
            if let Ok(t) = QuotedTokenizer::new(symbol) {
                tokenizer = Some(Box::new(t));
            }
        } else if KeywordTokenizer::is_valid_starting_character(symbol) {
            if let Ok(t) = KeywordTokenizer::new(symbol) {
                tokenizer = Some(Box::new(t));
            }
        } else if SymbolTokenizer::is_valid_starting_character(symbol) {
            if let Ok(t) = SymbolTokenizer::new(symbol) {
                tokenizer = Some(Box::new(t));
            }
        } else if NumberTokenizer::is_valid_starting_character(symbol) {
            if let Ok(t) = NumberTokenizer::new(symbol) {
                tokenizer = Some(Box::new(t));
            }
        } else if SpaceTokenizer::is_valid_starting_character(symbol) {
            if let Ok(t) = SpaceTokenizer::new(symbol) {
                tokenizer = Some(Box::new(t));
            }
        } else {
            tokens.push(Token::UndefinedTokenType);
        }
    }

    // the last token might not be finished so we need to create the
    // token from the last tokenizer if it's some value.
    if let Some(ref mut t) = tokenizer {
        tokens.push(t.to_token());
    }

    tokens
}
