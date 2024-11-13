use crate::lexer::lex::Token;

use super::lex;

fn vecs_equal<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
}

#[test]
fn test_token_types_match() {
    struct TestCase {
        case_name: String,
        t1: Token,
        t2: Token,
        expected_result: bool,
    }

    let test_cases = vec![
        TestCase {
            case_name: String::from("identifiers_equal"),
            t1: Token::Identifier("a1".to_string()),
            t2: Token::Identifier("a2".to_string()),
            expected_result: true,
        },
        TestCase {
            case_name: String::from("periods_equal"),
            t1: Token::Period,
            t2: Token::Period,
            expected_result: true,
        },
        TestCase {
            case_name: String::from("tokens_not_equal"),
            t1: Token::Period,
            t2: Token::Comma,
            expected_result: false,
        },
        TestCase {
            case_name: String::from("tokens_not_equal_id_and_num"),
            t1: Token::Identifier("a".to_string()),
            t2: Token::Number("123".to_string()),
            expected_result: false,
        },
    ];

    for test_case in test_cases {
        println!("running test case: {}", test_case.case_name);
        let result = Token::token_types_match(test_case.t1, test_case.t2);
        assert_eq!(test_case.expected_result, result);
    }
}

#[test]
fn test_lex_with_basic_sql_statements() {
    struct TestCase {
        case_name: String,
        query: String,
        expected_tokens: Vec<lex::Token>,
    }

    let test_cases = vec![
        TestCase {
            case_name: String::from("sample-1"),
            query: String::from("select * from bike
        where id = 42 and value > 90.0 and name = '🥵'"),
            expected_tokens: vec![
                lex::Token::Select,
                lex::Token::Space,
                lex::Token::Star,
                lex::Token::Space,
                lex::Token::From,
                lex::Token::Space,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Space,
                lex::Token::Where,
                lex::Token::Space,
                lex::Token::Identifier("id".to_string()),
                lex::Token::Space,
                lex::Token::Equal,
                lex::Token::Space,
                lex::Token::Number("42".to_string()),
                lex::Token::Space,
                lex::Token::And,
                lex::Token::Space,
                lex::Token::Identifier("value".to_string()),
                lex::Token::Space,
                lex::Token::GreaterThan,
                lex::Token::Space,
                lex::Token::Number("90.0".to_string()),
                lex::Token::Space,
                lex::Token::And,
                lex::Token::Space,
                lex::Token::Identifier("name".to_string()),
                lex::Token::Space,
                lex::Token::Equal,
                lex::Token::Space,
                lex::Token::StringToken("🥵".to_string()),
            ],
        },
        TestCase {
            case_name: String::from("sample-2"),
            query: String::from("select * from bike
        where lower(store) = 'bike stuff';"),
            expected_tokens: vec![
                lex::Token::Select,
                lex::Token::Space,
                lex::Token::Star,
                lex::Token::Space,
                lex::Token::From,
                lex::Token::Space,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Space,
                lex::Token::Where,
                lex::Token::Space,
                lex::Token::Identifier("lower".to_string()),
                lex::Token::LeftParenthesis,
                lex::Token::Identifier("store".to_string()),
                lex::Token::RightParenthesis,
                lex::Token::Space,
                lex::Token::Equal,
                lex::Token::Space,
                lex::Token::StringToken("bike stuff".to_string()),
                lex::Token::Semicolon,
            ],
        },
        TestCase {
            case_name: String::from("sample-3"),
            query: String::from("select id, name, value, payment_per_year from bike where true;"),
            expected_tokens: vec![
                lex::Token::Select,
                lex::Token::Space,
                lex::Token::Identifier("id".to_string()),
                lex::Token::Comma,
                lex::Token::Space,
                lex::Token::Identifier("name".to_string()),
                lex::Token::Comma,
                lex::Token::Space,
                lex::Token::Identifier("value".to_string()),
                lex::Token::Comma,
                lex::Token::Space,
                lex::Token::Identifier("payment_per_year".to_string()),
                lex::Token::Space,
                lex::Token::From,
                lex::Token::Space,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Space,
                lex::Token::Where,
                lex::Token::Space,
                lex::Token::True,
                lex::Token::Semicolon,
            ],
        },
        TestCase {
            case_name: String::from("sample-3"),
            query: String::from("select id, name, value, payment_per_year from bike where value >= 2 or value <= 5;"),
            expected_tokens: vec![
                lex::Token::Select,
                lex::Token::Space,
                lex::Token::Identifier("id".to_string()),
                lex::Token::Comma,
                lex::Token::Space,
                lex::Token::Identifier("name".to_string()),
                lex::Token::Comma,
                lex::Token::Space,
                lex::Token::Identifier("value".to_string()),
                lex::Token::Comma,
                lex::Token::Space,
                lex::Token::Identifier("payment_per_year".to_string()),
                lex::Token::Space,
                lex::Token::From,
                lex::Token::Space,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Space,
                lex::Token::Where,
                lex::Token::Space,
                lex::Token::Identifier("value".to_string()),
                lex::Token::Space,
                lex::Token::GreaterThanEqual,
                lex::Token::Space,
                lex::Token::Number("2".to_string()),
                lex::Token::Space,
                lex::Token::Or,
                lex::Token::Space,
                lex::Token::Identifier("value".to_string()),
                lex::Token::Space,
                lex::Token::LessThanEqual,
                lex::Token::Space,
                lex::Token::Number("5".to_string()),
                lex::Token::Semicolon,
            ],
        },
    ];

    for test_case in test_cases {
        println!("running test case: {}", test_case.case_name);
        let tokens = lex::lex(test_case.query);
        println!("expected: {:?}", test_case.expected_tokens);
        println!("actual: {:?}", tokens);
        assert_eq!(vecs_equal(&tokens, &test_case.expected_tokens), true);
    }
}
