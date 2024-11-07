use super::lex;

fn vecs_equal<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
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
                lex::Token::Star,
                lex::Token::From,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Where,
                lex::Token::Identifier("id".to_string()),
                lex::Token::Equal,
                lex::Token::Number("42".to_string()),
                lex::Token::And,
                lex::Token::Identifier("value".to_string()),
                lex::Token::GreaterThan,
                lex::Token::Number("90.0".to_string()),
                lex::Token::And,
                lex::Token::Identifier("name".to_string()),
                lex::Token::Equal,
                lex::Token::StringToken("🥵".to_string()),
            ],
        },
        TestCase {
            case_name: String::from("sample-2"),
            query: String::from("select * from bike
        where lower(store) = 'bike stuff';"),
            expected_tokens: vec![
                lex::Token::Select,
                lex::Token::Star,
                lex::Token::From,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Where,
                lex::Token::Identifier("lower".to_string()),
                lex::Token::LeftParenthesis,
                lex::Token::Identifier("store".to_string()),
                lex::Token::RightParenthesis,
                lex::Token::Equal,
                lex::Token::StringToken("bike stuff".to_string()),
                lex::Token::Semicolon,
            ],
        },
        TestCase {
            case_name: String::from("sample-3"),
            query: String::from("select id, name, value, payment_per_year from bike where true;"),
            expected_tokens: vec![
                lex::Token::Select,
                lex::Token::Identifier("id".to_string()),
                lex::Token::Comma,
                lex::Token::Identifier("name".to_string()),
                lex::Token::Comma,
                lex::Token::Identifier("value".to_string()),
                lex::Token::Comma,
                lex::Token::Identifier("payment_per_year".to_string()),
                lex::Token::From,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Where,
                lex::Token::True,
                lex::Token::Semicolon,
            ],
        },
        TestCase {
            case_name: String::from("sample-3"),
            query: String::from("select id, name, value, payment_per_year from bike where value >= 2 or value <= 5;"),
            expected_tokens: vec![
                lex::Token::Select,
                lex::Token::Identifier("id".to_string()),
                lex::Token::Comma,
                lex::Token::Identifier("name".to_string()),
                lex::Token::Comma,
                lex::Token::Identifier("value".to_string()),
                lex::Token::Comma,
                lex::Token::Identifier("payment_per_year".to_string()),
                lex::Token::From,
                lex::Token::Identifier("bike".to_string()),
                lex::Token::Where,
                lex::Token::Identifier("value".to_string()),
                lex::Token::GreaterThanEqual,
                lex::Token::Number("2".to_string()),
                lex::Token::Or,
                lex::Token::Identifier("value".to_string()),
                lex::Token::LessThanEqual,
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
