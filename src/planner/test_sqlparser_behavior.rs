use anyhow::Result;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[test]
fn test_parse_multiple_statements() -> Result<()> {
    let sql = "
    -- read all
    select * from abc;

    -- read by name
    select * from other
        where name='good;';

    select * from another
        where something='touchy';

    ;
    ;
    ";

    let statements = Parser::parse_sql(&GenericDialect {}, sql)?;
    assert_eq!(statements.len(), 3);

    Ok(())
}
