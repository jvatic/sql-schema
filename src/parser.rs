use thiserror::Error;

use crate::{ast, dialect, sealed::Sealed};

#[derive(Error, Debug)]
#[error("Oops, we couldn't parse that!")]
pub struct ParseError(#[from] sqlparser::parser::ParserError);

pub trait Parse: Sealed {
    fn parse_sql<'a, Dialect>(
        &self,
        sql: impl Into<&'a str>,
    ) -> Result<Vec<ast::Statement>, ParseError>;
}

fn parse_sql<'a>(
    dialect: Box<dyn sqlparser::dialect::Dialect>,
    sql: impl Into<&'a str>,
) -> Result<Vec<ast::Statement>, ParseError> {
    let tree = sqlparser::parser::Parser::parse_sql(dialect.as_ref(), sql.into())?;
    Ok(tree)
}

impl Parse for dialect::Generic {
    fn parse_sql<'a, Dialect>(
        &self,
        sql: impl Into<&'a str>,
    ) -> Result<Vec<ast::Statement>, ParseError> {
        parse_sql(Box::new(sqlparser::dialect::GenericDialect {}), sql)
    }
}

impl Parse for dialect::PostgreSQL {
    fn parse_sql<'a, Dialect>(
        &self,
        sql: impl Into<&'a str>,
    ) -> Result<Vec<ast::Statement>, ParseError> {
        parse_sql(Box::new(sqlparser::dialect::PostgreSqlDialect {}), sql)
    }
}

impl Parse for dialect::SQLite {
    fn parse_sql<'a, Dialect>(
        &self,
        sql: impl Into<&'a str>,
    ) -> Result<Vec<ast::Statement>, ParseError> {
        parse_sql(Box::new(sqlparser::dialect::SQLiteDialect {}), sql)
    }
}
