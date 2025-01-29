use std::fmt;

use bon::bon;
use diff::Diff;
use sqlparser::{
    ast::Statement,
    dialect::{Dialect, GenericDialect},
    parser::{Parser, ParserError},
};

mod diff;

#[derive(Debug)]
pub struct SyntaxTree(Vec<Statement>);

#[bon]
impl SyntaxTree {
    #[builder]
    pub fn new<'a>(
        dialect: Option<&dyn Dialect>,
        sql: impl Into<&'a str>,
    ) -> Result<Self, ParserError> {
        let generic = GenericDialect {};
        let dialect = dialect.unwrap_or(&generic);
        let ast = Parser::parse_sql(dialect, sql.into())?;
        Ok(Self(ast))
    }
}

impl SyntaxTree {
    pub fn diff(&self, other: &SyntaxTree) -> Option<Self> {
        Diff::diff(&self.0, &other.0).map(Self)
    }
}

impl fmt::Display for SyntaxTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iter = self.0.iter().peekable();
        while let Some(s) = iter.next() {
            write!(f, "{}", s)?;
            if iter.peek().is_some() {
                write!(f, "\n\n")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_create_table() {
        let sql_a = "CREATE TABLE foo(\
                id int PRIMARY KEY
            )";
        let sql_b = "CREATE TABLE foo(\
                id int PRIMARY KEY
            );\
            \
            CREATE TABLE bar (id INT PRIMARY KEY);";
        let sql_diff = "CREATE TABLE bar (id INT PRIMARY KEY)";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_diff = ast_a.diff(&ast_b);

        assert_eq!(ast_diff.unwrap().to_string(), sql_diff);
    }

    #[test]
    fn diff_drop_table() {
        let sql_a = "CREATE TABLE foo(\
                    id int PRIMARY KEY
                );\
                \
                CREATE TABLE bar (id INT PRIMARY KEY);";
        let sql_b = "CREATE TABLE foo(\
                    id int PRIMARY KEY
                )";
        let sql_diff = "DROP TABLE bar";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_diff = ast_a.diff(&ast_b);

        assert_eq!(ast_diff.unwrap().to_string(), sql_diff);
    }

    #[test]
    fn diff_add_column() {
        let sql_a = "CREATE TABLE foo(\
                id int PRIMARY KEY
            )";
        let sql_b = "CREATE TABLE foo(\
                id int PRIMARY KEY,
                bar text
            )";
        let sql_diff = "ALTER TABLE foo ADD COLUMN bar TEXT";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_diff = ast_a.diff(&ast_b);

        assert_eq!(ast_diff.unwrap().to_string(), sql_diff);
    }

    #[test]
    fn diff_drop_column() {
        let sql_a = "CREATE TABLE foo(\
                    id int PRIMARY KEY,
                    bar text
                )";
        let sql_b = "CREATE TABLE foo(\
                    id int PRIMARY KEY
                )";
        let sql_diff = "ALTER TABLE foo DROP COLUMN bar";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_diff = ast_a.diff(&ast_b);

        assert_eq!(ast_diff.unwrap().to_string(), sql_diff);
    }
}
