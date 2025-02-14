use std::fmt;

use bon::bon;
use diff::Diff;
use migration::Migrate;
use sqlparser::{
    ast::Statement,
    dialect::{Dialect, GenericDialect},
    parser::{Parser, ParserError},
};

mod diff;
mod migration;

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

    pub fn empty() -> Self {
        Self(vec![])
    }
}

impl SyntaxTree {
    pub fn diff(&self, other: &SyntaxTree) -> Option<Self> {
        Diff::diff(&self.0, &other.0).map(Self)
    }

    pub fn migrate(self, other: &SyntaxTree) -> Option<Self> {
        Migrate::migrate(self.0, &other.0).map(Self)
    }
}

impl fmt::Display for SyntaxTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iter = self.0.iter().peekable();
        while let Some(s) = iter.next() {
            let formatted = sqlformat::format(
                format!("{s};").as_str(),
                &sqlformat::QueryParams::None,
                &sqlformat::FormatOptions::default(),
            );
            write!(f, "{formatted}")?;
            if iter.peek().is_some() {
                write!(f, "\n\n")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::PostgreSqlDialect;

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
        let sql_diff = "CREATE TABLE bar (id INT PRIMARY KEY);";

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
        let sql_diff = "DROP TABLE bar;";

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
        let sql_diff = "ALTER TABLE\n  foo\nADD\n  COLUMN bar TEXT;";

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
        let sql_diff = "ALTER TABLE\n  foo DROP COLUMN bar;";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_diff = ast_a.diff(&ast_b);

        assert_eq!(ast_diff.unwrap().to_string(), sql_diff);
    }

    #[test]
    fn apply_create_table() {
        let sql_a = "CREATE TABLE bar (id INT PRIMARY KEY);";
        let sql_b = "CREATE TABLE foo (id INT PRIMARY KEY);";
        let sql_res = sql_a.to_owned() + "\n\n" + sql_b;

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_res = ast_a.migrate(&ast_b);

        assert_eq!(ast_res.unwrap().to_string(), sql_res);
    }

    #[test]
    fn apply_drop_table() {
        let sql_a = "CREATE TABLE bar (id INT PRIMARY KEY)";
        let sql_b = "DROP TABLE bar; CREATE TABLE foo (id INT PRIMARY KEY)";
        let sql_res = "CREATE TABLE foo (id INT PRIMARY KEY);";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_res = ast_a.migrate(&ast_b);

        assert_eq!(ast_res.unwrap().to_string(), sql_res);
    }

    #[test]
    fn apply_alter_table_add_column() {
        let sql_a = "CREATE TABLE bar (id INT PRIMARY KEY)";
        let sql_b = "ALTER TABLE bar ADD COLUMN bar TEXT";
        let sql_res = "CREATE TABLE bar (id INT PRIMARY KEY, bar TEXT);";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_res = ast_a.migrate(&ast_b);

        assert_eq!(ast_res.unwrap().to_string(), sql_res);
    }

    #[test]
    fn apply_alter_table_drop_column() {
        let sql_a = "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)";
        let sql_b = "ALTER TABLE bar DROP COLUMN bar";
        let sql_res = "CREATE TABLE bar (id INT PRIMARY KEY);";

        let ast_a = SyntaxTree::builder().sql(sql_a).build().unwrap();
        let ast_b = SyntaxTree::builder().sql(sql_b).build().unwrap();
        let ast_res = ast_a.migrate(&ast_b);

        assert_eq!(ast_res.unwrap().to_string(), sql_res);
    }

    #[test]
    fn apply_alter_table_alter_column() {
        #[derive(Debug)]
        struct TestCase {
            dialect: Box<dyn Dialect>,
            sql_a: &'static str,
            sql_b: &'static str,
            expect: &'static str,
        }
        let test_cases = vec![
            TestCase {
                dialect: Box::new(GenericDialect {}),
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET NOT NULL",
                expect: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Box::new(GenericDialect {}),
                sql_a: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar DROP NOT NULL",
                expect: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Box::new(GenericDialect {}),
                sql_a: "CREATE TABLE bar (bar TEXT NOT NULL DEFAULT 'foo', id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar DROP DEFAULT",
                expect: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Box::new(GenericDialect {}),
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET DATA TYPE INTEGER",
                expect: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Box::new(PostgreSqlDialect {}),
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET DATA TYPE timestamp with time zone\n USING timestamp with time zone 'epoch' + foo_timestamp * interval '1 second'",
                expect: "CREATE TABLE bar (bar TIMESTAMP WITH TIME ZONE, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Box::new(GenericDialect {}),
                sql_a: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar ADD GENERATED BY DEFAULT AS IDENTITY",
                expect: "CREATE TABLE bar (\n  bar INTEGER GENERATED BY DEFAULT AS IDENTITY,\n  id INT PRIMARY KEY\n);",
            },
            TestCase {
                dialect: Box::new(GenericDialect {}),
                sql_a: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar ADD GENERATED ALWAYS AS IDENTITY (START WITH 10)",
                expect: "CREATE TABLE bar (\n  bar INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 10),\n  id INT PRIMARY KEY\n);",
            },
        ];

        test_cases.into_iter().for_each(|tc| {
            let ast_a = SyntaxTree::builder()
                .dialect(tc.dialect.as_ref())
                .sql(tc.sql_a)
                .build()
                .unwrap();
            let ast_b = SyntaxTree::builder()
                .dialect(tc.dialect.as_ref())
                .sql(tc.sql_b)
                .build()
                .unwrap();
            let ast_res = ast_a.migrate(&ast_b);

            assert_eq!(ast_res.unwrap().to_string(), tc.expect, "{tc:?}");
        });
    }
}
