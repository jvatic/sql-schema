use std::fmt;

use bon::bon;
use diff::Diff;
use migration::Migrate;
use sqlparser::{
    ast::Statement,
    dialect::{self},
    parser::{self, Parser},
};

mod diff;
mod migration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(parser::ParserError);

impl From<parser::ParserError> for ParseError {
    fn from(value: parser::ParserError) -> Self {
        Self(value)
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum), clap(rename_all = "lower"))]
#[non_exhaustive]
pub enum Dialect {
    Ansi,
    BigQuery,
    ClickHouse,
    Databricks,
    DuckDb,
    #[default]
    Generic,
    Hive,
    MsSql,
    MySql,
    PostgreSql,
    RedshiftSql,
    Snowflake,
    SQLite,
}

impl Dialect {
    fn to_sqlparser_dialect(self) -> Box<dyn dialect::Dialect> {
        match self {
            Self::Ansi => Box::new(dialect::AnsiDialect {}),
            Self::BigQuery => Box::new(dialect::BigQueryDialect {}),
            Self::ClickHouse => Box::new(dialect::ClickHouseDialect {}),
            Self::Databricks => Box::new(dialect::DatabricksDialect {}),
            Self::DuckDb => Box::new(dialect::DuckDbDialect {}),
            Self::Generic => Box::new(dialect::GenericDialect {}),
            Self::Hive => Box::new(dialect::HiveDialect {}),
            Self::MsSql => Box::new(dialect::MsSqlDialect {}),
            Self::MySql => Box::new(dialect::MySqlDialect {}),
            Self::PostgreSql => Box::new(dialect::PostgreSqlDialect {}),
            Self::RedshiftSql => Box::new(dialect::RedshiftSqlDialect {}),
            Self::Snowflake => Box::new(dialect::SnowflakeDialect {}),
            Self::SQLite => Box::new(dialect::SQLiteDialect {}),
        }
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            format!("{self:?}")
                .to_ascii_lowercase()
                .split('-')
                .collect::<String>()
        )
    }
}

#[derive(Debug)]
pub struct SyntaxTree(Vec<Statement>);

#[bon]
impl SyntaxTree {
    #[builder]
    pub fn new<'a>(dialect: Option<Dialect>, sql: impl Into<&'a str>) -> Result<Self, ParseError> {
        let dialect = dialect.unwrap_or_default().to_sqlparser_dialect();
        let ast = Parser::parse_sql(dialect.as_ref(), sql.into())?;
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
    use super::*;

    #[derive(Debug)]
    struct TestCase {
        dialect: Dialect,
        sql_a: &'static str,
        sql_b: &'static str,
        expect: &'static str,
    }

    fn run_test_case<F>(tc: &TestCase, testfn: F)
    where
        F: Fn(SyntaxTree, SyntaxTree) -> SyntaxTree,
    {
        let ast_a = SyntaxTree::builder()
            .dialect(tc.dialect.clone())
            .sql(tc.sql_a)
            .build()
            .unwrap();
        let ast_b = SyntaxTree::builder()
            .dialect(tc.dialect.clone())
            .sql(tc.sql_b)
            .build()
            .unwrap();
        let actual = testfn(ast_a, ast_b);
        assert_eq!(actual.to_string(), tc.expect, "{tc:?}");
    }

    fn run_test_cases<F>(test_cases: Vec<TestCase>, testfn: F)
    where
        F: Fn(SyntaxTree, SyntaxTree) -> SyntaxTree,
    {
        test_cases
            .into_iter()
            .for_each(|tc| run_test_case(&tc, |ast_a, ast_b| testfn(ast_a, ast_b)));
    }

    #[test]
    fn diff_create_table() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE foo(\
                            id int PRIMARY KEY
                        )",
                sql_b: "CREATE TABLE foo(\
                            id int PRIMARY KEY
                        );\
                        CREATE TABLE bar (id INT PRIMARY KEY);",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);",
            }],
            |ast_a, ast_b| ast_a.diff(&ast_b).unwrap(),
        );
    }

    #[test]
    fn diff_drop_table() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE foo(\
                        id int PRIMARY KEY
                    );\
                    CREATE TABLE bar (id INT PRIMARY KEY);",
                sql_b: "CREATE TABLE foo(\
                        id int PRIMARY KEY
                    )",
                expect: "DROP TABLE bar;",
            }],
            |ast_a, ast_b| ast_a.diff(&ast_b).unwrap(),
        );
    }

    #[test]
    fn diff_add_column() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE foo(\
                        id int PRIMARY KEY
                    )",
                sql_b: "CREATE TABLE foo(\
                        id int PRIMARY KEY,
                        bar text
                    )",
                expect: "ALTER TABLE\n  foo\nADD\n  COLUMN bar TEXT;",
            }],
            |ast_a, ast_b| ast_a.diff(&ast_b).unwrap(),
        );
    }

    #[test]
    fn diff_drop_column() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE foo(\
                        id int PRIMARY KEY,
                        bar text
                    )",
                sql_b: "CREATE TABLE foo(\
                        id int PRIMARY KEY
                    )",
                expect: "ALTER TABLE\n  foo DROP COLUMN bar;",
            }],
            |ast_a, ast_b| ast_a.diff(&ast_b).unwrap(),
        );
    }

    #[test]
    fn apply_create_table() {
        run_test_cases(
            vec![TestCase {
            dialect: Dialect::Generic,
            sql_a: "CREATE TABLE bar (id INT PRIMARY KEY);",
            sql_b: "CREATE TABLE foo (id INT PRIMARY KEY);",
            expect:
                "CREATE TABLE bar (id INT PRIMARY KEY);\n\nCREATE TABLE foo (id INT PRIMARY KEY);",
        }],
            |ast_a, ast_b| ast_a.migrate(&ast_b).unwrap(),
        );
    }

    #[test]
    fn apply_drop_table() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (id INT PRIMARY KEY)",
                sql_b: "DROP TABLE bar; CREATE TABLE foo (id INT PRIMARY KEY)",
                expect: "CREATE TABLE foo (id INT PRIMARY KEY);",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b).unwrap(),
        );
    }

    #[test]
    fn apply_alter_table_add_column() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ADD COLUMN bar TEXT",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY, bar TEXT);",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b).unwrap(),
        );
    }

    #[test]
    fn apply_alter_table_drop_column() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar DROP COLUMN bar",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b).unwrap(),
        );
    }

    #[test]
    fn apply_alter_table_alter_column() {
        run_test_cases(vec![
            TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET NOT NULL",
                expect: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar DROP NOT NULL",
                expect: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (bar TEXT NOT NULL DEFAULT 'foo', id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar DROP DEFAULT",
                expect: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET DATA TYPE INTEGER",
                expect: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Dialect::PostgreSql,
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET DATA TYPE timestamp with time zone\n USING timestamp with time zone 'epoch' + foo_timestamp * interval '1 second'",
                expect: "CREATE TABLE bar (bar TIMESTAMP WITH TIME ZONE, id INT PRIMARY KEY);",
            },
            TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar ADD GENERATED BY DEFAULT AS IDENTITY",
                expect: "CREATE TABLE bar (\n  bar INTEGER GENERATED BY DEFAULT AS IDENTITY,\n  id INT PRIMARY KEY\n);",
            },
            TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar ADD GENERATED ALWAYS AS IDENTITY (START WITH 10)",
                expect: "CREATE TABLE bar (\n  bar INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 10),\n  id INT PRIMARY KEY\n);",
            },
        ], |ast_a, ast_b| ast_a.migrate(&ast_b).unwrap());
    }
}
