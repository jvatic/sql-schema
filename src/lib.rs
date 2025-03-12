use std::fmt;

use bon::bon;
use diff::Diff;
use migration::Migrate;
use sqlparser::{
    ast::Statement,
    dialect::{self},
    parser::{self, Parser},
};
use thiserror::Error;

mod diff;
mod migration;
pub mod path_template;

#[derive(Error, Debug)]
#[error("Oops, we couldn't parse that!")]
pub struct ParseError(#[from] parser::ParserError);

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
        // NOTE: this must match how clap::ValueEnum displays variants
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

#[derive(Debug, Clone)]
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

pub use diff::DiffError;
pub use migration::MigrateError;

impl SyntaxTree {
    pub fn diff(&self, other: &SyntaxTree) -> Result<Option<Self>, DiffError> {
        Ok(Diff::diff(&self.0, &other.0)?.map(Self))
    }

    pub fn migrate(self, other: &SyntaxTree) -> Result<Option<Self>, MigrateError> {
        Ok(Migrate::migrate(self.0, &other.0)?.map(Self))
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
        SyntaxTree::builder()
            .dialect(tc.dialect.clone())
            .sql(tc.expect)
            .build()
            .expect(format!("invalid SQL: {:?}", tc.expect).as_str());
        let actual = testfn(ast_a, ast_b);
        assert_eq!(actual.to_string(), tc.expect, "{tc:?}");
    }

    fn run_test_cases<F, E: fmt::Debug>(test_cases: Vec<TestCase>, testfn: F)
    where
        F: Fn(SyntaxTree, SyntaxTree) -> Result<Option<SyntaxTree>, E>,
    {
        test_cases.into_iter().for_each(|tc| {
            run_test_case(&tc, |ast_a, ast_b| {
                testfn(ast_a, ast_b)
                    .inspect_err(|err| eprintln!("Error: {err:?}"))
                    .unwrap()
                    .unwrap()
            })
        });
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
            |ast_a, ast_b| ast_a.diff(&ast_b),
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
            |ast_a, ast_b| ast_a.diff(&ast_b),
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
            |ast_a, ast_b| ast_a.diff(&ast_b),
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
            |ast_a, ast_b| ast_a.diff(&ast_b),
        );
    }

    #[test]
    fn diff_create_index() {
        run_test_cases(
            vec![
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                    sql_b: "CREATE UNIQUE INDEX title_idx ON films ((lower(title)));",
                    expect: "DROP INDEX title_idx;\n\nCREATE UNIQUE INDEX title_idx ON films((lower(title)));",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE UNIQUE INDEX IF NOT EXISTS title_idx ON films (title);",
                    sql_b: "CREATE UNIQUE INDEX IF NOT EXISTS title_idx ON films ((lower(title)));",
                    expect: "DROP INDEX IF EXISTS title_idx;\n\nCREATE UNIQUE INDEX IF NOT EXISTS title_idx ON films((lower(title)));",
                },
            ],
            |ast_a, ast_b| ast_a.diff(&ast_b),
        );
    }

    #[test]
    fn diff_create_type() {
        run_test_cases(
            vec![
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
                    sql_b: "CREATE TYPE foo AS ENUM ('bar');",
                    expect: "DROP TYPE bug_status;\n\nCREATE TYPE foo AS ENUM ('bar');",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                    sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'assigned', 'closed');",
                    expect: "ALTER TYPE bug_status\nADD\n  VALUE 'assigned'\nAFTER\n  'open';",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                    sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                    expect: "ALTER TYPE bug_status\nADD\n  VALUE 'new' BEFORE 'open';",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
                    sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                    expect: "ALTER TYPE bug_status\nADD\n  VALUE 'closed';",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
                    sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'assigned', 'closed');",
                    expect: "ALTER TYPE bug_status\nADD\n  VALUE 'assigned';\n\nALTER TYPE bug_status\nADD\n  VALUE 'closed';",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'critical');",
                    sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'assigned', 'closed', 'critical');",
                    expect: "ALTER TYPE bug_status\nADD\n  VALUE 'new' BEFORE 'open';\n\nALTER TYPE bug_status\nADD\n  VALUE 'assigned'\nAFTER\n  'open';\n\nALTER TYPE bug_status\nADD\n  VALUE 'closed'\nAFTER\n  'assigned';",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                    sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                    expect: "ALTER TYPE bug_status\nADD\n  VALUE 'new' BEFORE 'open';\n\nALTER TYPE bug_status\nADD\n  VALUE 'closed';",
                },
            ],
            |ast_a, ast_b| ast_a.diff(&ast_b),
        );
    }

    #[test]
    fn diff_create_extension() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE EXTENSION hstore;",
                sql_b: "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
                expect: "DROP EXTENSION hstore;\n\nCREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
            }],
            |ast_a, ast_b| ast_a.diff(&ast_b),
        );
    }

    #[test]
    fn apply_create_table() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TABLE bar (id INT PRIMARY KEY);",
                sql_b: "CREATE TABLE foo (id INT PRIMARY KEY);",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);\n\nCREATE TABLE foo (id INT PRIMARY KEY);",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
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
            |ast_a, ast_b| ast_a.migrate(&ast_b),
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
            |ast_a, ast_b| ast_a.migrate(&ast_b),
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
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }

    #[test]
    fn apply_alter_table_alter_column() {
        run_test_cases(
            vec![
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
            ],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }

    #[test]
    fn apply_create_index() {
        run_test_cases(
            vec![
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                    sql_b: "CREATE INDEX code_idx ON films (code);",
                    expect: "CREATE UNIQUE INDEX title_idx ON films(title);\n\nCREATE INDEX code_idx ON films(code);",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                    sql_b: "DROP INDEX title_idx;",
                    expect: "",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                    sql_b: "DROP INDEX title_idx;CREATE INDEX code_idx ON films (code);",
                    expect: "CREATE INDEX code_idx ON films(code);",
                },
            ],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }

    #[test]
    fn apply_alter_create_type() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                sql_b: "CREATE TYPE compfoo AS (f1 int, f2 text);",
                expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');\n\nCREATE TYPE compfoo AS (f1 INT, f2 TEXT);",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }

    #[test]
    fn apply_alter_type_rename() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                sql_b: "ALTER TYPE bug_status RENAME TO issue_status",
                expect: "CREATE TYPE issue_status AS ENUM ('open', 'closed');",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }

    #[test]
    fn apply_alter_type_add_value() {
        run_test_cases(
            vec![
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                    sql_b: "ALTER TYPE bug_status ADD VALUE 'new' BEFORE 'open';",
                    expect: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                    sql_b: "ALTER TYPE bug_status ADD VALUE 'closed' AFTER 'open';",
                    expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                },
                TestCase {
                    dialect: Dialect::Generic,
                    sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                    sql_b: "ALTER TYPE bug_status ADD VALUE 'closed';",
                    expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                },
            ],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }

    #[test]
    fn apply_alter_type_rename_value() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'closed');",
                sql_b: "ALTER TYPE bug_status RENAME VALUE 'new' TO 'open';",
                expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }

    #[test]
    fn apply_create_extension() {
        run_test_cases(
            vec![TestCase {
                dialect: Dialect::Generic,
                sql_a: "CREATE EXTENSION hstore;",
                sql_b: "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
                expect: "CREATE EXTENSION hstore;\n\nCREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
            }],
            |ast_a, ast_b| ast_a.migrate(&ast_b),
        );
    }
}
