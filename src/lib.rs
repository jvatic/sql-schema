use std::fmt;

use self::ast::Statement;

pub use self::{
    diff::TreeDiffer,
    migration::TreeMigrator,
    parser::{Parse, ParseError},
};

mod ast;
pub mod dialect;
mod diff;
mod migration;
pub mod name_gen;
mod parser;
pub mod path_template;
mod sealed;

#[derive(Debug, Clone)]
pub struct SyntaxTree<Dialect> {
    dialect: Dialect,
    pub(crate) tree: Vec<Statement>,
}

impl<Dialect: Default> SyntaxTree<Dialect> {
    pub fn empty() -> Self {
        Self {
            dialect: Default::default(),
            tree: Vec::with_capacity(0),
        }
    }
}

impl<Dialect> SyntaxTree<Dialect>
where
    Dialect: Parse,
{
    pub fn parse<'a>(dialect: Dialect, sql: impl Into<&'a str>) -> Result<Self, ParseError> {
        let tree = dialect.parse_sql::<Dialect>(sql)?;
        Ok(Self { dialect, tree })
    }
}

pub use diff::DiffError;
pub use migration::MigrateError;

impl<Dialect> SyntaxTree<Dialect>
where
    Dialect: TreeDiffer,
{
    pub fn diff(&self, other: &SyntaxTree<Dialect>) -> Result<Option<Self>, DiffError> {
        Ok(
            TreeDiffer::diff_tree(&self.dialect, &self.tree, &other.tree)?.map(|tree| Self {
                dialect: self.dialect.clone(),
                tree,
            }),
        )
    }
}

impl<Dialect> SyntaxTree<Dialect>
where
    Dialect: TreeMigrator,
{
    pub fn migrate(self, other: &SyntaxTree<Dialect>) -> Result<Self, MigrateError> {
        let tree = TreeMigrator::migrate_tree(&self.dialect, self.tree, &other.tree)?;
        Ok(Self {
            dialect: self.dialect.clone(),
            tree,
        })
    }
}

impl<Dialect> fmt::Display for SyntaxTree<Dialect> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iter = self.tree.iter().peekable();
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
    use super::dialect::Generic;
    use super::*;

    macro_rules! test_case {
        (
            @dialect($dialect:ty) $(,)?

            $(
                $test_name:ident { $( $field:ident : $value:literal ),+ $(,)? }
            ),* $(,)?

            => $test_fn:expr $(,)?
        ) => {
            $(
                #[test]
                fn $test_name() {
                    let dialect = <$dialect>::default();

                    let test_case: TestCase<$dialect> = TestCase {
                        dialect: dialect.clone(),
                        $( $field : $value ),+
                    };

                    run_test_case(&test_case, $test_fn);
                }
            )*
        };
    }

    #[derive(Debug)]
    struct TestCase<Dialect = Generic> {
        dialect: Dialect,
        sql_a: &'static str,
        sql_b: &'static str,
        expect: &'static str,
    }

    fn run_test_case<F, E, Dialect>(tc: &TestCase<Dialect>, testfn: F)
    where
        Dialect: Parse + TreeDiffer,
        E: std::error::Error,
        F: Fn(SyntaxTree<Dialect>, SyntaxTree<Dialect>) -> Result<Option<SyntaxTree<Dialect>>, E>,
    {
        let dialect = tc.dialect.clone();
        let ast_a = SyntaxTree::parse(dialect.clone(), tc.sql_a).unwrap();
        let ast_b = SyntaxTree::parse(dialect.clone(), tc.sql_b).unwrap();
        SyntaxTree::parse(dialect, tc.expect)
            .unwrap_or_else(|_| panic!("invalid SQL: {:?}", tc.expect));
        let actual = testfn(ast_a, ast_b)
            .inspect_err(|err| eprintln!("Error: {err:?}"))
            .unwrap()
            .unwrap();
        assert_eq!(actual.to_string(), tc.expect, "{tc:?}");
    }

    mod test_diff {
        use super::*;

        test_case!(
            @dialect(Generic)

            create_table_a {
                sql_a: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                )",
                sql_b: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                );\
                    CREATE TABLE bar (id INT PRIMARY KEY);",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);",
            },

            create_table_b {
                sql_a: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                )",
                sql_b: "CREATE TABLE foo(\
                    \"id\" int PRIMARY KEY
                );\
                    CREATE TABLE bar (id INT PRIMARY KEY);",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);",
            },

            create_table_c {
                sql_a: "CREATE TABLE foo(\
                    \"id\" int PRIMARY KEY
                )",
                sql_b: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                );\
                    CREATE TABLE bar (id INT PRIMARY KEY);",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);",
            },

            drop_table_a {
                sql_a: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                );\
                    CREATE TABLE bar (id INT PRIMARY KEY);",
                sql_b: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                )",
                expect: "DROP TABLE bar;",
            },

            add_column_a {
                sql_a: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                )",
                sql_b: "CREATE TABLE foo(\
                    id int PRIMARY KEY,
                    bar text
                )",
                expect: "ALTER TABLE\n  foo\nADD\n  COLUMN bar TEXT;",
            },

            drop_column_a {
                sql_a: "CREATE TABLE foo(\
                    id int PRIMARY KEY,
                    bar text
                )",
                sql_b: "CREATE TABLE foo(\
                    id int PRIMARY KEY
                )",
                expect: "ALTER TABLE\n  foo DROP COLUMN bar;",
            },

            create_index_a {
                sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                sql_b: "CREATE UNIQUE INDEX title_idx ON films ((lower(title)));",
                expect: "DROP INDEX title_idx;\n\nCREATE UNIQUE INDEX title_idx ON films((lower(title)));",
            },

            create_index_b {
                sql_a: "CREATE UNIQUE INDEX IF NOT EXISTS title_idx ON films (title);",
                sql_b: "CREATE UNIQUE INDEX IF NOT EXISTS title_idx ON films ((lower(title)));",
                expect: "DROP INDEX IF EXISTS title_idx;\n\nCREATE UNIQUE INDEX IF NOT EXISTS title_idx ON films((lower(title)));",
            },

            create_type_a {
                sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
                sql_b: "CREATE TYPE foo AS ENUM ('bar');",
                expect: "DROP TYPE bug_status;\n\nCREATE TYPE foo AS ENUM ('bar');",
            },

            create_type_b {
                sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'assigned', 'closed');",
                expect: "ALTER TYPE bug_status\nADD\n  VALUE 'assigned'\nAFTER\n  'open';",
            },

            create_type_c {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                expect: "ALTER TYPE bug_status\nADD\n  VALUE 'new' BEFORE 'open';",
            },

            create_type_d {
                sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
                sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                expect: "ALTER TYPE bug_status\nADD\n  VALUE 'closed';",
            },

            create_type_e {
                sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
                sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'assigned', 'closed');",
                expect: "ALTER TYPE bug_status\nADD\n  VALUE 'assigned';\n\nALTER TYPE bug_status\nADD\n  VALUE 'closed';",
            },

            create_type_f {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'critical');",
                sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'assigned', 'closed', 'critical');",
                expect: "ALTER TYPE bug_status\nADD\n  VALUE 'new' BEFORE 'open';\n\nALTER TYPE bug_status\nADD\n  VALUE 'assigned'\nAFTER\n  'open';\n\nALTER TYPE bug_status\nADD\n  VALUE 'closed'\nAFTER\n  'assigned';",
            },

            create_type_g {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                sql_b: "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');",
                expect: "ALTER TYPE bug_status\nADD\n  VALUE 'new' BEFORE 'open';\n\nALTER TYPE bug_status\nADD\n  VALUE 'closed';",
            },

            create_extension_a {
                sql_a: "CREATE EXTENSION hstore;",
                sql_b: "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
                expect: "DROP EXTENSION hstore;\n\nCREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
            },

            => |ast_a, ast_b| {
                ast_a.diff(&ast_b)
            }
        );

        test_case!(
            @dialect(Generic)

            create_domain_a {
                sql_a: "",
                sql_b: "CREATE DOMAIN email AS VARCHAR(255) CHECK (VALUE ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$');",
                expect: "CREATE DOMAIN email AS VARCHAR(255) CHECK (\n  VALUE ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'\n);",
            },

            edit_domain_a {
                sql_a: "CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0);",
                sql_b: "CREATE DOMAIN positive_int AS BIGINT CHECK (VALUE > 0 AND VALUE < 1000000);",
                expect: "DROP DOMAIN IF EXISTS positive_int;\n\nCREATE DOMAIN positive_int AS BIGINT CHECK (\n  VALUE > 0\n  AND VALUE < 1000000\n);",
            },

            => |ast_a, ast_b| {
                ast_a.diff(&ast_b)
            }
        );
    }

    mod migrate {
        use crate::dialect::PostgreSQL;

        use super::*;

        test_case!(
            @dialect(Generic)

            create_table_a {
                sql_a: "CREATE TABLE bar (id INT PRIMARY KEY);",
                sql_b: "CREATE TABLE foo (id INT PRIMARY KEY);",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);\n\nCREATE TABLE foo (id INT PRIMARY KEY);",
            },

            drop_table_a {
                sql_a: "CREATE TABLE bar (id INT PRIMARY KEY)",
                sql_b: "DROP TABLE bar; CREATE TABLE foo (id INT PRIMARY KEY)",
                expect: "CREATE TABLE foo (id INT PRIMARY KEY);",
            },

            alter_table_add_column_a {
                sql_a: "CREATE TABLE bar (id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ADD COLUMN bar TEXT",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY, bar TEXT);",
            },

            alter_table_drop_column_a {
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar DROP COLUMN bar",
                expect: "CREATE TABLE bar (id INT PRIMARY KEY);",
            },

            alter_table_alter_column_a {
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET NOT NULL",
                expect: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY);",
            },

            alter_table_alter_column_b {
                sql_a: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar DROP NOT NULL",
                expect: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY);",
            },

            alter_table_alter_column_c {
                sql_a: "CREATE TABLE bar (bar TEXT NOT NULL DEFAULT 'foo', id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar DROP DEFAULT",
                expect: "CREATE TABLE bar (bar TEXT NOT NULL, id INT PRIMARY KEY);",
            },

            alter_table_alter_column_d {
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET DATA TYPE INTEGER",
                expect: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY);",
            },

            alter_table_alter_column_f {
                sql_a: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar ADD GENERATED BY DEFAULT AS IDENTITY",
                expect: "CREATE TABLE bar (\n  bar INTEGER GENERATED BY DEFAULT AS IDENTITY,\n  id INT PRIMARY KEY\n);",
            },

            alter_table_alter_column_g {
                sql_a: "CREATE TABLE bar (bar INTEGER, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar ADD GENERATED ALWAYS AS IDENTITY (START WITH 10)",
                expect: "CREATE TABLE bar (\n  bar INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 10),\n  id INT PRIMARY KEY\n);",
            },

            create_index_a {
                sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                sql_b: "CREATE INDEX code_idx ON films (code);",
                expect: "CREATE UNIQUE INDEX title_idx ON films(title);\n\nCREATE INDEX code_idx ON films(code);",
            },

            create_index_b {
                sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                sql_b: "DROP INDEX title_idx;",
                expect: "",
            },

            create_index_c {
                sql_a: "CREATE UNIQUE INDEX title_idx ON films (title);",
                sql_b: "DROP INDEX title_idx;CREATE INDEX code_idx ON films (code);",
                expect: "CREATE INDEX code_idx ON films(code);",
            },

            alter_create_type_a {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                sql_b: "CREATE TYPE compfoo AS (f1 int, f2 text);",
                expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');\n\nCREATE TYPE compfoo AS (f1 INT, f2 TEXT);",
            },

            alter_type_rename_a {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
                sql_b: "ALTER TYPE bug_status RENAME TO issue_status",
                expect: "CREATE TYPE issue_status AS ENUM ('open', 'closed');",
            },

            alter_type_add_value_a {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                sql_b: "ALTER TYPE bug_status ADD VALUE 'new' BEFORE 'open';",
                expect: "CREATE TYPE bug_status AS ENUM ('new', 'open');",
            },

            alter_type_add_value_b {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                sql_b: "ALTER TYPE bug_status ADD VALUE 'closed' AFTER 'open';",
                expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
            },

            alter_type_add_value_c {
                sql_a: "CREATE TYPE bug_status AS ENUM ('open');",
                sql_b: "ALTER TYPE bug_status ADD VALUE 'closed';",
                expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
            },

            alter_type_rename_value_a {
                sql_a: "CREATE TYPE bug_status AS ENUM ('new', 'closed');",
                sql_b: "ALTER TYPE bug_status RENAME VALUE 'new' TO 'open';",
                expect: "CREATE TYPE bug_status AS ENUM ('open', 'closed');",
            },

            create_extension_a {
                sql_a: "CREATE EXTENSION hstore;",
                sql_b: "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
                expect: "CREATE EXTENSION hstore;\n\nCREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",
            },

            => |ast_a, ast_b| {
                Some(ast_a.migrate(&ast_b)).transpose()
            }
        );

        test_case!(
            @dialect(PostgreSQL)

            alter_table_alter_column_e {
                sql_a: "CREATE TABLE bar (bar TEXT, id INT PRIMARY KEY)",
                sql_b: "ALTER TABLE bar ALTER COLUMN bar SET DATA TYPE timestamp with time zone\n USING timestamp with time zone 'epoch' + foo_timestamp * interval '1 second'",
                expect: "CREATE TABLE bar (bar TIMESTAMP WITH TIME ZONE, id INT PRIMARY KEY);",
            },

            create_domain_a {
                sql_a: "CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0);",
                sql_b: "CREATE DOMAIN email AS VARCHAR(255) CHECK (VALUE ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$');",
                expect: "CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0);\n\nCREATE DOMAIN email AS VARCHAR(255) CHECK (\n  VALUE ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'\n);",
            },

            => |ast_a, ast_b| {
                Some(ast_a.migrate(&ast_b)).transpose()
            }
        );
    }
}
