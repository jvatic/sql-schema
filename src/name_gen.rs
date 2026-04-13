use crate::{
    ast::{
        AlterTable, AlterTableOperation, AlterType, ColumnDef, CreateIndex, CreateTable,
        ObjectName, ObjectType, RenameTableNameKind, Statement,
    },
    SyntaxTree,
};

#[bon::builder(finish_fn = build)]
pub fn generate_name(
    #[builder(start_fn)] tree: &SyntaxTree,
    max_len: Option<usize>,
) -> Option<String> {
    let mut parts = tree
        .0
        .iter()
        .filter_map(|s| match s {
            Statement::CreateTable(CreateTable { name, .. }) => Some(format!("create_{name}")),
            Statement::AlterTable(AlterTable {
                name, operations, ..
            }) => alter_table_name(name, operations),
            Statement::Drop {
                object_type, names, ..
            } => {
                let object_type = match object_type {
                    ObjectType::Table => String::new(),
                    _ => object_type.to_string().to_lowercase() + "_",
                };
                let names = names
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()
                    .join("_and_");
                Some(format!("drop_{object_type}{names}"))
            }
            Statement::CreateType { name, .. } => Some(format!("create_type_{name}")),
            Statement::AlterType(AlterType { name, .. }) => Some(format!("alter_type_{name}")),
            Statement::CreateIndex(CreateIndex {
                name, table_name, ..
            }) => {
                let name = name.as_ref().map(|n| format!("_{n}")).unwrap_or_default();
                Some(format!("create_{table_name}{name}"))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut suffix = None;
    let mut name = parts.join("__");
    let max_len = max_len.unwrap_or(50);
    while name.len() > max_len {
        suffix = Some("etc");
        parts.pop();
        name = parts.join("__");
    }

    if let Some(suffix) = suffix {
        name = format!("{name}__{suffix}");
    }

    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn alter_table_name(name: &ObjectName, operations: &[AlterTableOperation]) -> Option<String> {
    let mut table_verb = "alter";
    let ops = operations
        .iter()
        .filter_map(|op| match op {
            AlterTableOperation::AddColumn {
                column_def: ColumnDef { name, .. },
                ..
            } => Some(format!("add_{name}")),
            AlterTableOperation::DropColumn { column_names, .. } => Some(format!(
                "drop_{}",
                column_names
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join("_")
            )),
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => Some(format!("rename_{old_column_name}_to_{new_column_name}")),
            AlterTableOperation::AlterColumn { column_name, .. } => {
                Some(format!("alter_{column_name}"))
            }
            AlterTableOperation::RenameTable { table_name } => {
                table_verb = "rename";
                Some(format!(
                    "to_{table_name}",
                    table_name = match table_name {
                        RenameTableNameKind::As(name) => name,
                        RenameTableNameKind::To(name) => name,
                    }
                ))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    Some(if ops.is_empty() || ops.len() > 2 {
        format!("{table_verb}_{name}")
    } else {
        format!("{table_verb}_{name}_{}", ops.join("_"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestCase {
        sql: &'static str,
        name: &'static str,
    }

    fn run_test_case(tc: &TestCase) {
        let tree = SyntaxTree::builder().sql(tc.sql).build().unwrap();
        let actual = generate_name(&tree).build();
        assert_eq!(actual, Some(tc.name.to_owned()), "{tc:?}");
    }

    macro_rules! test_case {
        (
            $(
                $test_name:ident {
                    $( $field:ident : $value:expr ),+ $(,)?
                }
            ),* $(,)?
        ) => {
            $(
                #[test]
                fn $test_name() {
                    let test_case = TestCase {
                        $( $field : $value ),+
                    };

                    run_test_case(&test_case);
                }
            )*
        };
    }

    test_case!(
        create_table {
            sql: "CREATE TABLE foo(bar TEXT);",
            name: "create_foo",
        },
        create_two_tables {
            sql: "CREATE TABLE foo(bar TEXT); CREATE TABLE bar(foo TEXT);",
            name: "create_foo__create_bar",
        },
        create_four_tables {
            sql: "CREATE TABLE foo(bar TEXT); CREATE TABLE bar(foo TEXT); CREATE TABLE baz(id INT); CREATE TABLE some_really_long_name(id INT);",
            name: "create_foo__create_bar__create_baz__etc",
        },
        drop_column {
            sql: "ALTER TABLE foo DROP COLUMN bar;",
            name: "alter_foo_drop_bar",
        },
        add_column {
            sql: "ALTER TABLE foo ADD COLUMN bar TEXT;",
            name: "alter_foo_add_bar",
        },
        alter_column {
            sql: "ALTER TABLE foo ALTER COLUMN bar SET DATA TYPE INT;",
            name: "alter_foo_alter_bar",
        },
        rename_column {
            sql: "ALTER TABLE foo RENAME bar TO id;",
            name: "alter_foo_rename_bar_to_id",
        },
        rename_table {
            sql: "ALTER TABLE foo RENAME TO bar;",
            name: "rename_foo_to_bar",
        },
        drop_table {
            sql: "DROP TABLE foo;",
            name: "drop_foo",
        },
        create_enum_type {
            sql: "CREATE TYPE status AS ENUM('one', 'two', 'three');",
            name: "create_type_status",
        },
        drop_type {
            sql: "DROP TYPE status;",
            name: "drop_type_status",
        },
        create_index {
            sql: "CREATE UNIQUE INDEX title_idx ON films (title);",
            name: "create_films_title_idx",
        },
        drop_index {
            sql: "DROP INDEX title_idx",
            name: "drop_index_title_idx",
        },
    );
}
