use std::collections::HashSet;

use sqlparser::ast::{AlterTableOperation, CreateTable, Statement};

pub trait Diff: Sized {
    fn diff(&self, other: &Self) -> Option<Self>;
}

impl Diff for Vec<Statement> {
    fn diff(&self, other: &Self) -> Option<Self> {
        let res: Self = self
            .iter()
            .filter_map(|sa| match sa {
                // CreateTable: compare against another CreateTable with the same name
                // TODO: handle renames (e.g. use comments to tag a previous name for a table in a schema)
                Statement::CreateTable(a) => other
                    .iter()
                    .find(|sb| match sb {
                        Statement::CreateTable(b) => a.name == b.name,
                        _ => false,
                    })
                    .map_or_else(
                        || {
                            // drop the table if it wasn't found in `other`
                            Some(Statement::Drop {
                                object_type: sqlparser::ast::ObjectType::Table,
                                if_exists: a.if_not_exists,
                                names: vec![a.name.clone()],
                                cascade: false,
                                restrict: false,
                                purge: false,
                                temporary: false,
                            })
                        },
                        |sb| sa.diff(sb),
                    ),
                _ => todo!("diff all kinds of statments"),
            })
            // find resources that are in `other` but not in `self`
            .chain(other.iter().filter_map(|sb| {
                match sb {
                    Statement::CreateTable(b) => self.iter().find(|sa| match sa {
                        Statement::CreateTable(a) => a.name == b.name,
                        _ => false,
                    }),
                    _ => todo!("diff all kinds of statements (other)"),
                }
                // return the statement if it's not in `self`
                .map_or_else(|| Some(sb.clone()), |_| None)
            }))
            .collect();

        if res.is_empty() {
            None
        } else {
            Some(res)
        }
    }
}

impl Diff for Statement {
    fn diff(&self, other: &Self) -> Option<Self> {
        match self {
            Self::CreateTable(a) => match other {
                Self::CreateTable(b) => compare_create_table(a, b),
                _ => None,
            },
            _ => todo!("implement diff for all `Statement`s"),
        }
    }
}

fn compare_create_table(a: &CreateTable, b: &CreateTable) -> Option<Statement> {
    if a == b {
        return None;
    }

    let a_column_names: HashSet<_> = a.columns.iter().map(|c| c.name.clone()).collect();
    let b_column_names: HashSet<_> = b.columns.iter().map(|c| c.name.clone()).collect();

    let ops = a
        .columns
        .iter()
        .filter_map(|ac| {
            if b_column_names.contains(&ac.name) {
                None
            } else {
                // drop column if it only exists in `a`
                Some(AlterTableOperation::DropColumn {
                    column_name: ac.name.clone(),
                    if_exists: a.if_not_exists,
                    drop_behavior: None,
                })
            }
        })
        .chain(b.columns.iter().filter_map(|bc| {
            if a_column_names.contains(&bc.name) {
                None
            } else {
                // add the column if it only exists in `b`
                Some(AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: a.if_not_exists,
                    column_def: bc.clone(),
                    column_position: None,
                })
            }
        }))
        .collect();

    Some(Statement::AlterTable {
        name: a.name.clone(),
        if_exists: a.if_not_exists,
        only: false,
        operations: ops,
        location: None,
        on_cluster: a.on_cluster.clone(),
    })
}
