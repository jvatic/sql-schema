use std::{cmp::Ordering, collections::HashSet};

use sqlparser::ast::{
    AlterTableOperation, AlterType, AlterTypeAddValue, AlterTypeAddValuePosition,
    AlterTypeOperation, CreateIndex, CreateTable, Ident, ObjectName, ObjectType, Statement,
    UserDefinedTypeRepresentation,
};

pub trait Diff: Sized {
    type Diff;

    fn diff(&self, other: &Self) -> Self::Diff;
}

impl Diff for Vec<Statement> {
    type Diff = Option<Vec<Statement>>;

    fn diff(&self, other: &Self) -> Self::Diff {
        let res = self
            .iter()
            .filter_map(|sa| match sa {
                // CreateTable: compare against another CreateTable with the same name
                // TODO: handle renames (e.g. use comments to tag a previous name for a table in a schema)
                Statement::CreateTable(a) => find_and_compare_create_table(sa, a, other),
                Statement::CreateIndex(a) => find_and_compare_create_index(sa, a, other),
                Statement::CreateType { name, .. } => find_and_compare_create_type(sa, name, other),
                Statement::CreateExtension {
                    name,
                    if_not_exists,
                    cascade,
                    ..
                } => find_and_compare_create_extension(sa, name, *if_not_exists, *cascade, other),
                _ => todo!("diff all kinds of statments"),
            })
            // find resources that are in `other` but not in `self`
            .chain(other.iter().filter_map(|sb| {
                match sb {
                    Statement::CreateTable(b) => self.iter().find(|sa| match sa {
                        Statement::CreateTable(a) => a.name == b.name,
                        _ => false,
                    }),
                    Statement::CreateIndex(b) => self.iter().find(|sa| match sa {
                        Statement::CreateIndex(a) => a.name == b.name,
                        _ => false,
                    }),
                    Statement::CreateType { name: b_name, .. } => self.iter().find(|sa| match sa {
                        Statement::CreateType { name: a_name, .. } => a_name == b_name,
                        _ => false,
                    }),
                    Statement::CreateExtension { name: b_name, .. } => {
                        self.iter().find(|sa| match sa {
                            Statement::CreateExtension { name: a_name, .. } => a_name == b_name,
                            _ => false,
                        })
                    }
                    _ => todo!("diff all kinds of statements (other)"),
                }
                // return the statement if it's not in `self`
                .map_or_else(|| Some(vec![sb.clone()]), |_| None)
            }))
            .flatten()
            .collect::<Vec<_>>();

        if res.is_empty() {
            None
        } else {
            Some(res)
        }
    }
}

fn find_and_compare<MF, DF>(
    sa: &Statement,
    other: &[Statement],
    match_fn: MF,
    drop_fn: DF,
) -> Option<Vec<Statement>>
where
    MF: Fn(&&Statement) -> bool,
    DF: Fn() -> Option<Vec<Statement>>,
{
    other.iter().find(match_fn).map_or_else(
        // drop the statement if it wasn't found in `other`
        drop_fn,
        // otherwise diff the two statements
        |sb| sa.diff(sb),
    )
}

fn find_and_compare_create_table(
    sa: &Statement,
    a: &CreateTable,
    other: &[Statement],
) -> Option<Vec<Statement>> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateTable(b) => a.name == b.name,
            _ => false,
        },
        || {
            Some(vec![Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Table,
                if_exists: a.if_not_exists,
                names: vec![a.name.clone()],
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
            }])
        },
    )
}

fn find_and_compare_create_index(
    sa: &Statement,
    a: &CreateIndex,
    other: &[Statement],
) -> Option<Vec<Statement>> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateIndex(b) => a.name == b.name,
            _ => false,
        },
        || {
            let name = a
                .name
                .clone()
                .expect("can't drop an unnamed index, please ensure all indexes are named");
            Some(vec![Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Index,
                if_exists: a.if_not_exists,
                names: vec![name],
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
            }])
        },
    )
}

fn find_and_compare_create_type(
    sa: &Statement,
    a_name: &ObjectName,
    other: &[Statement],
) -> Option<Vec<Statement>> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateType { name: b_name, .. } => a_name == b_name,
            _ => false,
        },
        || {
            Some(vec![Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Type,
                if_exists: false,
                names: vec![a_name.clone()],
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
            }])
        },
    )
}

fn find_and_compare_create_extension(
    sa: &Statement,
    a_name: &Ident,
    if_not_exists: bool,
    cascade: bool,
    other: &[Statement],
) -> Option<Vec<Statement>> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateExtension { name: b_name, .. } => a_name == b_name,
            _ => false,
        },
        || {
            Some(vec![Statement::DropExtension {
                names: vec![a_name.clone()],
                if_exists: if_not_exists,
                cascade_or_restrict: if cascade {
                    Some(sqlparser::ast::ReferentialAction::Cascade)
                } else {
                    None
                },
            }])
        },
    )
}

impl Diff for Statement {
    type Diff = Option<Vec<Statement>>;

    fn diff(&self, other: &Self) -> Self::Diff {
        match self {
            Self::CreateTable(a) => match other {
                Self::CreateTable(b) => compare_create_table(a, b),
                _ => None,
            },
            Self::CreateIndex(a) => match other {
                Self::CreateIndex(b) => compare_create_index(a, b),
                _ => None,
            },
            Self::CreateType {
                name: a_name,
                representation: a_rep,
            } => match other {
                Self::CreateType {
                    name: b_name,
                    representation: b_rep,
                } => compare_create_type(a_name, a_rep, b_name, b_rep),
                _ => None,
            },
            _ => todo!("implement diff for all `Statement`s"),
        }
    }
}

fn compare_create_table(a: &CreateTable, b: &CreateTable) -> Option<Vec<Statement>> {
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

    Some(vec![Statement::AlterTable {
        name: a.name.clone(),
        if_exists: a.if_not_exists,
        only: false,
        operations: ops,
        location: None,
        on_cluster: a.on_cluster.clone(),
    }])
}

fn compare_create_index(a: &CreateIndex, b: &CreateIndex) -> Option<Vec<Statement>> {
    if a == b {
        return None;
    }

    if a.name.is_none() || b.name.is_none() {
        panic!("can't diff unnamed indexes!");
    }
    let name = a.name.clone().unwrap();

    Some(vec![
        Statement::Drop {
            object_type: ObjectType::Index,
            if_exists: a.if_not_exists,
            names: vec![name],
            cascade: false,
            restrict: false,
            purge: false,
            temporary: false,
        },
        Statement::CreateIndex(b.clone()),
    ])
}

fn compare_create_type(
    a_name: &ObjectName,
    a_rep: &UserDefinedTypeRepresentation,
    b_name: &ObjectName,
    b_rep: &UserDefinedTypeRepresentation,
) -> Option<Vec<Statement>> {
    if a_name == b_name && a_rep == b_rep {
        return None;
    }

    let operations = match a_rep {
        UserDefinedTypeRepresentation::Enum { labels: a_labels } => match b_rep {
            UserDefinedTypeRepresentation::Enum { labels: b_labels } => {
                match a_labels.len().cmp(&b_labels.len()) {
                    Ordering::Equal => {
                        let rename_labels: Vec<_> = a_labels
                            .iter()
                            .zip(b_labels.iter())
                            .filter_map(|(a, b)| {
                                if a == b {
                                    None
                                } else {
                                    Some(AlterTypeOperation::RenameValue(
                                        sqlparser::ast::AlterTypeRenameValue {
                                            from: a.clone(),
                                            to: b.clone(),
                                        },
                                    ))
                                }
                            })
                            .collect();
                        rename_labels
                    }
                    Ordering::Less => {
                        let mut a_labels_iter = a_labels.iter().peekable();
                        let mut operations = Vec::new();
                        let mut prev = None;
                        for b in b_labels {
                            match a_labels_iter.peek() {
                                Some(a) => {
                                    let a = *a;
                                    if a == b {
                                        prev = Some(a);
                                        a_labels_iter.next();
                                        continue;
                                    }

                                    let position = match prev {
                                        Some(a) => AlterTypeAddValuePosition::After(a.clone()),
                                        None => AlterTypeAddValuePosition::Before(a.clone()),
                                    };

                                    prev = Some(b);
                                    operations.push(AlterTypeOperation::AddValue(
                                        AlterTypeAddValue {
                                            if_not_exists: false,
                                            value: b.clone(),
                                            position: Some(position),
                                        },
                                    ));
                                }
                                None => {
                                    if a_labels.contains(b) {
                                        continue;
                                    }
                                    // labels occuring after all existing ones get added to the end
                                    operations.push(AlterTypeOperation::AddValue(
                                        AlterTypeAddValue {
                                            if_not_exists: false,
                                            value: b.clone(),
                                            position: None,
                                        },
                                    ));
                                }
                            }
                        }
                        operations
                    }
                    _ => {
                        todo!("Handle removing labels from an enum")
                    }
                }
            }
            _ => todo!("DROP type and CREATE type"),
        },
        _ => todo!("handle diffing composite attributes for CREATE TYPE"),
    };

    if operations.is_empty() {
        return None;
    }

    Some(
        operations
            .into_iter()
            .map(|operation| {
                Statement::AlterType(AlterType {
                    name: a_name.clone(),
                    operation,
                })
            })
            .collect(),
    )
}
