use std::{cmp::Ordering, collections::HashSet};

use crate::{
    ast::{
        AlterTable, AlterTableOperation, AlterType, AlterTypeAddValue, AlterTypeAddValuePosition,
        AlterTypeOperation, AlterTypeRenameValue, AttachedToken, CreateDomain, CreateIndex,
        CreateTable, CreateType, DropDomain, ObjectType, Statement, UserDefinedTypeRepresentation,
    },
    diff::{DiffError, DiffErrorKind, Result, StatementDiffer},
};

pub fn diff<Dialect>(
    dialect: &Dialect,
    sa: &Statement,
    sb: &Statement,
) -> Result<Option<Vec<Statement>>>
where
    Dialect: StatementDiffer,
{
    match sa {
        Statement::CreateTable(a) => match sb {
            Statement::CreateTable(b) => dialect.compare_create_table(a, b),
            _ => Ok(None),
        },
        Statement::CreateIndex(a) => match sb {
            Statement::CreateIndex(b) => dialect.compare_create_index(a, b),
            _ => Ok(None),
        },
        Statement::CreateType {
            name: a_name,
            representation: a_rep,
        } => match sb {
            Statement::CreateType {
                name: b_name,
                representation: b_rep,
            } => dialect.compare_create_type(
                &CreateType {
                    name: a_name.clone(),
                    representation: a_rep.clone(),
                },
                &CreateType {
                    name: b_name.clone(),
                    representation: b_rep.clone(),
                },
            ),
            _ => Ok(None),
        },
        Statement::CreateDomain(a) => match sb {
            Statement::CreateDomain(b) => dialect.compare_create_domain(a, b),
            _ => Ok(None),
        },
        _ => Err(DiffError::builder()
            .kind(DiffErrorKind::NotImplemented)
            .statement_a(sa.clone())
            .statement_b(sb.clone())
            .build()),
    }
}

pub fn compare_create_table(a: &CreateTable, b: &CreateTable) -> Result<Option<Vec<Statement>>> {
    if a == b {
        return Ok(None);
    }

    let a_column_names: HashSet<_> = a.columns.iter().map(|c| c.name.value.clone()).collect();
    let b_column_names: HashSet<_> = b.columns.iter().map(|c| c.name.value.clone()).collect();

    let operations: Vec<_> = a
        .columns
        .iter()
        .filter_map(|ac| {
            if b_column_names.contains(&ac.name.value) {
                None
            } else {
                // drop column if it only exists in `a`
                Some(AlterTableOperation::DropColumn {
                    column_names: vec![ac.name.clone()],
                    if_exists: a.if_not_exists,
                    drop_behavior: None,
                    has_column_keyword: true,
                })
            }
        })
        .chain(b.columns.iter().filter_map(|bc| {
            if a_column_names.contains(&bc.name.value) {
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

    if operations.is_empty() {
        return Ok(None);
    }

    Ok(Some(vec![Statement::AlterTable(AlterTable {
        table_type: None,
        name: a.name.clone(),
        if_exists: a.if_not_exists,
        only: false,
        operations,
        location: None,
        on_cluster: a.on_cluster.clone(),
        end_token: AttachedToken::empty(),
    })]))
}

pub fn compare_create_index(a: &CreateIndex, b: &CreateIndex) -> Result<Option<Vec<Statement>>> {
    if a == b {
        return Ok(None);
    }

    if a.name.is_none() || b.name.is_none() {
        Err(DiffError::builder()
            .kind(DiffErrorKind::CompareUnnamedIndex)
            .statement_a(Statement::CreateIndex(a.clone()))
            .statement_b(Statement::CreateIndex(b.clone()))
            .build())?;
    }
    let name = a.name.clone().unwrap();

    Ok(Some(vec![
        Statement::Drop {
            object_type: ObjectType::Index,
            if_exists: a.if_not_exists,
            names: vec![name],
            cascade: false,
            restrict: false,
            purge: false,
            temporary: false,
            table: None,
        },
        Statement::CreateIndex(b.clone()),
    ]))
}

pub fn compare_create_type(a: &CreateType, b: &CreateType) -> Result<Option<Vec<Statement>>> {
    if a == b {
        return Ok(None);
    }

    let operations = match &a.representation {
        Some(UserDefinedTypeRepresentation::Enum { labels: a_labels }) => match &b.representation {
            Some(UserDefinedTypeRepresentation::Enum { labels: b_labels }) => {
                match a_labels.len().cmp(&b_labels.len()) {
                    Ordering::Equal => {
                        let rename_labels: Vec<_> = a_labels
                            .iter()
                            .zip(b_labels.iter())
                            .filter_map(|(a, b)| {
                                if a == b {
                                    None
                                } else {
                                    Some(AlterTypeOperation::RenameValue(AlterTypeRenameValue {
                                        from: a.clone(),
                                        to: b.clone(),
                                    }))
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
                        return Err(DiffError::builder()
                            .kind(DiffErrorKind::RemoveEnumLabel)
                            .statement_a(a.clone())
                            .statement_b(b.clone())
                            .build())?;
                    }
                }
            }
            _ => {
                // TODO: DROP and CREATE type
                return Err(DiffError::builder()
                    .kind(DiffErrorKind::NotImplemented)
                    .statement_a(a.clone())
                    .statement_b(b.clone())
                    .build())?;
            }
        },
        _ => {
            // TODO: handle diffing composite attributes for CREATE TYPE
            return Err(DiffError::builder()
                .kind(DiffErrorKind::NotImplemented)
                .statement_a(a.clone())
                .statement_b(b.clone())
                .build())?;
        }
    };

    if operations.is_empty() {
        return Ok(None);
    }

    Ok(Some(
        operations
            .into_iter()
            .map(|operation| {
                Statement::AlterType(AlterType {
                    name: a.name.clone(),
                    operation,
                })
            })
            .collect(),
    ))
}

pub fn compare_create_domain(a: &CreateDomain, b: &CreateDomain) -> Result<Option<Vec<Statement>>> {
    if a == b {
        return Ok(None);
    }

    Ok(Some(vec![
        Statement::DropDomain(DropDomain {
            if_exists: true,
            name: a.name.clone(),
            drop_behavior: None,
        }),
        Statement::CreateDomain(b.clone()),
    ]))
}
