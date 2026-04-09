use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt,
};

use bon::bon;
use sqlparser::ast::{
    helpers::attached_token::AttachedToken, AlterColumnOperation, AlterTable, AlterTableOperation,
    AlterType, AlterTypeAddValue, AlterTypeAddValuePosition, AlterTypeOperation, ColumnOption,
    ColumnOptionDef, CreateDomain, CreateExtension, CreateIndex, CreateTable, DropDomain,
    DropExtension, Ident, ObjectName, ObjectType, Statement, UserDefinedTypeRepresentation,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub struct DiffError {
    kind: DiffErrorKind,
    statement_a: Option<Box<Statement>>,
    statement_b: Option<Box<Statement>>,
}

impl fmt::Display for DiffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Oops, we couldn't diff that: {reason}",
            reason = self.kind
        )?;
        if let Some(statement_a) = &self.statement_a {
            write!(f, "\n\nStatement A:\n{statement_a}")?;
        }
        if let Some(statement_b) = &self.statement_b {
            write!(f, "\n\nStatement B:\n{statement_b}")?;
        }
        Ok(())
    }
}

#[bon]
impl DiffError {
    #[builder]
    fn new(
        kind: DiffErrorKind,
        statement_a: Option<Statement>,
        statement_b: Option<Statement>,
    ) -> Self {
        Self {
            kind,
            statement_a: statement_a.map(Box::new),
            statement_b: statement_b.map(Box::new),
        }
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
enum DiffErrorKind {
    #[error("can't drop unnamed index")]
    DropUnnamedIndex,
    #[error("can't compare unnamed index")]
    CompareUnnamedIndex,
    #[error("removing enum labels is not supported")]
    RemoveEnumLabel,
    #[error("not yet supported")]
    NotImplemented,
}

pub(crate) trait Diff: Sized {
    type Diff;

    fn diff(&self, other: &Self) -> Result<Self::Diff, DiffError>;
}

impl Diff for Vec<Statement> {
    type Diff = Option<Vec<Statement>>;

    fn diff(&self, other: &Self) -> Result<Self::Diff, DiffError> {
        let res = self
            .iter()
            .filter_map(|sa| {
                match sa {
                    // CreateTable: compare against another CreateTable with the same name
                    // TODO: handle renames (e.g. use comments to tag a previous name for a table in a schema)
                    Statement::CreateTable(a) => find_and_compare_create_table(sa, a, other),
                    Statement::CreateIndex(a) => find_and_compare_create_index(sa, a, other),
                    Statement::CreateType { name, .. } => {
                        find_and_compare_create_type(sa, name, other)
                    }
                    Statement::CreateExtension(CreateExtension {
                        name,
                        if_not_exists,
                        cascade,
                        ..
                    }) => {
                        find_and_compare_create_extension(sa, name, *if_not_exists, *cascade, other)
                    }
                    Statement::CreateDomain(a) => find_and_compare_create_domain(sa, a, other),
                    _ => Err(DiffError::builder()
                        .kind(DiffErrorKind::NotImplemented)
                        .statement_a(sa.clone())
                        .build()),
                }
                .transpose()
            })
            // find resources that are in `other` but not in `self`
            .chain(other.iter().filter_map(|sb| {
                match sb {
                    Statement::CreateTable(b) => Ok(self.iter().find(|sa| match sa {
                        Statement::CreateTable(a) => a.name == b.name,
                        _ => false,
                    })),
                    Statement::CreateIndex(b) => Ok(self.iter().find(|sa| match sa {
                        Statement::CreateIndex(a) => a.name == b.name,
                        _ => false,
                    })),
                    Statement::CreateType { name: b_name, .. } => {
                        Ok(self.iter().find(|sa| match sa {
                            Statement::CreateType { name: a_name, .. } => a_name == b_name,
                            _ => false,
                        }))
                    }
                    Statement::CreateExtension(CreateExtension { name: b_name, .. }) => {
                        Ok(self.iter().find(|sa| match sa {
                            Statement::CreateExtension(CreateExtension {
                                name: a_name, ..
                            }) => a_name == b_name,
                            _ => false,
                        }))
                    }
                    Statement::CreateDomain(b) => Ok(self.iter().find(|sa| match sa {
                        Statement::CreateDomain(a) => a.name == b.name,
                        _ => false,
                    })),
                    _ => Err(DiffError::builder()
                        .kind(DiffErrorKind::NotImplemented)
                        .statement_a(sb.clone())
                        .build()),
                }
                .transpose()
                // return the statement if it's not in `self`
                .map_or_else(|| Some(Ok(vec![sb.clone()])), |_| None)
            }))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        if res.is_empty() {
            Ok(None)
        } else {
            Ok(Some(res))
        }
    }
}

fn find_and_compare<MF, DF>(
    sa: &Statement,
    other: &[Statement],
    match_fn: MF,
    drop_fn: DF,
) -> Result<Option<Vec<Statement>>, DiffError>
where
    MF: Fn(&&Statement) -> bool,
    DF: Fn() -> Result<Option<Vec<Statement>>, DiffError>,
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
) -> Result<Option<Vec<Statement>>, DiffError> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateTable(b) => a.name == b.name,
            _ => false,
        },
        || {
            Ok(Some(vec![Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Table,
                if_exists: a.if_not_exists,
                names: vec![a.name.clone()],
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
                table: None,
            }]))
        },
    )
}

fn find_and_compare_create_index(
    sa: &Statement,
    a: &CreateIndex,
    other: &[Statement],
) -> Result<Option<Vec<Statement>>, DiffError> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateIndex(b) => a.name == b.name,
            _ => false,
        },
        || {
            let name = a.name.clone().ok_or_else(|| {
                DiffError::builder()
                    .kind(DiffErrorKind::DropUnnamedIndex)
                    .statement_a(sa.clone())
                    .build()
            })?;

            Ok(Some(vec![Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Index,
                if_exists: a.if_not_exists,
                names: vec![name],
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
                table: None,
            }]))
        },
    )
}

fn find_and_compare_create_type(
    sa: &Statement,
    a_name: &ObjectName,
    other: &[Statement],
) -> Result<Option<Vec<Statement>>, DiffError> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateType { name: b_name, .. } => a_name == b_name,
            _ => false,
        },
        || {
            Ok(Some(vec![Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Type,
                if_exists: false,
                names: vec![a_name.clone()],
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
                table: None,
            }]))
        },
    )
}

fn find_and_compare_create_extension(
    sa: &Statement,
    a_name: &Ident,
    if_not_exists: bool,
    cascade: bool,
    other: &[Statement],
) -> Result<Option<Vec<Statement>>, DiffError> {
    find_and_compare(
        sa,
        other,
        |sb| match sb {
            Statement::CreateExtension(CreateExtension { name: b_name, .. }) => a_name == b_name,
            _ => false,
        },
        || {
            Ok(Some(vec![Statement::DropExtension(DropExtension {
                names: vec![a_name.clone()],
                if_exists: if_not_exists,
                cascade_or_restrict: if cascade {
                    Some(sqlparser::ast::ReferentialAction::Cascade)
                } else {
                    None
                },
            })]))
        },
    )
}

fn find_and_compare_create_domain(
    orig: &Statement,
    domain: &CreateDomain,
    other: &[Statement],
) -> Result<Option<Vec<Statement>>, DiffError> {
    let res = other
        .iter()
        .find(|sb| match sb {
            Statement::CreateDomain(b) => b.name == domain.name,
            _ => false,
        })
        .map(|sb| orig.diff(sb))
        .transpose()?
        .flatten();
    Ok(res)
}

impl Diff for Statement {
    type Diff = Option<Vec<Statement>>;

    fn diff(&self, other: &Self) -> Result<Self::Diff, DiffError> {
        match self {
            Self::CreateTable(a) => match other {
                Self::CreateTable(b) => Ok(compare_create_table(a, b)),
                _ => Ok(None),
            },
            Self::CreateIndex(a) => match other {
                Self::CreateIndex(b) => compare_create_index(a, b),
                _ => Ok(None),
            },
            Self::CreateType {
                name: a_name,
                representation: a_rep,
            } => match other {
                Self::CreateType {
                    name: b_name,
                    representation: b_rep,
                } => compare_create_type(self, a_name, a_rep, other, b_name, b_rep),
                _ => Ok(None),
            },
            Self::CreateDomain(a) => match other {
                Self::CreateDomain(b) => Ok(compare_create_domain(a, b)),
                _ => Ok(None),
            },
            _ => Err(DiffError::builder()
                .kind(DiffErrorKind::NotImplemented)
                .statement_a(self.clone())
                .statement_b(other.clone())
                .build()),
        }
    }
}

fn compare_create_table(a: &CreateTable, b: &CreateTable) -> Option<Vec<Statement>> {
    if a == b {
        return None;
    }

    let a_columns: HashMap<_, _> = a
        .columns
        .iter()
        .map(|c| (c.name.value.clone(), c))
        .collect();
    let b_columns: HashMap<_, _> = b
        .columns
        .iter()
        .map(|c| (c.name.value.clone(), c))
        .collect();

    fn remove_column_option(op: &ColumnOptionDef) -> AlterColumnOperation {
        match &op.option {
            ColumnOption::NotNull => AlterColumnOperation::DropNotNull,
            ColumnOption::Default(_) => AlterColumnOperation::DropDefault,
            _ => todo!("remove column option not supported for {op:?}"),
        }
    }

    fn add_column_option(op: &ColumnOptionDef) -> AlterColumnOperation {
        match &op.option {
            ColumnOption::NotNull => AlterColumnOperation::SetNotNull,
            ColumnOption::Default(expr) => AlterColumnOperation::SetDefault {
                value: expr.clone(),
            },
            _ => todo!("add column option not supported for {op:?}"),
        }
    }

    fn compare_column_options(
        oa: &[ColumnOptionDef],
        ob: &[ColumnOptionDef],
    ) -> Vec<AlterColumnOperation> {
        let obs: HashSet<_> = ob.iter().cloned().collect();
        let oas: HashSet<_> = oa.iter().cloned().collect();
        let mut ops = Vec::with_capacity(oa.len() + ob.len());

        for aop in oa {
            match obs.get(aop) {
                Some(bop) if aop == bop => {}
                Some(bop) => {
                    // column option has changed
                    ops.push(remove_column_option(aop));
                    ops.push(add_column_option(bop));
                }
                None => {
                    // column option has been removed
                    ops.push(remove_column_option(aop));
                }
            }
        }

        for bop in ob {
            match oas.get(bop) {
                Some(_) => {
                    // column option has changed, covered above
                }
                None => {
                    // column option has been added
                    ops.push(add_column_option(bop));
                }
            }
        }

        ops
    }

    let operations: Vec<_> = a
        .columns
        .iter()
        .filter_map(|ac| {
            match b_columns.get(&ac.name.value) {
                // nothing has changed
                Some(bc) if ac.data_type == bc.data_type && ac.options == bc.options => None,
                // data type and/or options have changed
                Some(bc) => {
                    let mut ops = Vec::new();

                    if ac.data_type != bc.data_type {
                        ops.push(AlterTableOperation::AlterColumn {
                            column_name: bc.name.clone(),
                            op: AlterColumnOperation::SetDataType {
                                data_type: bc.data_type.clone(),
                                using: None,
                                had_set: true,
                            },
                        });
                    }

                    ops.extend(
                        compare_column_options(&ac.options, &bc.options)
                            .into_iter()
                            .map(|op| AlterTableOperation::AlterColumn {
                                column_name: bc.name.clone(),
                                op: op,
                            }),
                    );

                    Some(ops)
                }
                // column only exists in `a`
                None => Some(vec![AlterTableOperation::DropColumn {
                    column_names: vec![ac.name.clone()],
                    if_exists: a.if_not_exists,
                    drop_behavior: None,
                    has_column_keyword: true,
                }]),
            }
        })
        .chain(b.columns.iter().filter_map(|bc| {
            match a_columns.get(&bc.name.value) {
                // data type and/or options have changed, so it's handled above
                Some(_) => None,
                // column only exists in `b`
                None => Some(vec![AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: a.if_not_exists,
                    column_def: bc.clone(),
                    column_position: None,
                }]),
            }
        }))
        .flatten()
        .collect();

    if operations.is_empty() {
        return None;
    }

    Some(vec![Statement::AlterTable(AlterTable {
        table_type: None,
        name: a.name.clone(),
        if_exists: false,
        only: false,
        operations,
        location: None,
        on_cluster: a.on_cluster.clone(),
        end_token: AttachedToken::empty(),
    })])
}

fn compare_create_index(
    a: &CreateIndex,
    b: &CreateIndex,
) -> Result<Option<Vec<Statement>>, DiffError> {
    if a == b {
        return Ok(None);
    }

    if a.name.is_none() || b.name.is_none() {
        return Err(DiffError::builder()
            .kind(DiffErrorKind::CompareUnnamedIndex)
            .statement_a(Statement::CreateIndex(a.clone()))
            .statement_b(Statement::CreateIndex(b.clone()))
            .build());
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

fn compare_create_type(
    a: &Statement,
    a_name: &ObjectName,
    a_rep: &Option<UserDefinedTypeRepresentation>,
    b: &Statement,
    b_name: &ObjectName,
    b_rep: &Option<UserDefinedTypeRepresentation>,
) -> Result<Option<Vec<Statement>>, DiffError> {
    if a_name == b_name && a_rep == b_rep {
        return Ok(None);
    }

    let operations = match a_rep {
        Some(UserDefinedTypeRepresentation::Enum { labels: a_labels }) => match b_rep {
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
                        return Err(DiffError::builder()
                            .kind(DiffErrorKind::RemoveEnumLabel)
                            .statement_a(a.clone())
                            .statement_b(b.clone())
                            .build());
                    }
                }
            }
            _ => {
                // TODO: DROP and CREATE type
                return Err(DiffError::builder()
                    .kind(DiffErrorKind::NotImplemented)
                    .statement_a(a.clone())
                    .statement_b(b.clone())
                    .build());
            }
        },
        _ => {
            // TODO: handle diffing composite attributes for CREATE TYPE
            return Err(DiffError::builder()
                .kind(DiffErrorKind::NotImplemented)
                .statement_a(a.clone())
                .statement_b(b.clone())
                .build());
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
                    name: a_name.clone(),
                    operation,
                })
            })
            .collect(),
    ))
}

fn compare_create_domain(a: &CreateDomain, b: &CreateDomain) -> Option<Vec<Statement>> {
    if a == b {
        return None;
    }

    Some(vec![
        Statement::DropDomain(DropDomain {
            if_exists: true,
            name: a.name.clone(),
            drop_behavior: None,
        }),
        Statement::CreateDomain(b.clone()),
    ])
}
