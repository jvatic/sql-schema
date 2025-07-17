use std::fmt;

use bon::bon;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, AlterType, AlterTypeAddValuePosition,
    AlterTypeOperation, ColumnOption, ColumnOptionDef, CreateTable, GeneratedAs, ObjectName,
    ObjectNamePart, ObjectType, Statement, UserDefinedTypeRepresentation,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub struct MigrateError {
    kind: MigrateErrorKind,
    statement_a: Option<Box<Statement>>,
    statement_b: Option<Box<Statement>>,
}

impl fmt::Display for MigrateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Oops, we couldn't migrate that: {reason}",
            reason = self.kind
        )?;
        if let Some(statement_a) = &self.statement_a {
            write!(f, "\n\nSubject:\n{statement_a}")?;
        }
        if let Some(statement_b) = &self.statement_b {
            write!(f, "\n\nMigration:\n{statement_b}")?;
        }
        Ok(())
    }
}

#[bon]
impl MigrateError {
    #[builder]
    fn new(
        kind: MigrateErrorKind,
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
enum MigrateErrorKind {
    #[error("can't migrate unnamed index")]
    UnnamedIndex,
    #[error("ALTER TABLE operation \"{0}\" not yet supported")]
    AlterTableOpNotImplemented(Box<AlterTableOperation>),
    #[error("invalid ALTER TYPE operation \"{0}\"")]
    AlterTypeInvalidOp(Box<AlterTypeOperation>),
    #[error("not yet supported")]
    NotImplemented,
}

pub(crate) trait Migrate: Sized {
    fn migrate(self, other: &Self) -> Result<Option<Self>, MigrateError>;
}

impl Migrate for Vec<Statement> {
    fn migrate(self, other: &Self) -> Result<Option<Self>, MigrateError> {
        let next: Self = self
            .into_iter()
            // perform any transformations on existing schema (e.g. ALTER/DROP table)
            .filter_map(|sa| {
                let orig = sa.clone();
                match &sa {
                    Statement::CreateTable(ca) => other
                        .iter()
                        .find(|sb| match sb {
                            Statement::AlterTable { name, .. } => *name == ca.name,
                            Statement::Drop {
                                object_type, names, ..
                            } => {
                                *object_type == ObjectType::Table
                                    && names.len() == 1
                                    && names[0] == ca.name
                            }
                            _ => false,
                        })
                        .map_or(Some(Ok(orig)), |sb| sa.migrate(sb).transpose()),
                    Statement::CreateIndex(a) => other
                        .iter()
                        .find(|sb| match sb {
                            Statement::Drop {
                                object_type, names, ..
                            } => {
                                *object_type == ObjectType::Index
                                    && names.len() == 1
                                    && Some(&names[0]) == a.name.as_ref()
                            }
                            _ => false,
                        })
                        .map_or(Some(Ok(orig)), |sb| sa.migrate(sb).transpose()),
                    Statement::CreateType { name, .. } => other
                        .iter()
                        .find(|sb| match sb {
                            Statement::AlterType(b) => *name == b.name,
                            Statement::Drop {
                                object_type, names, ..
                            } => {
                                *object_type == ObjectType::Type
                                    && names.len() == 1
                                    && names[0] == *name
                            }
                            _ => false,
                        })
                        .map_or(Some(Ok(orig)), |sb| sa.migrate(sb).transpose()),
                    Statement::CreateExtension { name, .. } => other
                        .iter()
                        .find(|sb| match sb {
                            Statement::DropExtension { names, .. } => names.contains(name),
                            _ => false,
                        })
                        .map_or(Some(Ok(orig)), |sb| sa.migrate(sb).transpose()),
                    Statement::CreateDomain(a) => other
                        .iter()
                        .find(|sb| match sb {
                            Statement::DropDomain(b) => a.name == b.name,
                            _ => false,
                        })
                        .map_or(Some(Ok(orig)), |sb| sa.migrate(sb).transpose()),
                    _ => Some(Err(MigrateError::builder()
                        .kind(MigrateErrorKind::NotImplemented)
                        .statement_a(sa.clone())
                        .build())),
                }
            })
            // CREATE table etc.
            .chain(other.iter().filter_map(|sb| match sb {
                Statement::CreateTable(_)
                | Statement::CreateIndex { .. }
                | Statement::CreateType { .. }
                | Statement::CreateExtension { .. }
                | Statement::CreateDomain(..) => Some(Ok(sb.clone())),
                _ => None,
            }))
            .collect::<Result<_, _>>()?;
        Ok(Some(next))
    }
}

impl Migrate for Statement {
    fn migrate(self, other: &Self) -> Result<Option<Self>, MigrateError> {
        match self {
            Self::CreateTable(ca) => match other {
                Self::AlterTable {
                    name, operations, ..
                } => {
                    if *name == ca.name {
                        Ok(Some(Self::CreateTable(migrate_alter_table(
                            ca, operations,
                        )?)))
                    } else {
                        // ALTER TABLE statement for another table
                        Ok(Some(Self::CreateTable(ca)))
                    }
                }
                Self::Drop {
                    object_type, names, ..
                } => {
                    if *object_type == ObjectType::Table && names.contains(&ca.name) {
                        Ok(None)
                    } else {
                        // DROP statement is for another table
                        Ok(Some(Self::CreateTable(ca)))
                    }
                }
                _ => Err(MigrateError::builder()
                    .kind(MigrateErrorKind::NotImplemented)
                    .statement_a(Self::CreateTable(ca))
                    .statement_b(other.clone())
                    .build()),
            },
            Self::CreateIndex(a) => match other {
                Self::Drop {
                    object_type, names, ..
                } => {
                    let name = a.name.clone().ok_or_else(|| {
                        MigrateError::builder()
                            .kind(MigrateErrorKind::UnnamedIndex)
                            .statement_a(Self::CreateIndex(a.clone()))
                            .statement_b(other.clone())
                            .build()
                    })?;
                    if *object_type == ObjectType::Index && names.contains(&name) {
                        Ok(None)
                    } else {
                        // DROP statement is for another index
                        Ok(Some(Self::CreateIndex(a)))
                    }
                }
                _ => Err(MigrateError::builder()
                    .kind(MigrateErrorKind::NotImplemented)
                    .statement_a(Self::CreateIndex(a))
                    .statement_b(other.clone())
                    .build()),
            },
            Self::CreateType {
                name,
                representation,
            } => match other {
                Self::AlterType(ba) => {
                    if name == ba.name {
                        let (name, representation) =
                            migrate_alter_type(name.clone(), representation.clone(), ba)?;
                        Ok(Some(Self::CreateType {
                            name,
                            representation,
                        }))
                    } else {
                        // ALTER TYPE statement for another type
                        Ok(Some(Self::CreateType {
                            name,
                            representation,
                        }))
                    }
                }
                Self::Drop {
                    object_type, names, ..
                } => {
                    if *object_type == ObjectType::Type && names.contains(&name) {
                        Ok(None)
                    } else {
                        // DROP statement is for another type
                        Ok(Some(Self::CreateType {
                            name,
                            representation,
                        }))
                    }
                }
                _ => Err(MigrateError::builder()
                    .kind(MigrateErrorKind::NotImplemented)
                    .statement_a(Self::CreateType {
                        name,
                        representation,
                    })
                    .statement_b(other.clone())
                    .build()),
            },
            _ => Err(MigrateError::builder()
                .kind(MigrateErrorKind::NotImplemented)
                .statement_a(self)
                .statement_b(other.clone())
                .build()),
        }
    }
}

fn migrate_alter_table(
    mut t: CreateTable,
    ops: &[AlterTableOperation],
) -> Result<CreateTable, MigrateError> {
    for op in ops.iter() {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                t.columns.push(column_def.clone());
            }
            AlterTableOperation::DropColumn { column_name, .. } => {
                t.columns.retain(|c| c.name != *column_name);
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                t.columns.iter_mut().for_each(|c| {
                    if c.name != *column_name {
                        return;
                    }
                    match op {
                        AlterColumnOperation::SetNotNull => {
                            c.options.push(ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            });
                        }
                        AlterColumnOperation::DropNotNull => {
                            c.options
                                .retain(|o| !matches!(o.option, ColumnOption::NotNull));
                        }
                        AlterColumnOperation::SetDefault { value } => {
                            c.options
                                .retain(|o| !matches!(o.option, ColumnOption::Default(_)));
                            c.options.push(ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(value.clone()),
                            });
                        }
                        AlterColumnOperation::DropDefault => {
                            c.options
                                .retain(|o| !matches!(o.option, ColumnOption::Default(_)));
                        }
                        AlterColumnOperation::SetDataType {
                            data_type,
                            using: _, // not applicable since we're not running the query
                        } => {
                            c.data_type = data_type.clone();
                        }
                        AlterColumnOperation::AddGenerated {
                            generated_as,
                            sequence_options,
                        } => {
                            c.options
                                .retain(|o| !matches!(o.option, ColumnOption::Generated { .. }));
                            c.options.push(ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Generated {
                                    generated_as: generated_as
                                        .clone()
                                        .unwrap_or(GeneratedAs::Always),
                                    sequence_options: sequence_options.clone(),
                                    generation_expr: None,
                                    generation_expr_mode: None,
                                    generated_keyword: true,
                                },
                            });
                        }
                    }
                });
            }
            op => {
                return Err(MigrateError::builder()
                    .kind(MigrateErrorKind::AlterTableOpNotImplemented(Box::new(
                        op.clone(),
                    )))
                    .statement_a(Statement::CreateTable(t.clone()))
                    .build())
            }
        }
    }

    Ok(t)
}

fn migrate_alter_type(
    name: ObjectName,
    representation: UserDefinedTypeRepresentation,
    other: &AlterType,
) -> Result<(ObjectName, UserDefinedTypeRepresentation), MigrateError> {
    match &other.operation {
        AlterTypeOperation::Rename(r) => {
            let mut parts = name.0;
            parts.pop();
            parts.push(ObjectNamePart::Identifier(r.new_name.clone()));
            let name = ObjectName(parts);

            Ok((name, representation))
        }
        AlterTypeOperation::AddValue(a) => match representation {
            UserDefinedTypeRepresentation::Enum { mut labels } => {
                match &a.position {
                    Some(AlterTypeAddValuePosition::Before(before_name)) => {
                        let index = labels
                            .iter()
                            .enumerate()
                            .find(|(_, l)| *l == before_name)
                            .map(|(i, _)| i)
                            // insert at the beginning if `before_name` can't be found
                            .unwrap_or(0);
                        labels.insert(index, a.value.clone());
                    }
                    Some(AlterTypeAddValuePosition::After(after_name)) => {
                        let index = labels
                            .iter()
                            .enumerate()
                            .find(|(_, l)| *l == after_name)
                            .map(|(i, _)| i + 1);
                        match index {
                            Some(index) => labels.insert(index, a.value.clone()),
                            // push it to the end if `after_name` can't be found
                            None => labels.push(a.value.clone()),
                        }
                    }
                    None => labels.push(a.value.clone()),
                }

                Ok((name, UserDefinedTypeRepresentation::Enum { labels }))
            }
            UserDefinedTypeRepresentation::Composite { .. } => Err(MigrateError::builder()
                .kind(MigrateErrorKind::AlterTypeInvalidOp(Box::new(
                    other.operation.clone(),
                )))
                .statement_a(Statement::CreateType {
                    name,
                    representation,
                })
                .statement_b(Statement::AlterType(other.clone()))
                .build()),
        },
        AlterTypeOperation::RenameValue(rv) => match representation {
            UserDefinedTypeRepresentation::Enum { labels } => {
                let labels = labels
                    .into_iter()
                    .map(|l| if l == rv.from { rv.to.clone() } else { l })
                    .collect::<Vec<_>>();

                Ok((name, UserDefinedTypeRepresentation::Enum { labels }))
            }
            UserDefinedTypeRepresentation::Composite { .. } => Err(MigrateError::builder()
                .kind(MigrateErrorKind::AlterTypeInvalidOp(Box::new(
                    other.operation.clone(),
                )))
                .statement_a(Statement::CreateType {
                    name,
                    representation,
                })
                .statement_b(Statement::AlterType(other.clone()))
                .build()),
        },
    }
}
