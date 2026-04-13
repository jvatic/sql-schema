use crate::{
    ast::{
        AlterColumnOperation, AlterTable, AlterTableOperation, AlterType,
        AlterTypeAddValuePosition, AlterTypeOperation, ColumnOption, ColumnOptionDef, CreateDomain,
        CreateExtension, CreateIndex, CreateTable, CreateType, GeneratedAs, ObjectName,
        ObjectNamePart, ObjectType, Statement, UserDefinedTypeRepresentation,
    },
    migration::{MigrateError, MigrateErrorKind, Result, StatementMigrator},
};

pub fn migrate<Dialect: StatementMigrator>(
    dialect: &Dialect,
    sa: &Statement,
    sb: &Statement,
) -> Result<Vec<Statement>> {
    match sa {
        Statement::CreateTable(a) => dialect.migrate_create_table(a, sb),
        Statement::CreateIndex(a) => dialect.migrate_create_index(a, sb),
        Statement::CreateType {
            name,
            representation,
        } => dialect.migrate_create_type(
            &CreateType {
                name: name.clone(),
                representation: representation.clone(),
            },
            sb,
        ),
        Statement::CreateExtension(a) => dialect.migrate_create_extension(a, sb),
        Statement::CreateDomain(a) => dialect.migrate_create_domain(a, sb),
        _ => Err(MigrateError::builder()
            .kind(MigrateErrorKind::NotImplemented)
            .statement_a(sa.clone())
            .statement_b(sb.clone())
            .build()),
    }
}

pub fn migrate_create_table<Dialect: StatementMigrator>(
    dialect: &Dialect,
    a: &CreateTable,
    sb: &Statement,
) -> Result<Vec<Statement>> {
    match &sb {
        Statement::AlterTable(b) => dialect.migrate_alter_table(a, b),
        Statement::Drop {
            object_type, names, ..
        } => {
            assert_eq!(
                *object_type,
                ObjectType::Table,
                "attempt to apply non-table DROP to {}",
                a.name
            );
            assert!(
                names.contains(&a.name),
                "attempt to apply DROP {:?} to {}",
                names,
                a.name
            );
            Ok(Vec::with_capacity(0))
        }
        _ => Err(MigrateError::builder()
            .kind(MigrateErrorKind::NotImplemented)
            .statement_a(Statement::CreateTable(a.clone()))
            .statement_b(sb.clone())
            .build()),
    }
}

pub fn migrate_create_index<Dialect: StatementMigrator>(
    _dialect: &Dialect,
    a: &CreateIndex,
    sb: &Statement,
) -> Result<Vec<Statement>> {
    match sb {
        Statement::Drop {
            object_type, names, ..
        } => {
            let name = a
                .name
                .clone()
                .expect("index must be named to apply drop statement");
            assert_eq!(
                *object_type,
                ObjectType::Index,
                "attempt to apply non-index DROP to index {name}"
            );
            assert!(
                names.contains(&name),
                "attempt to apply DROP index {names:?} to {name}"
            );
            Ok(Vec::with_capacity(0))
        }
        _ => Err(MigrateError::builder()
            .kind(MigrateErrorKind::NotImplemented)
            .statement_a(Statement::CreateIndex(a.clone()))
            .statement_b(sb.clone())
            .build()),
    }
}

pub fn migrate_create_type<Dialect: StatementMigrator>(
    dialect: &Dialect,
    a: &CreateType,
    sb: &Statement,
) -> Result<Vec<Statement>> {
    match sb {
        Statement::AlterType(b) => dialect.migrate_alter_type(a, b),
        Statement::Drop {
            object_type, names, ..
        } => {
            assert_eq!(
                *object_type,
                ObjectType::Type,
                "attempt to apply non-type DROP to TYPE {}",
                a.name
            );
            assert!(
                names.contains(&a.name),
                "attempt to apply DROP {names:?} to {}",
                a.name
            );

            Ok(Vec::with_capacity(0))
        }
        _ => Err(MigrateError::builder()
            .kind(MigrateErrorKind::NotImplemented)
            .statement_a(a.clone().into())
            .statement_b(sb.clone())
            .build()),
    }
}

pub fn migrate_create_extension<Dialect: StatementMigrator>(
    _dialect: &Dialect,
    _a: &CreateExtension,
    _b: &Statement,
) -> Result<Vec<Statement>> {
    todo!()
}

pub fn migrate_create_domain<Dialect: StatementMigrator>(
    _dialect: &Dialect,
    _a: &CreateDomain,
    _b: &Statement,
) -> Result<Vec<Statement>> {
    todo!()
}

pub fn migrate_alter_table<Dialect: StatementMigrator>(
    _dialect: &Dialect,
    a: &CreateTable,
    b: &AlterTable,
) -> Result<Vec<Statement>, MigrateError> {
    assert_eq!(
        a.name, b.name,
        "attempt to apply ALTER TABLE {} to {}",
        b.name, a.name
    );

    let mut a = a.clone();
    for op in b.operations.iter() {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                a.columns.push(column_def.clone());
            }
            AlterTableOperation::DropColumn { column_names, .. } => {
                a.columns
                    .retain(|c| !column_names.iter().any(|name| c.name.value == name.value));
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                a.columns.iter_mut().for_each(|c| {
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
                            using: _,   // not applicable since we're not running the query
                            had_set: _, // this doesn't change the meaning
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
                                    generated_as: (*generated_as).unwrap_or(GeneratedAs::Always),
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
                    .statement_a(Statement::CreateTable(a.clone()))
                    .build())
            }
        }
    }

    Ok(vec![Statement::CreateTable(a)])
}

pub fn migrate_alter_type<Dialect: StatementMigrator>(
    _dialect: &Dialect,
    a: &CreateType,
    b: &AlterType,
) -> Result<Vec<Statement>, MigrateError> {
    assert_eq!(
        a.name, b.name,
        "attempt to apply ALTER TYPE {} to {}",
        b.name, a.name
    );

    let (name, representation) = match &b.operation {
        AlterTypeOperation::Rename(r) => {
            let mut parts = a.name.0.clone();
            parts.pop();
            parts.push(ObjectNamePart::Identifier(r.new_name.clone()));
            let name = ObjectName(parts);

            Ok((name, a.representation.clone()))
        }
        AlterTypeOperation::AddValue(av) => match &a.representation {
            Some(UserDefinedTypeRepresentation::Enum { labels }) => {
                let mut labels = labels.clone();
                match &av.position {
                    Some(AlterTypeAddValuePosition::Before(before_name)) => {
                        let index = labels
                            .iter()
                            .enumerate()
                            .find(|(_, l)| *l == before_name)
                            .map(|(i, _)| i)
                            // insert at the beginning if `before_name` can't be found
                            .unwrap_or(0);
                        labels.insert(index, av.value.clone());
                    }
                    Some(AlterTypeAddValuePosition::After(after_name)) => {
                        let index = labels
                            .iter()
                            .enumerate()
                            .find(|(_, l)| *l == after_name)
                            .map(|(i, _)| i + 1);
                        match index {
                            Some(index) => labels.insert(index, av.value.clone()),
                            // push it to the end if `after_name` can't be found
                            None => labels.push(av.value.clone()),
                        }
                    }
                    None => labels.push(av.value.clone()),
                }

                Ok((
                    a.name.clone(),
                    Some(UserDefinedTypeRepresentation::Enum { labels }),
                ))
            }
            Some(_) | None => Err(MigrateError::builder()
                .kind(MigrateErrorKind::AlterTypeInvalidOp(Box::new(
                    b.operation.clone(),
                )))
                .statement_a(a.clone().into())
                .statement_b(Statement::AlterType(b.clone()))
                .build()),
        },
        AlterTypeOperation::RenameValue(rv) => match &a.representation {
            Some(UserDefinedTypeRepresentation::Enum { labels }) => {
                let labels = labels
                    .iter()
                    .cloned()
                    .map(|l| if l == rv.from { rv.to.clone() } else { l })
                    .collect::<Vec<_>>();

                Ok((
                    a.name.clone(),
                    Some(UserDefinedTypeRepresentation::Enum { labels }),
                ))
            }
            Some(_) | None => Err(MigrateError::builder()
                .kind(MigrateErrorKind::AlterTypeInvalidOp(Box::new(
                    b.operation.clone(),
                )))
                .statement_a(a.clone().into())
                .statement_b(Statement::AlterType(b.clone()))
                .build()),
        },
    }?;
    Ok(vec![Statement::CreateType {
        name,
        representation,
    }])
}
