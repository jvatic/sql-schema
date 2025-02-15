use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnOption, ColumnOptionDef, CreateTable,
    GeneratedAs, ObjectType, Statement,
};

pub trait Migrate: Sized {
    fn migrate(self, other: &Self) -> Option<Self>;
}

impl Migrate for Vec<Statement> {
    fn migrate(self, other: &Self) -> Option<Self> {
        let mut next: Self = self
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
                        .map_or(Some(orig), |sb| sa.migrate(sb)),
                    _ => todo!("handle migrating statement: {:?}", other),
                }
            })
            // CREATE table etc.
            .chain(other.iter().filter_map(|sb| match sb {
                Statement::CreateTable(_) => Some(sb.clone()),
                _ => None,
            }))
            .collect();
        next.sort(); // TODO: does this do what we want?
        Some(next)
    }
}

impl Migrate for Statement {
    fn migrate(self, other: &Self) -> Option<Self> {
        match self {
            Self::CreateTable(ca) => match other {
                Self::AlterTable {
                    name, operations, ..
                } => {
                    if *name == ca.name {
                        Some(Self::CreateTable(migrate_alter_table(ca, operations)))
                    } else {
                        // ALTER TABLE statement for another table
                        Some(Self::CreateTable(ca))
                    }
                }
                Self::Drop {
                    object_type, names, ..
                } => {
                    if *object_type == ObjectType::Table && names.contains(&ca.name) {
                        None
                    } else {
                        // DROP statement is for another table
                        Some(Self::CreateTable(ca))
                    }
                }
                _ => todo!("handle migrating statement: {:?}", other),
            },
            _ => todo!("handle migrating statement: {:?}", other),
        }
    }
}

fn migrate_alter_table(mut t: CreateTable, ops: &[AlterTableOperation]) -> CreateTable {
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
            _ => todo!("handle alter table operation {:?}", op),
        }
    }

    t
}
