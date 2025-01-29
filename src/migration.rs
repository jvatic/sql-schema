use sqlparser::ast::{AlterTableOperation, CreateTable, ObjectType, Statement};

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
                            Statement::AlterTable {
                                name,
                                if_exists: _,
                                only: _,
                                operations: _,
                                location: _,
                                on_cluster: _,
                            } => *name == ca.name,
                            Statement::Drop {
                                object_type,
                                names,
                                if_exists: _,
                                cascade: _,
                                restrict: _,
                                purge: _,
                                temporary: _,
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
                    name,
                    operations,
                    if_exists: _,
                    only: _,
                    location: _,
                    on_cluster: _,
                } => {
                    if *name == ca.name {
                        Some(Self::CreateTable(migrate_alter_table(ca, operations)))
                    } else {
                        // ALTER TABLE statement for another table
                        Some(Self::CreateTable(ca))
                    }
                }
                Self::Drop {
                    object_type,
                    names,
                    if_exists: _,
                    cascade: _,
                    restrict: _,
                    purge: _,
                    temporary: _,
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
            AlterTableOperation::AddColumn {
                column_def,
                column_keyword: _,
                if_not_exists: _,
                column_position: _,
            } => {
                t.columns.push(column_def.clone());
            }
            AlterTableOperation::DropColumn {
                column_name,
                if_exists: _,
                drop_behavior: _,
            } => {
                t.columns.retain(|c| c.name != *column_name);
            }
            _ => todo!("handle alter table operation {:?}", op),
        }
    }

    t
}
