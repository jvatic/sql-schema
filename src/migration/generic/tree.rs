use crate::{
    ast::{
        AlterTable, CreateDomain, CreateExtension, CreateIndex, CreateTable, CreateType,
        DropExtension, ObjectType, Statement,
    },
    migration::{MigrateError, MigrateErrorKind, Result, StatementMigrator, TreeMigrator},
};

pub fn migrate_tree<Dialect: TreeMigrator>(
    dialect: &Dialect,
    a: Vec<Statement>,
    b: &[Statement],
) -> Result<Vec<Statement>> {
    let next = a
        .into_iter()
        // perform any transformations on existing schema (e.g. ALTER/DROP table)
        .map(|sa| match &sa {
            Statement::CreateTable(a) => dialect.match_and_migrate_create_table(&sa, a, b),
            Statement::CreateIndex(a) => dialect.match_and_migrate_create_index(&sa, a, b),
            Statement::CreateType {
                name,
                representation,
            } => dialect.match_and_migrate_create_type(
                &sa,
                &CreateType {
                    name: name.clone(),
                    representation: representation.clone(),
                },
                b,
            ),
            Statement::CreateExtension(a) => dialect.match_and_migrate_create_extension(&sa, a, b),
            Statement::CreateDomain(a) => dialect.match_and_migrate_create_domain(&sa, a, b),
            _ => Err(MigrateError::builder()
                .kind(MigrateErrorKind::NotImplemented)
                .statement_a(sa.clone())
                .build()),
        })
        // CREATE table etc.
        .chain(b.iter().filter_map(|sb| match sb {
            Statement::CreateTable(_)
            | Statement::CreateIndex { .. }
            | Statement::CreateType { .. }
            | Statement::CreateExtension { .. }
            | Statement::CreateDomain(..) => Some(Ok(vec![sb.clone()])),
            _ => None,
        }))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    Ok(next)
}

fn match_and_migrate<Dialect, MF>(
    dialect: &Dialect,
    sa: &Statement,
    b: &[Statement],
    match_fn: MF,
) -> Result<Vec<Statement>>
where
    Dialect: StatementMigrator,
    MF: Fn(&&Statement) -> bool,
{
    b.iter().find(match_fn).map_or_else(
        // keep the statement as-is if there's no counterpart
        || Ok(vec![sa.clone()]),
        // otherwise diff the two statements
        |sb| StatementMigrator::migrate(dialect, sa, sb),
    )
}

pub fn match_and_migrate_create_table<Dialect: TreeMigrator>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateTable,
    b: &[Statement],
) -> Result<Vec<Statement>> {
    match_and_migrate(dialect, sa, b, |sb| match sb {
        Statement::AlterTable(AlterTable { name, .. }) => *name == a.name,
        Statement::Drop {
            object_type, names, ..
        } => *object_type == ObjectType::Table && names.len() == 1 && names[0] == a.name,
        _ => false,
    })
}

pub fn match_and_migrate_create_index<Dialect: TreeMigrator>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateIndex,
    b: &[Statement],
) -> Result<Vec<Statement>> {
    match_and_migrate(dialect, sa, b, |sb| match sb {
        Statement::Drop {
            object_type, names, ..
        } => {
            *object_type == ObjectType::Index
                && names.len() == 1
                && Some(&names[0]) == a.name.as_ref()
        }
        _ => false,
    })
}

pub fn match_and_migrate_create_type<Dialect: TreeMigrator>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateType,
    b: &[Statement],
) -> Result<Vec<Statement>> {
    match_and_migrate(dialect, sa, b, |sb| match sb {
        Statement::AlterType(b) => a.name == b.name,
        Statement::Drop {
            object_type, names, ..
        } => *object_type == ObjectType::Type && names.len() == 1 && names[0] == a.name,
        _ => false,
    })
}

pub fn match_and_migrate_create_extension<Dialect: TreeMigrator>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateExtension,
    b: &[Statement],
) -> Result<Vec<Statement>> {
    match_and_migrate(dialect, sa, b, |sb| match sb {
        Statement::DropExtension(DropExtension { names, .. }) => names.contains(&a.name),
        _ => false,
    })
}

pub fn match_and_migrate_create_domain<Dialect: TreeMigrator>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateDomain,
    b: &[Statement],
) -> Result<Vec<Statement>> {
    match_and_migrate(dialect, sa, b, |sb| match sb {
        Statement::DropDomain(b) => a.name == b.name,
        _ => false,
    })
}
