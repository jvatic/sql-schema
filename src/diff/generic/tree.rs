use crate::{
    ast::{
        CreateDomain, CreateExtension, CreateIndex, CreateTable, CreateType, DropDomain,
        DropExtension, Statement,
    },
    diff::{DiffError, DiffErrorKind, Result, StatementDiffer, TreeDiffer},
};

pub fn tree_diff<Dialect>(
    dialect: &Dialect,
    a: &[Statement],
    b: &[Statement],
) -> Result<Option<Vec<Statement>>>
where
    Dialect: TreeDiffer,
{
    let res = a
        .iter()
        .filter_map(|sa| {
            match sa {
                // CreateTable: compare against another CreateTable with the same name
                // TODO: handle renames (e.g. use comments to tag a previous name for a table in a schema)
                Statement::CreateTable(a) => dialect.find_and_compare_create_table(sa, a, b),
                Statement::CreateIndex(a) => dialect.find_and_compare_create_index(sa, a, b),
                Statement::CreateType {
                    name,
                    representation,
                } => dialect.find_and_compare_create_type(
                    sa,
                    &CreateType {
                        name: name.clone(),
                        representation: representation.clone(),
                    },
                    b,
                ),
                Statement::CreateExtension(sb) => {
                    dialect.find_and_compare_create_extension(sa, sb, b)
                }
                Statement::CreateDomain(a) => dialect.find_and_compare_create_domain(sa, a, b),
                _ => Err(DiffError::builder()
                    .kind(DiffErrorKind::NotImplemented)
                    .statement_a(sa.clone())
                    .build()),
            }
            .transpose()
        })
        // find resources that are in `other` but not in `a`
        .chain(b.iter().filter_map(|sb| {
            match sb {
                Statement::CreateTable(b) => Ok(a.iter().find(|sa| match sa {
                    Statement::CreateTable(a) => a.name == b.name,
                    _ => false,
                })),
                Statement::CreateIndex(b) => Ok(a.iter().find(|sa| match sa {
                    Statement::CreateIndex(a) => a.name == b.name,
                    _ => false,
                })),
                Statement::CreateType { name: b_name, .. } => Ok(a.iter().find(|sa| match sa {
                    Statement::CreateType { name: a_name, .. } => a_name == b_name,
                    _ => false,
                })),
                Statement::CreateExtension(CreateExtension { name: b_name, .. }) => {
                    Ok(a.iter().find(|sa| match sa {
                        Statement::CreateExtension(CreateExtension { name: a_name, .. }) => {
                            a_name == b_name
                        }
                        _ => false,
                    }))
                }
                Statement::CreateDomain(b) => Ok(a.iter().find(|sa| match sa {
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

fn find_and_compare<Dialect, MF, DF>(
    dialect: &Dialect,
    sa: &Statement,
    b: &[Statement],
    match_fn: MF,
    drop_fn: DF,
) -> Result<Option<Vec<Statement>>>
where
    Dialect: StatementDiffer,
    MF: Fn(&&Statement) -> bool,
    DF: Fn() -> Result<Option<Vec<Statement>>>,
{
    b.iter().find(match_fn).map_or_else(
        // drop the statement if it wasn't found in `other`
        drop_fn,
        // otherwise diff the two statements
        |sb| StatementDiffer::diff(dialect, sa, sb),
    )
}

pub fn find_and_compare_create_table<Dialect>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateTable,
    b: &[Statement],
) -> Result<Option<Vec<Statement>>>
where
    Dialect: StatementDiffer,
{
    find_and_compare(
        dialect,
        sa,
        b,
        |sb| match sb {
            Statement::CreateTable(b) => a.name == b.name,
            _ => false,
        },
        || {
            Ok(Some(vec![Statement::Drop {
                object_type: crate::ast::ObjectType::Table,
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

pub fn find_and_compare_create_index<Dialect>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateIndex,
    b: &[Statement],
) -> Result<Option<Vec<Statement>>>
where
    Dialect: StatementDiffer,
{
    find_and_compare(
        dialect,
        sa,
        b,
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
                object_type: crate::ast::ObjectType::Index,
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

pub fn find_and_compare_create_type<Dialect>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateType,
    b: &[Statement],
) -> Result<Option<Vec<Statement>>>
where
    Dialect: StatementDiffer,
{
    let a_name = &a.name;
    find_and_compare(
        dialect,
        sa,
        b,
        |sb| match sb {
            Statement::CreateType { name: b_name, .. } => a_name == b_name,
            _ => false,
        },
        || {
            Ok(Some(vec![Statement::Drop {
                object_type: crate::ast::ObjectType::Type,
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

pub fn find_and_compare_create_extension<Dialect>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateExtension,
    b: &[Statement],
) -> Result<Option<Vec<Statement>>>
where
    Dialect: StatementDiffer,
{
    let a_name = &a.name;
    let if_not_exists = a.if_not_exists;
    let cascade = a.cascade;

    find_and_compare(
        dialect,
        sa,
        b,
        |sb| match sb {
            Statement::CreateExtension(CreateExtension { name: b_name, .. }) => a_name == b_name,
            _ => false,
        },
        || {
            Ok(Some(vec![Statement::DropExtension(DropExtension {
                names: vec![a_name.clone()],
                if_exists: if_not_exists,
                cascade_or_restrict: if cascade {
                    Some(crate::ast::ReferentialAction::Cascade)
                } else {
                    None
                },
            })]))
        },
    )
}

pub fn find_and_compare_create_domain<Dialect>(
    dialect: &Dialect,
    sa: &Statement,
    a: &CreateDomain,
    b: &[Statement],
) -> Result<Option<Vec<Statement>>>
where
    Dialect: StatementDiffer,
{
    find_and_compare(
        dialect,
        sa,
        b,
        |sb| match sb {
            Statement::CreateDomain(b) => b.name == a.name,
            _ => false,
        },
        || {
            Ok(Some(vec![Statement::DropDomain(DropDomain {
                name: a.name.clone(),
                if_exists: false,
                drop_behavior: None,
            })]))
        },
    )
}
