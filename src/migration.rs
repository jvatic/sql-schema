use std::fmt;

use bon::bon;
use sqlparser::ast::{CreateDomain, CreateIndex};
use thiserror::Error;

use crate::{
    ast::{
        AlterTable, AlterTableOperation, AlterType, AlterTypeOperation, CreateExtension,
        CreateTable, CreateType, Statement,
    },
    dialect::{Generic, PostgreSQL, SQLite},
    sealed::Sealed,
};

pub mod generic;

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
    #[error("ALTER TABLE operation \"{0}\" not yet supported")]
    AlterTableOpNotImplemented(Box<AlterTableOperation>),
    #[error("invalid ALTER TYPE operation \"{0}\"")]
    AlterTypeInvalidOp(Box<AlterTypeOperation>),
    #[error("not yet supported")]
    NotImplemented,
}

type Result<T, E = MigrateError> = std::result::Result<T, E>;

pub trait TreeMigrator: StatementMigrator + Sealed {
    fn migrate_tree(&self, a: Vec<Statement>, b: &[Statement]) -> Result<Vec<Statement>> {
        generic::tree::migrate_tree(self, a, b)
    }

    fn match_and_migrate_create_table(
        &self,
        sa: &Statement,
        a: &CreateTable,
        b: &[Statement],
    ) -> Result<Vec<Statement>> {
        generic::tree::match_and_migrate_create_table(self, sa, a, b)
    }

    fn match_and_migrate_create_index(
        &self,
        sa: &Statement,
        a: &CreateIndex,
        b: &[Statement],
    ) -> Result<Vec<Statement>> {
        generic::tree::match_and_migrate_create_index(self, sa, a, b)
    }

    fn match_and_migrate_create_type(
        &self,
        sa: &Statement,
        a: &CreateType,
        b: &[Statement],
    ) -> Result<Vec<Statement>> {
        generic::tree::match_and_migrate_create_type(self, sa, a, b)
    }

    fn match_and_migrate_create_extension(
        &self,
        sa: &Statement,
        a: &CreateExtension,
        b: &[Statement],
    ) -> Result<Vec<Statement>> {
        generic::tree::match_and_migrate_create_extension(self, sa, a, b)
    }

    fn match_and_migrate_create_domain(
        &self,
        sa: &Statement,
        a: &CreateDomain,
        b: &[Statement],
    ) -> Result<Vec<Statement>> {
        generic::tree::match_and_migrate_create_domain(self, sa, a, b)
    }
}

impl TreeMigrator for Generic {}

impl TreeMigrator for PostgreSQL {}

impl TreeMigrator for SQLite {}

pub trait StatementMigrator: fmt::Debug + Default + Clone + Sized + Sealed {
    fn migrate(&self, a: &Statement, b: &Statement) -> Result<Vec<Statement>> {
        generic::statement::migrate(self, a, b)
    }

    fn migrate_create_table(&self, a: &CreateTable, sb: &Statement) -> Result<Vec<Statement>> {
        generic::statement::migrate_create_table(self, a, sb)
    }

    fn migrate_alter_table(&self, a: &CreateTable, b: &AlterTable) -> Result<Vec<Statement>> {
        generic::statement::migrate_alter_table(self, a, b)
    }

    fn migrate_create_index(&self, a: &CreateIndex, sb: &Statement) -> Result<Vec<Statement>> {
        generic::statement::migrate_create_index(self, a, sb)
    }

    fn migrate_create_type(&self, a: &CreateType, sb: &Statement) -> Result<Vec<Statement>> {
        generic::statement::migrate_create_type(self, a, sb)
    }

    fn migrate_alter_type(&self, a: &CreateType, b: &AlterType) -> Result<Vec<Statement>> {
        generic::statement::migrate_alter_type(self, a, b)
    }

    fn migrate_create_extension(
        &self,
        a: &CreateExtension,
        sb: &Statement,
    ) -> Result<Vec<Statement>> {
        generic::statement::migrate_create_extension(self, a, sb)
    }

    fn migrate_create_domain(&self, a: &CreateDomain, sb: &Statement) -> Result<Vec<Statement>> {
        generic::statement::migrate_create_domain(self, a, sb)
    }
}

impl StatementMigrator for Generic {}

impl StatementMigrator for PostgreSQL {}

impl StatementMigrator for SQLite {}
