use std::fmt;

use bon::bon;
use thiserror::Error;

use crate::{
    ast::{CreateDomain, CreateExtension, CreateIndex, CreateTable, CreateType, Statement},
    dialect::{Generic, PostgreSQL, SQLite},
    sealed::Sealed,
};

pub mod generic;

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
    pub(crate) fn new(
        kind: DiffErrorKind,
        #[builder(into)] statement_a: Option<Statement>,
        #[builder(into)] statement_b: Option<Statement>,
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
pub enum DiffErrorKind {
    #[error("can't drop unnamed index")]
    DropUnnamedIndex,
    #[error("can't compare unnamed index")]
    CompareUnnamedIndex,
    #[error("removing enum labels is not supported")]
    RemoveEnumLabel,
    #[error("not yet supported")]
    NotImplemented,
}

pub type Result<T, E = DiffError> = std::result::Result<T, E>;

pub trait TreeDiffer: StatementDiffer + Sealed {
    fn diff_tree(&self, a: &[Statement], b: &[Statement]) -> Result<Option<Vec<Statement>>> {
        generic::tree::tree_diff(self, a, b)
    }

    fn find_and_compare_create_table(
        &self,
        sa: &Statement,
        a: &CreateTable,
        b: &[Statement],
    ) -> Result<Option<Vec<Statement>>> {
        generic::tree::find_and_compare_create_table(self, sa, a, b)
    }

    fn find_and_compare_create_index(
        &self,
        sa: &Statement,
        a: &CreateIndex,
        b: &[Statement],
    ) -> Result<Option<Vec<Statement>>> {
        generic::tree::find_and_compare_create_index(self, sa, a, b)
    }

    fn find_and_compare_create_type(
        &self,
        sa: &Statement,
        a: &CreateType,
        b: &[Statement],
    ) -> Result<Option<Vec<Statement>>> {
        generic::tree::find_and_compare_create_type(self, sa, a, b)
    }

    fn find_and_compare_create_extension(
        &self,
        sa: &Statement,
        a: &CreateExtension,
        b: &[Statement],
    ) -> Result<Option<Vec<Statement>>> {
        generic::tree::find_and_compare_create_extension(self, sa, a, b)
    }

    fn find_and_compare_create_domain(
        &self,
        sa: &Statement,
        a: &CreateDomain,
        b: &[Statement],
    ) -> Result<Option<Vec<Statement>>> {
        generic::tree::find_and_compare_create_domain(self, sa, a, b)
    }
}

impl TreeDiffer for Generic {}

impl TreeDiffer for PostgreSQL {}

impl TreeDiffer for SQLite {}

pub trait StatementDiffer: fmt::Debug + Default + Clone + Sized + Sealed {
    fn diff(&self, sa: &Statement, sb: &Statement) -> Result<Option<Vec<Statement>>> {
        generic::statement::diff(self, sa, sb)
    }

    fn compare_create_table(
        &self,
        a: &CreateTable,
        b: &CreateTable,
    ) -> Result<Option<Vec<Statement>>> {
        generic::statement::compare_create_table(a, b)
    }

    fn compare_create_index(
        &self,
        a: &CreateIndex,
        b: &CreateIndex,
    ) -> Result<Option<Vec<Statement>>> {
        generic::statement::compare_create_index(a, b)
    }

    fn compare_create_type(
        &self,
        a: &CreateType,
        b: &CreateType,
    ) -> Result<Option<Vec<Statement>>> {
        generic::statement::compare_create_type(a, b)
    }

    fn compare_create_domain(
        &self,
        a: &CreateDomain,
        b: &CreateDomain,
    ) -> Result<Option<Vec<Statement>>> {
        generic::statement::compare_create_domain(a, b)
    }
}

impl StatementDiffer for Generic {}

impl StatementDiffer for PostgreSQL {}

impl StatementDiffer for SQLite {}
