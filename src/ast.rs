pub use sqlparser::ast::{
    helpers::attached_token::AttachedToken, AlterColumnOperation, AlterTable, AlterTableOperation,
    AlterType, AlterTypeAddValue, AlterTypeAddValuePosition, AlterTypeOperation,
    AlterTypeRenameValue, ColumnDef, ColumnOption, ColumnOptionDef, CreateDomain, CreateExtension,
    CreateIndex, CreateTable, DropDomain, DropExtension, GeneratedAs, ObjectName, ObjectNamePart,
    ObjectType, ReferentialAction, RenameTableNameKind, Statement, UserDefinedTypeRepresentation,
};

/// This is a copy of [`Statement::CreateType`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CreateType {
    /// Type name to create.
    pub name: ObjectName,
    /// Optional type representation details.
    pub representation: Option<UserDefinedTypeRepresentation>,
}

impl From<CreateType> for Statement {
    fn from(value: CreateType) -> Self {
        Statement::CreateType {
            name: value.name,
            representation: value.representation,
        }
    }
}
