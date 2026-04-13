use crate::sealed::Sealed;

#[derive(Debug, Default, Clone)]
pub struct Generic;

#[derive(Debug, Default, Clone)]
pub struct PostgreSQL;

#[derive(Debug, Default, Clone)]
pub struct SQLite;

impl Sealed for Generic {}
impl Sealed for PostgreSQL {}
impl Sealed for SQLite {}
