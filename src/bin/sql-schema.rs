use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    process::{self},
    time::SystemTime,
};

use anyhow::{anyhow, Context};
use camino::{Utf8DirEntry, Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use sql_schema::{
    path_template::{PathTemplate, TemplateData, UpDown},
    Dialect, SyntaxTree,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

const DEFAULT_MIGRATIONS_DIR: &str = "./schema/migrations";
const DEFAULT_SCHEMA_PATH: &str = "./schema/schema.sql";

#[derive(Debug, Subcommand)]
enum Commands {
    /// generate a new schema
    Schema(SchemaCommand),
    /// generate a new migration
    Migration(MigrationCommand),
}

#[derive(Parser, Debug)]
struct SchemaCommand {
    /// path to schema file
    #[arg(short, long, default_value_t = Utf8PathBuf::from(DEFAULT_SCHEMA_PATH))]
    schema_path: Utf8PathBuf,
    /// path to migrations directory
    #[arg(short, long, default_value_t = Utf8PathBuf::from(DEFAULT_MIGRATIONS_DIR))]
    migrations_dir: Utf8PathBuf,
    /// dialect of SQL to use
    #[arg(short, long, default_value_t = Dialect::Generic)]
    dialect: Dialect,
}

#[derive(Parser, Debug)]
struct MigrationCommand {
    /// path to schema file
    #[arg(short, long, default_value_t = Utf8PathBuf::from(DEFAULT_SCHEMA_PATH))]
    schema_path: Utf8PathBuf,
    /// path to migrations directory
    #[arg(short, long, default_value_t = Utf8PathBuf::from(DEFAULT_MIGRATIONS_DIR))]
    migrations_dir: Utf8PathBuf,
    /// dialect of SQL to use
    #[arg(short, long, default_value_t = Dialect::Generic)]
    dialect: Dialect,
    /// name of migration
    #[arg(short, long, default_value = "generated_migration")]
    name: String,
    /// creates both an up and down migration when true
    ///
    /// default is to match the pattern in the migrations dir
    #[arg(long)]
    include_down: Option<bool>,
}

#[derive(Debug, Default)]
struct MigrationOptions {
    path_template: PathTemplate,
    include_down: bool,
}

impl MigrationOptions {
    fn reconcile(self, cmd: &MigrationCommand) -> Self {
        let include_down = if let Some(include_down) = cmd.include_down {
            include_down
        } else {
            self.include_down
        };
        let path_template = self.path_template;
        Self {
            include_down,
            path_template,
        }
    }
}

fn main() {
    let args = Args::parse();

    if let Err(err) = match args.command {
        Commands::Schema(command) => run_schema(command).context("schema"),
        Commands::Migration(command) => run_migration(command).context("migration"),
    } {
        eprintln!("Error: {err:?}");
        process::exit(1);
    }
}

/// create or update schema file from migrations
fn run_schema(command: SchemaCommand) -> anyhow::Result<()> {
    ensure_schema_file(&command.schema_path)?;
    ensure_migration_dir(&command.migrations_dir)?;

    let (migrations, _) = parse_migrations(command.dialect, &command.migrations_dir)?;
    let schema = parse_sql_file(command.dialect, &command.schema_path)?;
    let diff = schema.diff(&migrations)?.unwrap_or_else(SyntaxTree::empty);
    let schema = schema.migrate(&diff)?.unwrap_or_else(SyntaxTree::empty);
    eprintln!("writing {}", command.schema_path);
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&command.schema_path)?
        .write_all(schema.to_string().as_bytes())?;
    Ok(())
}

/// create a new migration from edits to schema file
fn run_migration(command: MigrationCommand) -> anyhow::Result<()> {
    ensure_schema_file(&command.schema_path)?;
    ensure_migration_dir(&command.migrations_dir)?;

    let (migrations, opts) = parse_migrations(command.dialect, &command.migrations_dir)?;
    let opts = opts.reconcile(&command);
    let schema = parse_sql_file(command.dialect, &command.schema_path)?;
    match migrations.diff(&schema)? {
        Some(up_migration) => {
            let path_data = TemplateData {
                timestamp: DateTime::<Utc>::from(SystemTime::now()),
                name: command.name.clone(),
                up_down: if opts.include_down {
                    Some(UpDown::Up)
                } else {
                    None
                },
                ..Default::default()
            };

            let path_template = if opts.include_down {
                // ensure template includes an UpDown token
                opts.path_template.with_up_down()
            } else {
                opts.path_template
            };

            let up_path = command
                .migrations_dir
                .join(path_template.resolve(&path_data));

            if opts.include_down {
                let down_migration = schema
                    .diff(&migrations)
                    .inspect_err(|err| eprintln!("WARNING: error creating down migration: {err}"))
                    .unwrap_or(None)
                    .unwrap_or_else(SyntaxTree::empty);

                let path_data = TemplateData {
                    up_down: Some(UpDown::Down),
                    ..path_data
                };
                let down_path = command
                    .migrations_dir
                    .join(path_template.resolve(&path_data));

                write_migration(up_migration, &up_path)?;
                write_migration(down_migration, &down_path)
            } else {
                write_migration(up_migration, &up_path)
            }
        }
        None => {
            eprintln!("existing migrations and the schema file are the same");
            Ok(())
        }
    }
}

fn write_migration(migration: SyntaxTree, path: &Utf8Path) -> anyhow::Result<()> {
    eprintln!("writing {path}");
    if let Some(parent) = path.parent() {
        eprintln!("creating {parent}");
        ensure_migration_dir(parent)?;
    }
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?
        .write_all(migration.to_string().as_bytes())?;
    Ok(())
}

fn ensure_schema_file(path: &Utf8Path) -> anyhow::Result<()> {
    if !path.try_exists()? {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        eprintln!("creating {path}");
        File::create(path)?;
    }
    let meta = fs::metadata(path)?;
    if !meta.is_file() {
        return Err(anyhow!("schema path must be a file"));
    }
    Ok(())
}

fn ensure_migration_dir(dir: &Utf8Path) -> anyhow::Result<()> {
    if !dir.try_exists()? {
        fs::create_dir_all(dir)?;
    }
    Ok(())
}

fn parse_sql_file(dialect: Dialect, path: &Utf8Path) -> anyhow::Result<SyntaxTree> {
    let data = fs::read_to_string(path)?;
    SyntaxTree::builder()
        .dialect(dialect)
        .sql(data.as_str())
        .build()
        .context(format!("path: {path}"))
}

/// builds a [SyntaxTree] by applying each migration in order
fn parse_migrations(
    dialect: Dialect,
    dir: &Utf8Path,
) -> anyhow::Result<(SyntaxTree, MigrationOptions)> {
    fn process_dir_entry(
        entry: io::Result<Utf8DirEntry>,
    ) -> anyhow::Result<Option<Vec<Utf8PathBuf>>> {
        let entry = entry?;
        let meta = entry.metadata()?;
        let path: Utf8PathBuf = entry.path().into();
        // step into any dir we encounter
        if meta.is_dir() {
            let res = entry
                .into_path()
                .read_dir_utf8()?
                .map(process_dir_entry)
                .collect::<anyhow::Result<Vec<Option<_>>>>()
                .map(|e| Some(e.into_iter().flatten().flatten().collect::<Vec<_>>()));
            return res;
        }
        // skip over non-file entries
        if !meta.is_file() {
            return Ok(None);
        }
        // skip over non-sql files
        match path.extension() {
            Some("sql") => {}
            _ => {
                eprintln!("skipping {path}");
                return Ok(None);
            }
        };
        let stem = path
            .file_stem()
            .ok_or_else(|| anyhow!("{:?} is missing a name", path))?;
        // skip over "down" migrations
        if stem.ends_with(".down") || stem.ends_with(".undo") || stem == "down" || stem == "undo" {
            eprintln!("skipping {path}");
            return Ok(None);
        }

        Ok(Some(vec![path]))
    }

    let mut migrations = dir
        .read_dir_utf8()?
        .map(process_dir_entry)
        .collect::<anyhow::Result<Vec<Option<_>>>>()?
        .into_iter()
        .flatten()
        .flatten()
        .collect::<Vec<_>>();
    migrations.sort();
    let path_template = match migrations.last() {
        Some(path) => {
            let path = path.strip_prefix(dir)?;
            PathTemplate::parse(path.as_str()).context(format!("path: {path}"))?
        }
        None => PathTemplate::default(),
    };
    let opts = MigrationOptions {
        include_down: path_template.includes_up_down(),
        path_template,
    };
    let tree =
        migrations
            .iter()
            .try_fold(SyntaxTree::empty(), |schema, path| -> anyhow::Result<_> {
                eprintln!("parsing {path}");
                let migration = parse_sql_file(dialect, path)?;
                let schema = schema
                    .migrate(&migration)?
                    .unwrap_or_else(SyntaxTree::empty);
                Ok(schema)
            })?;
    Ok((tree, opts))
}
