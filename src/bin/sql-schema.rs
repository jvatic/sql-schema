use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use sql_schema::SyntaxTree;

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
}

#[derive(Parser, Debug)]
struct MigrationCommand {
    /// path to schema file
    #[arg(short, long, default_value_t = Utf8PathBuf::from(DEFAULT_SCHEMA_PATH))]
    schema_path: Utf8PathBuf,
    /// path to migrations directory
    #[arg(short, long, default_value_t = Utf8PathBuf::from(DEFAULT_MIGRATIONS_DIR))]
    migrations_dir: Utf8PathBuf,
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
    include_down: bool,
}

impl MigrationOptions {
    fn reconcile(self, cmd: &MigrationCommand) -> Self {
        let include_down = if let Some(include_down) = cmd.include_down {
            include_down
        } else {
            self.include_down
        };
        Self { include_down }
    }
}

fn main() {
    let args = Args::parse();

    if let Err(err) = match args.command {
        Commands::Schema(command) => run_schema(command).context("schema"),
        Commands::Migration(command) => run_migration(command).context("migration"),
    } {
        panic!("Error: {:?}", err)
    }
}

/// create or update schema file from migrations
fn run_schema(command: SchemaCommand) -> anyhow::Result<()> {
    ensure_schema_file(&command.schema_path)?;
    ensure_migration_dir(&command.migrations_dir)?;

    let (migrations, _) = parse_migrations(&command.migrations_dir)?;
    let schema = parse_sql_file(&command.schema_path)?;
    let diff = schema.diff(&migrations).unwrap_or_else(SyntaxTree::empty);
    let schema = schema.migrate(&diff).unwrap_or_else(SyntaxTree::empty);
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

    let (migrations, opts) = parse_migrations(&command.migrations_dir)?;
    let opts = opts.reconcile(&command);
    let schema = parse_sql_file(&command.schema_path)?;
    match migrations.diff(&schema) {
        Some(up_migration) => {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_secs();
            if opts.include_down {
                let down_migration = up_migration
                    .diff(&SyntaxTree::empty())
                    .unwrap_or_else(SyntaxTree::empty);

                write_migration(
                    up_migration,
                    timestamp,
                    &command.name,
                    "up.sql",
                    &command.migrations_dir,
                )?;

                write_migration(
                    down_migration,
                    timestamp,
                    &command.name,
                    "down.sql",
                    &command.migrations_dir,
                )
            } else {
                write_migration(
                    up_migration,
                    timestamp,
                    &command.name,
                    "sql",
                    &command.migrations_dir,
                )
            }
        }
        None => {
            eprintln!("existing migrations and the schema file are the same");
            Ok(())
        }
    }
}

fn write_migration(
    migration: SyntaxTree,
    timestamp: u64,
    name: &str,
    ext: &str,
    dir: &Utf8Path,
) -> anyhow::Result<()> {
    let filename = Utf8PathBuf::from(format!("{timestamp}_{name}.{ext}"));
    let path = dir.join(filename);
    eprintln!("writing {path}");
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

fn parse_sql_file(path: &Utf8Path) -> anyhow::Result<SyntaxTree> {
    let data = fs::read_to_string(path)?;
    Ok(SyntaxTree::builder().sql(data.as_str()).build()?)
}

/// builds a [SyntaxTree] by applying each migration in order
fn parse_migrations(dir: &Utf8Path) -> anyhow::Result<(SyntaxTree, MigrationOptions)> {
    let mut opts = MigrationOptions::default();
    let mut migrations = dir
        .read_dir_utf8()?
        .map(|entry| -> anyhow::Result<_> {
            let entry = entry?;
            let meta = entry.metadata()?;
            let path: Utf8PathBuf = entry.path().into();
            // skip over non-file entries
            if !meta.is_file() {
                eprintln!("skipping {path}");
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
            if stem.ends_with(".down") {
                eprintln!("skipping {path}");
                opts.include_down = true;
                return Ok(None);
            }

            Ok(Some(path))
        })
        .collect::<anyhow::Result<Vec<Option<_>>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    migrations.sort();
    migrations
        .into_iter()
        .try_fold(SyntaxTree::empty(), |schema, path| -> anyhow::Result<_> {
            eprintln!("parsing {path}");
            let migration = parse_sql_file(&path)?;
            let schema = schema.migrate(&migration).unwrap_or_else(SyntaxTree::empty);
            Ok(schema)
        })
        .map(|t| (t, opts))
}
