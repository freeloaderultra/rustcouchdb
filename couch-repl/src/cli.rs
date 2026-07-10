use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "couch-repl",
    version,
    about = "Standalone high-throughput CouchDB replicator"
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Replicate a source database to a target database
    Replicate(ReplicateArgs),
    /// Print the replication id and checkpoint document id for a job
    Id(IdArgs),
    /// Generate a benchmark dataset into a database
    Gen(GenArgs),
    /// Run as a daemon managing multiple replication jobs over an HTTP API
    Serve(ServeArgs),
}

#[derive(Args)]
pub struct ServeArgs {
    /// Listen address for the HTTP API
    #[arg(long, default_value = "127.0.0.1:7984")]
    pub listen: String,
    /// JSON file with jobs to start: {"jobs":[{...}]} or a bare array.
    /// Each job takes the same fields as POST /_jobs.
    #[arg(long)]
    pub config: Option<std::path::PathBuf>,
}

#[derive(Args)]
pub struct ReplicateArgs {
    /// Source database URL, e.g. https://user:pass@host:5984/db
    pub source: String,
    /// Target database URL
    pub target: String,

    /// Keep replicating new changes until interrupted
    #[arg(long)]
    pub continuous: bool,
    /// Create the target database if it does not exist
    #[arg(long)]
    pub create_target: bool,
    /// Override the start sequence (default: resume from checkpoint, else 0)
    #[arg(long)]
    pub since: Option<String>,
    /// Replicate only these doc ids (comma-separated, repeatable)
    #[arg(long, value_delimiter = ',')]
    pub doc_ids: Option<Vec<String>>,
    /// Replicate only docs matching this Mango selector (JSON), evaluated
    /// natively in couch-repl (never on the server, never through JS)
    #[arg(long)]
    pub selector: Option<String>,

    /// Concurrent _bulk_get requests in flight
    #[arg(long, default_value_t = 32)]
    pub fetch_concurrency: usize,
    /// Concurrent _bulk_docs requests in flight
    #[arg(long, default_value_t = 8)]
    pub write_concurrency: usize,
    /// Concurrent attachment-doc transfers
    #[arg(long, default_value_t = 16)]
    pub att_concurrency: usize,
    /// Docs per _bulk_get / _bulk_docs batch
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,
    /// Max bytes buffered per _bulk_docs batch
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    pub max_batch_bytes: usize,
    /// Docs whose total attachment size is at or below this ride the bulk
    /// path with inline base64 instead of streaming multipart
    #[arg(long, default_value_t = 65536)]
    pub inline_att_threshold: u64,
    /// Rows per _changes page in one-shot mode
    #[arg(long, default_value_t = 10000)]
    pub changes_limit: usize,

    /// Milliseconds between checkpoints
    #[arg(long, default_value_t = 30000)]
    pub checkpoint_interval: u64,
    /// Do not read or write checkpoints (always start from 0)
    #[arg(long)]
    pub no_checkpoints: bool,
    /// Never use _bulk_get; fetch docs individually
    #[arg(long)]
    pub no_bulk_get: bool,

    /// Per-request timeout in seconds (streams are exempt)
    #[arg(long, default_value_t = 60)]
    pub timeout: u64,
    /// Retries per failed request
    #[arg(long, default_value_t = 10)]
    pub max_retries: u32,
    /// Log and skip documents that fail permanently instead of aborting
    #[arg(long)]
    pub continue_on_error: bool,
    /// Extra header for source requests, "Name: value" (repeatable)
    #[arg(long)]
    pub source_header: Vec<String>,
    /// Extra header for target requests, "Name: value" (repeatable)
    #[arg(long)]
    pub target_header: Vec<String>,
    /// Skip TLS certificate verification
    #[arg(long)]
    pub insecure: bool,
    /// Seconds between progress lines
    #[arg(long, default_value_t = 5)]
    pub stats_interval: u64,
}

#[derive(Args)]
pub struct IdArgs {
    pub source: String,
    pub target: String,
    #[arg(long)]
    pub continuous: bool,
    #[arg(long, value_delimiter = ',')]
    pub doc_ids: Option<Vec<String>>,
    #[arg(long)]
    pub selector: Option<String>,
}

#[derive(Args)]
pub struct GenArgs {
    /// Database URL to fill (created if missing)
    pub db: String,
    /// Number of documents to create
    #[arg(long, default_value_t = 100_000)]
    pub docs: u64,
    /// Approximate JSON payload per doc, in KiB
    #[arg(long, default_value_t = 1)]
    pub doc_kb: usize,
    /// Attachments per doc
    #[arg(long, default_value_t = 0)]
    pub atts: usize,
    /// Size of each attachment, in KiB
    #[arg(long, default_value_t = 100)]
    pub att_kb: usize,
    /// Doc id prefix
    #[arg(long, default_value = "doc")]
    pub prefix: String,
    /// First doc index (to extend an existing dataset)
    #[arg(long, default_value_t = 0)]
    pub start: u64,
    /// Docs per _bulk_docs insert
    #[arg(long, default_value_t = 1000)]
    pub batch: usize,
    /// Concurrent insert requests
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
    /// Skip TLS certificate verification
    #[arg(long)]
    pub insecure: bool,
}

/// Parse repeatable "Name: value" header flags.
pub fn parse_headers(raw: &[String]) -> Result<Vec<(String, String)>, String> {
    raw.iter()
        .map(|h| {
            h.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .ok_or_else(|| format!("invalid header (expected \"Name: value\"): {h}"))
        })
        .collect()
}
