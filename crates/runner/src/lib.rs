//! Torii binary executable.
//!
//! ## Feature Flags
//!
//! - `jemalloc`: Uses [jemallocator](https://github.com/tikv/jemallocator) as the global allocator.
//!   This is **not recommended on Windows**. See [here](https://rust-lang.github.io/rfcs/1974-global-allocators.html#jemalloc)
//!   for more info.
//! - `jemalloc-prof`: Enables [jemallocator's](https://github.com/tikv/jemallocator) heap profiling
//!   and leak detection functionality. See [jemalloc's opt.prof](https://jemalloc.net/jemalloc.3.html#opt.prof)
//!   documentation for usage details. This is **not recommended on Windows**. See [here](https://rust-lang.github.io/rfcs/1974-global-allocators.html#jemalloc)
//!   for more info.

use std::cmp;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use camino::Utf8PathBuf;
use dojo_types::naming::try_compute_selector_from_tag;
use futures::future::join_all;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use starknet::core::types::{BlockId, BlockTag};
use starknet::providers::jsonrpc::HttpTransport;
use starknet::providers::{JsonRpcClient, Provider};
use starknet_crypto::Felt;
use tempfile::TempDir;
use terminal_size::{terminal_size, Height, Width};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use torii_cache::InMemoryCache;
use torii_cli::ToriiArgs;
use torii_controllers::sync::ControllersSync;
use torii_indexer::engine::{Engine, EngineConfig};
use torii_indexer::{FetcherConfig, FetchingFlags, IndexingFlags};
use torii_messaging::{Messaging, MessagingConfig};
use torii_processors::{EventProcessorConfig, Processors};
use torii_db::executor::Executor;
use torii_db::{Sql, SqlConfig};
use torii_storage::proto::{ContractDefinition, ContractType};
use tracing::{debug, info, info_span, warn, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

mod constants;

use crate::constants::LOG_TARGET;
const MIN_THREADS: usize = 1;

#[derive(Debug, Clone)]
pub enum AllocationStrategy {
    Adaptive,
    QueryPriority,
    IndexerPriority,
    Balanced,
}

impl From<&str> for AllocationStrategy {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "adaptive" => AllocationStrategy::Adaptive,
            "query_priority" => AllocationStrategy::QueryPriority,
            "indexer_priority" => AllocationStrategy::IndexerPriority,
            "balanced" => AllocationStrategy::Balanced,
            _ => AllocationStrategy::Adaptive,
        }
    }
}

#[derive(Debug)]
pub struct RuntimeAllocation {
    pub query_threads: usize,
    pub indexer_threads: usize,
    pub main_threads: usize,
}

impl RuntimeAllocation {
    pub fn calculate(
        cpu_count: usize,
        strategy: &AllocationStrategy,
        query_override: usize,
        indexer_override: usize,
    ) -> Self {
        // Reserve at least 1 thread for main runtime (proxy, messaging, etc.)
        let available_threads = cpu_count.saturating_sub(1);

        let (query_threads, indexer_threads) = match strategy {
            AllocationStrategy::QueryPriority => {
                // 70% query, 30% indexer
                let query = ((available_threads * 7) / 10)
                    .max(MIN_THREADS)
                    .min(available_threads);
                let indexer = available_threads
                    .saturating_sub(query)
                    .max(MIN_THREADS)
                    .min(available_threads);
                (query, indexer)
            }
            AllocationStrategy::IndexerPriority => {
                // 30% query, 70% indexer
                let indexer = ((available_threads * 7) / 10)
                    .max(MIN_THREADS)
                    .min(available_threads);
                let query = available_threads
                    .saturating_sub(indexer)
                    .max(MIN_THREADS)
                    .min(available_threads);
                (query, indexer)
            }
            AllocationStrategy::Balanced => {
                // 50% each
                let half = available_threads / 2;
                (
                    half.max(MIN_THREADS).min(available_threads),
                    half.max(MIN_THREADS).min(available_threads),
                )
            }
            AllocationStrategy::Adaptive => {
                // Default: 60% query, 40% indexer (queries are user-facing)
                let query = ((available_threads * 6) / 10)
                    .max(MIN_THREADS)
                    .min(available_threads);
                let indexer = available_threads
                    .saturating_sub(query)
                    .max(MIN_THREADS)
                    .min(available_threads);
                (query, indexer)
            }
        };

        Self {
            query_threads: if query_override > 0 {
                query_override.max(MIN_THREADS).min(cpu_count)
            } else {
                query_threads
            },
            indexer_threads: if indexer_override > 0 {
                indexer_override.max(MIN_THREADS).min(cpu_count)
            } else {
                indexer_threads
            },
            main_threads: 1, // Keep main runtime lightweight
        }
    }
}

// Structure to hold runtime and its handle for proper shutdown
struct ManagedRuntime {
    runtime: tokio::runtime::Runtime,
    handle: tokio::runtime::Handle,
}

impl ManagedRuntime {
    fn new(threads: usize, name: &str, stack_size: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name(name)
            .thread_stack_size(stack_size)
            .enable_all()
            .build()
            .expect("Failed to create runtime");

        let handle = runtime.handle().clone();

        Self { runtime, handle }
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    // Shutdown runtime in a blocking context to avoid the panic
    fn shutdown(self) {
        self.runtime.shutdown_background();
    }
}

// Function to create a configurable query runtime
fn create_query_runtime(threads: usize) -> ManagedRuntime {
    ManagedRuntime::new(threads, "torii-query", 2 * 1024 * 1024) // 2MB stack for complex queries
}

// Function to create a dedicated indexer runtime
fn create_indexer_runtime(threads: usize) -> ManagedRuntime {
    ManagedRuntime::new(threads, "torii-indexer", 1024 * 1024) // 1MB stack (less than queries)
}

/// Creates a responsive progress bar template based on terminal size
fn create_progress_bar_template() -> String {
    let (terminal_width, msg_width) = if let Some((Width(w), Height(_))) = terminal_size() {
        // Calculate appropriate widths based on terminal size
        let width = w as usize;
        let min_width = 80;
        let max_width = 120;
        let effective_width = cmp::max(min_width, cmp::min(width, max_width));

        // Calculate message width first (needs space for " XX.Xs" format)
        let msg_width = (effective_width / 8).clamp(8, 20); // Ensure at least 8 chars for seconds

        // Calculate progress bar width (reserve space for other elements)
        // " {spinner:.yellow} snapshot [BAR] {bytes}/{total_bytes} Downloading{msg}"
        let reserved_space = 45 + msg_width; // Space for spinner, labels, bytes, and message
        let bar_width = if effective_width > reserved_space {
            // Use most of the available space for the bar
            let available_space = effective_width - reserved_space;
            cmp::min(60, (available_space * 8) / 10) // Max 60 chars, 80% of available
        } else {
            30 // Minimum bar width
        };

        (bar_width, msg_width)
    } else {
        // Default values if terminal size cannot be determined
        (40, 20)
    };

    format!(
        " {{spinner:.yellow}} snapshot [{{bar:{}.cyan/blue}}] {{bytes}}/{{total_bytes}} Downloading{{wide_msg:>{}.blue}}",
        terminal_width, msg_width
    )
}

#[derive(Debug)]
pub struct Runner {
    args: ToriiArgs,
    version_spec: String,
}

impl Runner {
    pub fn new(args: ToriiArgs, version_spec: String) -> Self {
        Self { args, version_spec }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // dump the config to the given path if it is provided
        if let Some(dump_config) = &self.args.dump_config {
            let mut dump = self.args.clone();
            // remove the config and dump_config params from the dump
            dump.config = None;
            dump.dump_config = None;

            let config = toml::to_string_pretty(&dump)?;
            std::fs::write(dump_config, config)?;
        }

        // Add world to list of generic contracts if it is provided
        if let Some(world_address) = self.args.world_address {
            self.args.indexing.contracts.push(ContractDefinition {
                address: world_address,
                r#type: ContractType::WORLD,
                starting_block: None,
            });
        }

        // Setup cancellation for graceful shutdown
        let (shutdown_tx, _) = broadcast::channel(1);

        let shutdown_tx_clone = shutdown_tx.clone();
        ctrlc::set_handler(move || {
            let _ = shutdown_tx_clone.send(());
        })
        .expect("Error setting Ctrl-C handler");

        let transport = HttpTransport::new(self.args.rpc.clone()).with_header(
            "User-Agent".to_string(),
            format!("Torii/{}", self.version_spec),
        );
        let provider: Arc<_> = JsonRpcClient::new(transport).into();

        // Check provider spec version. We only support v0.9.
        let supported_spec = "0.9";
        let spec_version = provider.spec_version().await?;
        if !spec_version.starts_with(supported_spec) {
            return Err(anyhow::anyhow!(
                "Provider spec version is not supported. Please use a provider that supports v{supported_spec}. Got: {spec_version}. You might need to add a `rpc/v{}` to the end of the URL.",
                supported_spec.replace('.', "_")
            ));
        }

        // Verify contracts are deployed
        if self.args.runner.check_contracts {
            let undeployed =
                verify_contracts_deployed(&provider, &self.args.indexing.contracts).await?;
            if !undeployed.is_empty() {
                return Err(anyhow::anyhow!(
                    "The following contracts are not deployed: {:?}",
                    undeployed
                ));
            }
        }

        // Build PostgreSQL connection URL
        let db_url = if let Some(url) = &self.args.database.url {
            url.clone()
        } else {
            let mut connection_url = format!(
                "postgresql://{}:{}@{}:{}/{}",
                self.args.database.username,
                self.args.database.password.as_deref().unwrap_or(""),
                self.args.database.host,
                self.args.database.port,
                self.args.database.database
            );
            
            if self.args.database.ssl {
                connection_url.push_str("?sslmode=require");
            } else {
                connection_url.push_str("?sslmode=prefer");
            }
            
            connection_url
        };

        info!(target: LOG_TARGET, "Connecting to PostgreSQL database: {}@{}:{}/{}", 
              self.args.database.username, self.args.database.host, 
              self.args.database.port, self.args.database.database);

        // TODO: Implement snapshot download for PostgreSQL
        // For now, snapshot functionality is disabled as it was SQLite-specific
        if self.args.snapshot.snapshot_url.is_some() {
            warn!(target: LOG_TARGET, "Snapshot downloads are not yet supported with PostgreSQL. Continuing with fresh database connection.");
        }

        // Calculate optimal runtime allocation early for database configuration
        let cpu_count = num_cpus::get();
        let strategy = AllocationStrategy::from(self.args.runner.allocation_strategy.as_str());
        let allocation = RuntimeAllocation::calculate(
            cpu_count,
            &strategy,
            self.args.runner.query_threads,
            self.args.runner.indexer_threads,
        );

        // Create PostgreSQL connection options
        let mut options = PgConnectOptions::from_str(&db_url)?;
        
        // Set SSL mode
        if self.args.database.ssl {
            options = options.ssl_mode(PgSslMode::Require);
        } else {
            options = options.ssl_mode(PgSslMode::Prefer);
        }

        // PostgreSQL connection settings optimized for indexing workload
        // Note: PostgreSQL performance tuning is done via server configuration,
        // connection-level options are more limited compared to SQLite PRAGMA settings

        // Create a single connection pool for PostgreSQL
        // PostgreSQL handles concurrent reads/writes better than SQLite, so we don't need separate pools
        let max_connections = cmp::max(
            self.args.sql.max_connections,
            (allocation.query_threads + allocation.indexer_threads) as u32,
        );

        let database_pool = PgPoolOptions::new()
            .min_connections(cmp::min(4, max_connections)) // Keep some connections warm
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_millis(self.args.sql.acquire_timeout))
            .idle_timeout(Some(Duration::from_millis(self.args.sql.idle_timeout)))
            .connect_with(options)
            .await?;

        // Test the connection
        let mut test_conn = database_pool.acquire().await?;
        sqlx::query("SELECT 1").fetch_one(&mut *test_conn).await?;
        
        info!(target: LOG_TARGET, "Successfully connected to PostgreSQL database");

        let mut migrate_handle = database_pool.acquire().await?;
        if let Some(migrations) = self.args.sql.migrations {
            // Create a temporary directory to combine migrations
            let temp_migrations = TempDir::new()?;

            // Copy default migrations first
            let default_migrations_dir =
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../migrations");
            for entry in std::fs::read_dir(default_migrations_dir)? {
                let entry = entry?;
                let target = temp_migrations.path().join(entry.file_name());
                std::fs::copy(entry.path(), target)?;
            }

            // Copy custom migrations
            for entry in std::fs::read_dir(&migrations)? {
                let entry = entry?;
                let target = temp_migrations.path().join(entry.file_name());
                std::fs::copy(entry.path(), target)?;
            }

            // Run combined migrations
            let migrator = sqlx::migrate::Migrator::new(temp_migrations.path()).await?;
            migrator.run(&mut migrate_handle).await?;
        } else {
            sqlx::migrate!("../migrations")
                .run(&mut migrate_handle)
                .await?;
        }

        // PostgreSQL doesn't use PRAGMA statements like SQLite
        // For PostgreSQL, we could run ANALYZE but it's not critical during startup
        // sqlx::query("ANALYZE").execute(&mut *migrate_handle).await?;

        drop(migrate_handle);

        if self.args.sql.all_model_indices && !self.args.sql.model_indices.is_empty() {
            warn!(
                target: LOG_TARGET,
                "all_model_indices is true, which will override any specific indices in model_indices"
            );
        }

        // Validate activity tracking configuration
        if self.args.activity.enabled && !self.args.indexing.transactions {
            return Err(anyhow::anyhow!(
                "Activity tracking is enabled but transaction indexing is disabled. \
                 Activity tracking requires transaction data to function. \
                 Please enable transaction indexing with --indexing.transactions or \
                 disable activity tracking with --activity.enabled=false"
            ));
        }

        let historical_models = self.args.sql.historical.clone().into_iter().try_fold(
            HashSet::new(),
            |mut acc, tag| {
                let selector = try_compute_selector_from_tag(&tag)
                    .map_err(|_| anyhow::anyhow!("Invalid model tag: {}", tag))?;
                acc.insert(selector);
                Ok::<HashSet<Felt>, anyhow::Error>(acc)
            },
        )?;

        // Build excluded entrypoints set - use defaults if not specified
        let default_excluded = [
            "execute_from_outside_v3",
            "request_random",
            "submit_random",
            "assert_consumed",
            "deployContract",
            "set_name",
            "register_model",
            "entities",
            "init_contract",
            "upgrade_model",
            "emit_events",
            "emit_event",
            "set_metadata",
        ];

        let activity_excluded_entrypoints: HashSet<String> =
            if self.args.activity.excluded_entrypoints.is_empty() {
                default_excluded.iter().map(|s| s.to_string()).collect()
            } else {
                self.args
                    .activity
                    .excluded_entrypoints
                    .iter()
                    .cloned()
                    .collect()
            };

        let sql_config = SqlConfig {
            all_model_indices: self.args.sql.all_model_indices,
            model_indices: self.args.sql.model_indices.clone(),
            historical_models: historical_models.clone(),
            hooks: self.args.sql.hooks.clone(),
            aggregators: self.args.sql.aggregators.clone(),
            wal_truncate_size_threshold: self.args.sql.wal_truncate_size_threshold,
            optimize_interval: self.args.sql.optimize_interval,
            activity_enabled: self.args.activity.enabled,
            activity_session_timeout: self.args.activity.session_timeout,
            activity_excluded_entrypoints,
        };

        let (mut executor, sender) = Executor::new_with_config(
            database_pool.clone(),
            shutdown_tx.clone(),
            provider.clone(),
            sql_config.clone(),
            std::path::PathBuf::new(), // PostgreSQL doesn't use db_path (SQLite-specific)
        )
        .await?;
        let executor_handle = tokio::spawn(async move { executor.run().await });

        let db = Sql::new_with_config(
            database_pool.clone(),
            sender.clone(),
            &self.args.indexing.contracts,
            sql_config.clone(),
        )
        .await?;
        let cache = Arc::new(InMemoryCache::new(Arc::new(db.clone())).await.unwrap());
        let db = db.with_cache(cache.clone());

        let processors = Arc::new(Processors::default());

        let mut indexing_flags = IndexingFlags::empty();
        if self.args.events.raw {
            indexing_flags.insert(IndexingFlags::RAW_EVENTS);
        }
        let mut fetching_flags = FetchingFlags::empty();
        if self.args.indexing.transactions {
            fetching_flags.insert(FetchingFlags::TRANSACTIONS);
        }
        if self.args.indexing.preconfirmed {
            fetching_flags.insert(FetchingFlags::PRECONFIRMED_BLOCK);
        }

        let storage = Arc::new(db.clone());
        let controllers = if self.args.indexing.controllers {
            Some(Arc::new(
                ControllersSync::new(storage.clone()).await.unwrap(),
            ))
        } else {
            None
        };

        // Scale max_concurrent_tasks based on indexer threads for better CPU utilization
        let optimal_concurrent_tasks = if self.args.indexing.max_concurrent_tasks == 100 {
            // Default value, scale with indexer threads
            (allocation.indexer_threads * 8).clamp(50, 500) // 8 tasks per thread, reasonable bounds
        } else {
            // User specified, respect their choice
            self.args.indexing.max_concurrent_tasks
        };

        debug!(target: LOG_TARGET,
            cpu_count = cpu_count,
            strategy = ?strategy,
            query_threads = allocation.query_threads,
            indexer_threads = allocation.indexer_threads,
            max_concurrent_tasks = optimal_concurrent_tasks,
            "Runtime allocation calculated"
        );

        let mut engine: Engine<Arc<JsonRpcClient<HttpTransport>>> = Engine::new_with_controllers(
            storage.clone(),
            cache.clone(),
            provider.clone(),
            processors.clone(),
            EngineConfig {
                max_concurrent_tasks: optimal_concurrent_tasks,
                fetcher_config: FetcherConfig {
                    batch_chunk_size: self.args.indexing.batch_chunk_size,
                    blocks_chunk_size: self.args.indexing.blocks_chunk_size,
                    events_chunk_size: self.args.indexing.events_chunk_size,
                    world_block: self.args.indexing.world_block,
                    flags: fetching_flags,
                },
                polling_interval: Duration::from_millis(self.args.indexing.polling_interval),
                flags: indexing_flags,
                event_processor_config: EventProcessorConfig {
                    strict_model_reader: self.args.indexing.strict_model_reader,
                    namespaces: self.args.indexing.namespaces.into_iter().collect(),
                    historical_models,
                    max_metadata_tasks: self.args.erc.max_metadata_tasks,
                    models: self.args.indexing.models.clone().into_iter().collect(),
                    external_contracts: self.args.indexing.external_contracts,
                    external_contract_whitelist: self
                        .args
                        .indexing
                        .external_contract_whitelist
                        .clone()
                        .into_iter()
                        .collect(),
                },
                world_block: self.args.indexing.world_block,
            },
            shutdown_tx.clone(),
            controllers,
        );

        let _shutdown_rx = shutdown_tx.subscribe();
        let temp_dir = TempDir::new()?;
        let artifacts_path = self
            .args
            .erc
            .artifacts_path
            .unwrap_or_else(|| Utf8PathBuf::from(temp_dir.path().to_str().unwrap()));

        tokio::fs::create_dir_all(&artifacts_path).await?;
        let _absolute_path = artifacts_path.canonicalize_utf8()?;

        // Create messaging instance with default configuration (server features disabled)
        let messaging_config = MessagingConfig {
            max_age: 86400, // 24 hours default
            future_tolerance: 300, // 5 minutes default
            require_timestamp: false, // Default to false
        };
        let _messaging = Arc::new(Messaging::new(
            messaging_config,
            storage.clone(),
            provider.clone(),
        ));

        // Core indexing setup complete, starting indexing services only
        info!(target: LOG_TARGET, "Starting Torii indexer services...");
        info!(target: LOG_TARGET, "Server endpoints disabled - running indexer-only mode");
        info!(target: LOG_TARGET, path = %artifacts_path, "Using ERC artifacts at path");

        // Metrics disabled in indexer-only mode
        // Note: Metrics functionality removed with server components

        // Create dedicated runtimes
        let query_runtime = create_query_runtime(allocation.query_threads);
        let indexer_runtime = create_indexer_runtime(allocation.indexer_threads);

        // Move engine to dedicated indexer runtime for CPU isolation
        let engine_handle = indexer_runtime
            .handle()
            .spawn(async move { engine.start().await });

        info!(target: LOG_TARGET, "Core indexing services started successfully");

        // Macro to handle task results uniformly
        macro_rules! handle_task {
            ($result:expr, $name:literal) => {
                match $result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(anyhow::anyhow!("{} failed: {}", $name, e)),
                    Err(e) => Err(anyhow::anyhow!("{} task panicked: {}", $name, e)),
                }
            };
        }

        // Wait for shutdown signal or any core task completion
        let result = tokio::select! {
            res = engine_handle => handle_task!(res, "Engine"),
            res = executor_handle => handle_task!(res, "Executor"),
            _ = dojo_utils::signal::wait_signals() => {
                info!(target: LOG_TARGET, "Shutdown signal received, cleaning up...");
                Ok(())
            },
        };

        // Properly shutdown runtimes
        query_runtime.shutdown();
        indexer_runtime.shutdown();

        info!(target: LOG_TARGET, "Shutdown complete");
        result
    }
}



async fn verify_contracts_deployed(
    provider: &JsonRpcClient<HttpTransport>,
    contracts: &[ContractDefinition],
) -> anyhow::Result<Vec<ContractDefinition>> {
    // Create a future for each contract verification
    let verification_futures = contracts.iter().map(|contract| {
        let contract = contract.clone();
        async move {
            let result = provider
                .get_class_at(BlockId::Tag(BlockTag::PreConfirmed), contract.address)
                .await;
            (contract, result)
        }
    });

    // Run all verifications concurrently
    let results = join_all(verification_futures).await;

    // Collect undeployed contracts
    let undeployed = results
        .into_iter()
        .filter_map(|(contract, result)| match result {
            Ok(_) => None,
            Err(_) => Some(contract),
        })
        .collect();

    Ok(undeployed)
}

/// Streams a snapshot into a file, displaying progress and handling potential errors.
///
/// # Arguments
/// * `url` - The URL to download from.
/// * `destination_path` - The path to save the downloaded file.
/// * `client` - An instance of `reqwest::Client`.
///
/// # Returns
/// * `Ok(())` if the download is successful.
/// * `Err(anyhow::Error)` if any error occurs during download or file writing.
async fn stream_snapshot_into_file(
    url: &str,
    destination_path: &Path,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let response = client.get(url).send().await?.error_for_status()?;
    let total_size = response.content_length().unwrap_or(0);

    let span = info_span!("download_snapshot", url);
    span.pb_set_style(
        &indicatif::ProgressStyle::default_bar()
            .template(&create_progress_bar_template())?
            .progress_chars("⣿⣤⠀")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    span.pb_set_length(total_size);
    span.pb_set_message(&format!(" {:.1}s", 0.0));

    let instrumented_future = async {
        let mut file = File::create(destination_path).await?;
        let mut downloaded: u64 = 0;
        let mut stream = response.bytes_stream();
        let start_time = std::time::Instant::now();

        while let Some(item) = stream.next().await {
            let chunk = item?;
            file.write_all(&chunk).await?;
            let new = cmp::min(downloaded.saturating_add(chunk.len() as u64), total_size);
            downloaded = new;
            let elapsed = start_time.elapsed().as_secs_f64();
            Span::current().pb_set_position(new);
            Span::current().pb_set_message(&format!(" {:.1}s", elapsed));
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        Span::current().pb_set_message(&format!(" {:.1}s", elapsed));
        Ok(())
    }
    .instrument(span);

    instrumented_future.await
}


