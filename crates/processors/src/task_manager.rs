use std::sync::Arc;

use hashlink::LinkedHashMap;
use starknet::core::types::Event;
use starknet::providers::Provider;
use tokio::sync::Semaphore;
use torii_cache::Cache;
use torii_proto::ContractType;
use torii_storage::Storage;
use tracing::{debug, error};

use crate::error::Error;
use crate::processors::Processors;
use crate::{EventKey, EventProcessorConfig, EventProcessorContext, IndexingMode};
use metrics::{counter, histogram};

const LOG_TARGET: &str = "torii::indexer::task_manager";

pub type TaskId = u64;
pub type TaskPriority = usize;

#[derive(Debug, Clone)]
pub struct ParallelizedEvent {
    pub indexing_mode: IndexingMode,
    pub contract_type: ContractType,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub event_id: String,
    pub event: Event,
}

#[derive(Debug, Clone, Default)]
struct TaskData {
    events: Vec<ParallelizedEvent>,
    latest_only_events: LinkedHashMap<EventKey, ParallelizedEvent>,
}

#[allow(missing_debug_implementations)]
pub struct TaskManager<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> {
    storage: Arc<dyn Storage>,
    cache: Arc<dyn Cache>,
    provider: P,
    pending_tasks: LinkedHashMap<TaskId, TaskData>,
    processors: Arc<Processors<P>>,
    event_processor_config: EventProcessorConfig,
    nft_metadata_semaphore: Arc<Semaphore>,
}

impl<P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static> TaskManager<P> {
    pub fn new(
        storage: Arc<dyn Storage>,
        cache: Arc<dyn Cache>,
        provider: P,
        processors: Arc<Processors<P>>,
        _max_concurrent_tasks: usize,
        event_processor_config: EventProcessorConfig,
    ) -> Self {
        Self {
            storage,
            cache,
            provider,
            pending_tasks: LinkedHashMap::new(),
            processors,
            nft_metadata_semaphore: Arc::new(Semaphore::new(
                event_processor_config.max_metadata_tasks,
            )),
            event_processor_config,
        }
    }

    pub fn pending_tasks_count(&self) -> usize {
        self.pending_tasks.len()
    }

    pub fn add_parallelized_event(
        &mut self,
        task_identifier: TaskId,
        parallelized_event: ParallelizedEvent,
    ) {
        self.add_parallelized_event_with_dependencies(task_identifier, vec![], parallelized_event);
    }

    pub fn add_parallelized_event_with_dependencies(
        &mut self,
        task_identifier: TaskId,
        _dependencies: Vec<TaskId>,
        parallelized_event: ParallelizedEvent,
    ) {
        let task_data = self.pending_tasks.entry(task_identifier).or_insert_with(Default::default);
        match parallelized_event.indexing_mode {
            IndexingMode::Latest(event_key) => {
                task_data
                    .latest_only_events
                    .insert(event_key, parallelized_event);
            }
            IndexingMode::Historical => {
                task_data.events.push(parallelized_event);
            }
        }
    }

    pub async fn process_ready_tasks(&mut self) -> Result<(), Error> {
        if self.pending_tasks.is_empty() {
            return Ok(());
        }

        debug!(target: LOG_TARGET, "Processing {} tasks", self.pending_tasks.len());

        let start = std::time::Instant::now();

        // Collect all tasks first to avoid borrowing issues
        let tasks: Vec<_> = self.pending_tasks.drain().collect();
        
        // Process all tasks (simplified without dependency management)
        for (_task_id, task_data) in tasks {
            let mut all_events = task_data.events;
            all_events.extend(task_data.latest_only_events.into_iter().map(|(_, v)| v));
            
            if !all_events.is_empty() {
                self.process_events(all_events).await?;
            }
        }

        let duration = start.elapsed();
        histogram!("torii_indexer_task_manager_process_ready_tasks_duration_seconds")
            .record(duration.as_secs_f64());

        Ok(())
    }

    async fn process_events(&self, events: Vec<ParallelizedEvent>) -> Result<(), Error> {
        let start = std::time::Instant::now();

        for parallelized_event in events {
            let context = EventProcessorContext {
                storage: self.storage.clone(),
                cache: self.cache.clone(),
                provider: self.provider.clone(),
                block_number: parallelized_event.block_number,
                block_timestamp: parallelized_event.block_timestamp,
                event_id: parallelized_event.event_id.clone(),
                event: parallelized_event.event.clone(),
                config: self.event_processor_config.clone(),
                nft_metadata_semaphore: self.nft_metadata_semaphore.clone(),
            };

            // Get the appropriate event processors for this contract type
            let event_processors_map = self.processors.get_event_processors(parallelized_event.contract_type);

            let mut processed = false;
            for processors_vec in event_processors_map.values() {
                for processor in processors_vec {
                    if processor.validate(&parallelized_event.event) {
                        if let Err(e) = processor.process(&context).await {
                            error!(
                                target: LOG_TARGET,
                                event_id = parallelized_event.event_id,
                                processor = processor.event_key(),
                                "Failed to process event: {}",
                                e
                            );
                            counter!("torii_indexer_task_manager_events_processed_total", "status" => "failed")
                                .increment(1);
                        } else {
                            counter!("torii_indexer_task_manager_events_processed_total", "status" => "success")
                                .increment(1);
                            processed = true;
                        }
                    }
                }
            }

            if !processed {
                // Use catch-all processor if no specific processor handled it
                if let Err(e) = self.processors.catch_all_event.process(&context).await {
                    error!(
                        target: LOG_TARGET,
                        event_id = parallelized_event.event_id,
                        "Failed to process event with catch-all processor: {}",
                        e
                    );
                    counter!("torii_indexer_task_manager_events_processed_total", "status" => "failed")
                        .increment(1);
                }
            }
        }

        let duration = start.elapsed();
        histogram!("torii_indexer_task_manager_process_events_duration_seconds")
            .record(duration.as_secs_f64());

        Ok(())
    }

    pub fn clear(&mut self) {
        self.pending_tasks.clear();
    }
}