use std::hash::{DefaultHasher, Hash, Hasher};

use async_trait::async_trait;
use dojo_types::naming::compute_selector_from_names;
use dojo_world::contracts::abigen::model::Layout;
use dojo_world::contracts::abigen::world::Event as WorldEvent;
use dojo_world::contracts::model::ModelError;
use starknet::core::types::Event;
use starknet::providers::Provider;
use torii_proto::Model;
use tracing::{debug, info};

use crate::error::Error;
use crate::schema::parse_struct_to_schema_with_namespace;
use crate::task_manager::TaskId;
use crate::{EventProcessor, EventProcessorContext};

pub(crate) const LOG_TARGET: &str = "torii::indexer::processors::register_model_with_schema";

#[derive(Default, Debug)]
pub struct RegisterModelWithSchemaProcessor;

#[async_trait]
impl<P> EventProcessor<P> for RegisterModelWithSchemaProcessor
where
    P: Provider + Send + Sync + Clone + std::fmt::Debug + 'static,
{
    fn event_key(&self) -> String {
        "ModelWithSchemaRegistered".to_string()
    }

    // We might not need this anymore, since we don't have fallback and all world events must
    // be handled.
    fn validate(&self, _event: &Event) -> bool {
        true
    }

    fn task_identifier(&self, event: &Event) -> TaskId {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let selector = match WorldEvent::try_from(event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <RegisterModelWithSchemaProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ModelWithSchemaRegistered(e) => compute_selector_from_names(
                &e.namespace.to_string().unwrap(),
                &e.name.to_string().unwrap(),
            ),
            _ => {
                unreachable!()
            }
        };

        let mut hasher = DefaultHasher::new();
        selector.hash(&mut hasher);
        hasher.finish()
    }

    async fn process(&self, ctx: &EventProcessorContext<P>) -> Result<(), Error> {
        // Torii version is coupled to the world version, so we can expect the event to be well
        // formed.
        let event = match WorldEvent::try_from(&ctx.event).unwrap_or_else(|_| {
            panic!(
                "Expected {} event to be well formed.",
                <RegisterModelWithSchemaProcessor as EventProcessor<P>>::event_key(self)
            )
        }) {
            WorldEvent::ModelWithSchemaRegistered(e) => e,
            _ => {
                unreachable!()
            }
        };

        // Safe to unwrap, since it's coming from the chain.
        let namespace = event.namespace.to_string().unwrap();
        let name = event.name.to_string().unwrap();
        let selector = compute_selector_from_names(&namespace, &name);

        // If the namespace is not in the list of namespaces to index, silently ignore it.
        // If our config is empty, we index all namespaces.
        if !ctx.config.should_index(&namespace, &name) {
            return Ok(());
        }

        let schema = parse_struct_to_schema_with_namespace(&event.schema, &namespace, &name)
            .map_err(ModelError::Parse)?;
        let packed_size = 0;
        let unpacked_size = 0;
        let class_hash = 0.into();
        let contract_address = 0.into();
        let layout = Layout::Fixed(vec![]);

        info!(
            target: LOG_TARGET,
            namespace = %namespace,
            name = %name,
            "Registered model with Schema."
        );

        debug!(
            target: LOG_TARGET,
            name,
            schema = ?schema,
            layout = ?layout,
            class_hash = ?class_hash,
            contract_address = ?contract_address,
            packed_size = %packed_size,
            unpacked_size = %unpacked_size,
            "Registered model content."
        );

        ctx.storage
            .register_model(
                selector,
                &schema,
                &layout,
                class_hash,
                contract_address,
                packed_size,
                unpacked_size,
                ctx.block_timestamp,
                None,
                None,
                false,
            )
            .await?;

        ctx.cache
            .register_model(
                selector,
                Model {
                    selector,
                    namespace,
                    name,
                    class_hash: class_hash.into(),
                    contract_address: contract_address.into(),
                    packed_size,
                    unpacked_size,
                    layout,
                    schema,
                    use_legacy_store: false,
                },
            )
            .await;

        Ok(())
    }
}
