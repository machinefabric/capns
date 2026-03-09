//! Cap Plan Builder
//!
//! Utility for building cap execution plans. This module provides:
//! - Plan construction from pre-computed paths (via `build_plan_from_path`)
//! - Argument analysis for slot presentation
//!
//! NOTE: Path finding has been moved to `LiveCapGraph`. Use `LiveCapGraph` for
//! `get_reachable_targets()` and `find_paths_to_exact_target()`, then pass the
//! resulting `CapChainPathInfo` to `build_plan_from_path()` here.

use std::collections::HashMap;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use serde_json::json;

use crate::{Cap, CapRegistry, MediaUrn, MediaUrnRegistry, MediaValidation};
use super::argument_binding::{ArgumentBinding, ArgumentBindings};
use super::cardinality::InputCardinality;
use super::plan::{CapEdge, CapExecutionPlan, CapNode};
use super::PlannerError;
use super::live_cap_graph::CapChainPathInfo;

type PlannerResult<T> = Result<T, PlannerError>;

/// Builder for creating cap execution plans.
///
/// NOTE: Path finding methods have been moved to `LiveCapGraph`.
/// This builder handles plan construction from pre-computed paths.
pub struct CapPlanBuilder {
    /// Cap registry for looking up cap definitions
    cap_registry: Arc<CapRegistry>,
    /// Media URN registry for resolving media specs
    media_registry: Arc<MediaUrnRegistry>,
}

impl CapPlanBuilder {
    /// Create a new plan builder with the given registries.
    pub fn new(cap_registry: Arc<CapRegistry>, media_registry: Arc<MediaUrnRegistry>) -> Self {
        Self {
            cap_registry,
            media_registry,
        }
    }

    /// Find the file-path argument in a cap by checking the media URN type.
    /// Returns the argument media_urn if found, None otherwise.
    /// This uses tagged URN matching (via `is_any_file_path()`).
    fn find_file_path_arg(cap: &Cap) -> Option<String> {
        for arg in cap.get_args() {
            if let Ok(urn) = MediaUrn::from_string(&arg.media_urn) {
                if urn.is_any_file_path() {
                    return Some(arg.media_urn.clone());
                }
            }
        }
        None
    }

    /// Check if a file-path arg is also the primary stdin input slot.
    /// Returns true if the arg has a stdin source whose media URN matches the cap's in_spec.
    /// This means the arg can receive piped data from the previous cap in a chain,
    /// not just a literal file path.
    fn is_file_path_stdin_chainable(cap: &Cap) -> bool {
        let in_spec = cap.urn.in_spec();
        for arg in cap.get_args() {
            let is_file_path = MediaUrn::from_string(&arg.media_urn)
                .map(|urn| urn.is_any_file_path())
                .unwrap_or(false);
            if !is_file_path {
                continue;
            }
            for source in &arg.sources {
                if let crate::ArgSource::Stdin { stdin } = source {
                    if stdin == in_spec {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Build a plan from a pre-defined path.
    /// Looks up cap definitions to find file-path argument names by media URN type.
    ///
    /// Takes a `CapChainPathInfo` from LiveCapGraph which uses typed URNs.
    pub async fn build_plan_from_path(
        &self,
        name: &str,
        path: &CapChainPathInfo,
        input_cardinality: InputCardinality,
    ) -> PlannerResult<CapExecutionPlan> {
        let mut plan = CapExecutionPlan::new(name);

        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to get caps: {}", e)))?;

        // Build a map from cap_urn string to (file-path arg name, stdin-chainable)
        let file_path_info: HashMap<String, (Option<String>, bool)> = path.steps
            .iter()
            .map(|step| {
                let cap_urn_str = step.cap_urn.to_string();
                let cap = caps.iter().find(|c| c.urn.to_string() == cap_urn_str);
                let arg_name = cap.and_then(|c| Self::find_file_path_arg(c));
                let chainable = cap.map(|c| Self::is_file_path_stdin_chainable(c)).unwrap_or(false);
                (cap_urn_str, (arg_name, chainable))
            })
            .collect();

        let source_spec_str = path.source_spec.to_string();
        let target_spec_str = path.target_spec.to_string();

        let input_slot_id = "input_slot";
        plan.add_node(CapNode::input_slot(
            input_slot_id,
            "input",
            &source_spec_str,
            input_cardinality,
        ));

        let mut prev_node_id = input_slot_id.to_string();

        for (i, step) in path.steps.iter().enumerate() {
            let node_id = format!("cap_{}", i);
            let cap_urn_str = step.cap_urn.to_string();

            let mut bindings = ArgumentBindings::new();

            let cap = caps.iter().find(|c| c.urn.to_string() == cap_urn_str);

            let in_spec = cap.map(|c| c.urn.in_spec()).unwrap_or_default();
            let out_spec = cap.map(|c| c.urn.out_spec()).unwrap_or_default();

            if let Some((Some(arg_name), stdin_chainable)) = file_path_info.get(&cap_urn_str) {
                if i == 0 {
                    bindings.add_file_path(arg_name);
                } else if *stdin_chainable {
                    bindings.add(
                        arg_name.clone(),
                        ArgumentBinding::PreviousOutput {
                            node_id: prev_node_id.clone(),
                            output_field: None,
                        },
                    );
                } else {
                    bindings.add_file_path(arg_name);
                }
            }

            // Add Slot bindings for all non-I/O arguments
            if let Some(cap) = cap {
                for arg in cap.get_args() {
                    if arg.media_urn == in_spec || arg.media_urn == out_spec {
                        continue;
                    }

                    let is_file_path_type = MediaUrn::from_string(&arg.media_urn)
                        .map(|urn| urn.is_any_file_path())
                        .unwrap_or(false);
                    if is_file_path_type {
                        continue;
                    }

                    if bindings.bindings.contains_key(&arg.media_urn) {
                        continue;
                    }

                    bindings.add(
                        arg.media_urn.clone(),
                        ArgumentBinding::Slot {
                            name: arg.media_urn.clone(),
                            schema: None,
                        },
                    );
                }
            }

            let node = CapNode::cap_with_bindings(&node_id, &cap_urn_str, bindings);
            plan.add_node(node);
            plan.add_edge(CapEdge::direct(&prev_node_id, &node_id));

            prev_node_id = node_id;
        }

        let output_id = "output";
        plan.add_node(CapNode::output(output_id, "result", &prev_node_id));
        plan.add_edge(CapEdge::direct(&prev_node_id, output_id));

        plan.metadata = Some(HashMap::from([
            ("source_spec".to_string(), json!(source_spec_str)),
            ("target_spec".to_string(), json!(target_spec_str)),
        ]));

        Ok(plan)
    }
}

// NOTE: Path finding methods (find_path, get_reachable_targets, get_reachable_targets_with_metadata,
// find_all_paths) have been moved to LiveCapGraph. Use LiveCapGraph for path finding and
// build_plan_from_path for plan construction.
//
// The old string-based ReachableTargetInfo, CapChainStepInfo, CapChainPathInfo types have been
// replaced by the typed versions in live_cap_graph.rs.

// =============================================================================
// ARGUMENT ANALYSIS FOR SLOT PRESENTATION
// =============================================================================

/// How an argument will be resolved
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentResolution {
    /// Auto-resolved from input file (for first cap's file_path)
    FromInputFile,
    /// Auto-resolved from previous cap's output
    FromPreviousOutput,
    /// Has a default value in cap definition
    HasDefault,
    /// Must be provided by user (slot)
    RequiresUserInput,
}

/// Information about a single argument for UI presentation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgumentInfo {
    /// Argument name (e.g., "file_path", "width")
    pub name: String,
    /// Media URN describing the type (e.g., "media:integer")
    pub media_urn: String,
    /// Human-readable description
    pub description: String,
    /// How this argument will be resolved
    pub resolution: ArgumentResolution,
    /// Default value if any
    pub default_value: Option<serde_json::Value>,
    /// Whether this is a required argument
    pub is_required: bool,
    /// Validation rules if any
    pub validation: Option<serde_json::Value>,
}

/// Argument requirements for a single step in the path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepArgumentRequirements {
    /// Cap URN for this step
    pub cap_urn: String,
    /// Step index (0-based)
    pub step_index: usize,
    /// Cap title
    pub title: String,
    /// All arguments for this cap with their resolution status
    pub arguments: Vec<ArgumentInfo>,
    /// Arguments that require user input (slots)
    pub slots: Vec<ArgumentInfo>,
}

/// Argument requirements for an entire path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathArgumentRequirements {
    /// Source media spec
    pub source_spec: String,
    /// Target media spec
    pub target_spec: String,
    /// Requirements for each step
    pub steps: Vec<StepArgumentRequirements>,
    /// All slots across all steps that need user input
    pub all_slots: Vec<ArgumentInfo>,
    /// Whether this path can execute without any user input
    pub can_execute_without_input: bool,
}

impl CapPlanBuilder {
    /// Analyze argument requirements for a path.
    ///
    /// Takes the new typed `CapChainPathInfo` from `live_cap_graph` which uses
    /// typed `MediaUrn` and `CapUrn` values.
    pub async fn analyze_path_arguments(
        &self,
        path: &CapChainPathInfo,
    ) -> PlannerResult<PathArgumentRequirements> {
        let caps = self.cap_registry.get_cached_caps().await
            .map_err(|e| PlannerError::RegistryError(format!("Failed to get caps: {}", e)))?;

        let mut step_requirements = Vec::new();
        let mut all_slots = Vec::new();

        for (step_index, step) in path.steps.iter().enumerate() {
            let cap_urn_str = step.cap_urn.to_string();
            let cap = caps.iter()
                .find(|c| c.urn.to_string() == cap_urn_str)
                .ok_or_else(|| PlannerError::NotFound(format!(
                    "Cap '{}' not found in registry",
                    cap_urn_str
                )))?;

            let in_spec = cap.urn.in_spec();
            let out_spec = cap.urn.out_spec();

            let mut arguments = Vec::new();
            let mut slots = Vec::new();

            for arg in cap.get_args() {
                let resolution = self.determine_resolution_with_io_check(
                    &arg.media_urn,
                    &in_spec,
                    &out_spec,
                    step_index,
                    arg.required,
                    &arg.default_value,
                );

                // Resolve validation from media spec
                let resolved_spec = crate::media::spec::resolve_media_urn(
                    &arg.media_urn, Some(&cap.media_specs), &self.media_registry
                ).await.ok();
                let validation = resolved_spec.and_then(|spec| spec.validation);

                let arg_info = ArgumentInfo {
                    name: arg.media_urn.clone(),
                    media_urn: arg.media_urn.clone(),
                    description: arg.arg_description.clone().unwrap_or_default(),
                    resolution: resolution.clone(),
                    default_value: arg.default_value.clone(),
                    is_required: arg.required,
                    validation: Self::validation_to_json(validation.as_ref()),
                };

                let is_io_arg = resolution == ArgumentResolution::FromInputFile
                    || resolution == ArgumentResolution::FromPreviousOutput;

                if !is_io_arg {
                    slots.push(arg_info.clone());
                    all_slots.push(arg_info.clone());
                }
                arguments.push(arg_info);
            }

            step_requirements.push(StepArgumentRequirements {
                cap_urn: cap_urn_str,
                step_index,
                title: step.title.clone(),
                arguments,
                slots,
            });
        }

        let can_execute_without_input = all_slots.is_empty();

        Ok(PathArgumentRequirements {
            source_spec: path.source_spec.to_string(),
            target_spec: path.target_spec.to_string(),
            steps: step_requirements,
            all_slots,
            can_execute_without_input,
        })
    }

    /// Convert MediaValidation to JSON if it has any constraints
    fn validation_to_json(validation: Option<&MediaValidation>) -> Option<serde_json::Value> {
        let validation = validation?;

        let has_constraints = validation.min.is_some()
            || validation.max.is_some()
            || validation.min_length.is_some()
            || validation.max_length.is_some()
            || validation.pattern.is_some()
            || validation.allowed_values.is_some();

        if has_constraints {
            serde_json::to_value(validation).ok()
        } else {
            None
        }
    }

    /// Determine how an argument will be resolved based on I/O matching and media URN type.
    fn determine_resolution_with_io_check(
        &self,
        media_urn: &str,
        in_spec: &str,
        out_spec: &str,
        step_index: usize,
        _is_required: bool,
        default_value: &Option<serde_json::Value>,
    ) -> ArgumentResolution {
        // Check if this arg is the input arg (matches cap's in= spec)
        if media_urn == in_spec {
            if step_index == 0 {
                return ArgumentResolution::FromInputFile;
            } else {
                return ArgumentResolution::FromPreviousOutput;
            }
        }

        // Check if this arg is the output arg (matches cap's out= spec)
        if media_urn == out_spec {
            return ArgumentResolution::FromPreviousOutput;
        }

        // Check for file-path types
        let is_file_path_type = if let Ok(urn) = MediaUrn::from_string(media_urn) {
            urn.is_any_file_path()
        } else {
            false
        };

        if is_file_path_type {
            if step_index == 0 {
                return ArgumentResolution::FromInputFile;
            } else {
                return ArgumentResolution::FromPreviousOutput;
            }
        }

        // All other args need user input
        if default_value.is_some() {
            return ArgumentResolution::HasDefault;
        }

        ArgumentResolution::RequiresUserInput
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};
    use crate::CapUrn;

    /// Helper to create a test cap with given in/out specs (full media URNs)
    fn make_test_cap(op: &str, in_spec: &str, out_spec: &str, title: &str) -> Result<Cap, crate::urn::cap_urn::CapUrnError> {
        let mut tags = BTreeMap::new();
        tags.insert("op".to_string(), op.to_string());
        let urn = CapUrn::new(in_spec.to_string(), out_spec.to_string(), tags)?;
        Ok(Cap::new(urn, title.to_string(), "test-command".to_string()))
    }

    /// Simulates the graph-building duplicate detection logic
    fn check_for_duplicate_caps(caps: &[Cap]) -> std::result::Result<usize, String> {
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();
        let mut edge_count = 0;

        for cap in caps {
            let input_spec = cap.urn.in_spec();
            let output_spec = cap.urn.out_spec();

            if input_spec.is_empty() || output_spec.is_empty() {
                continue;
            }

            let cap_urn = cap.urn.to_string();

            let edge_key = (input_spec.to_string(), cap_urn.clone());
            if !seen_edges.insert(edge_key) {
                return Err(format!(
                    "Duplicate cap_urn detected: {} (input_spec: {})",
                    cap_urn, input_spec
                ));
            }
            edge_count += 1;
        }

        Ok(edge_count)
    }

    // TEST750: Tests duplicate detection passes for caps with unique URN combinations
    // Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations
    #[test]
    fn test750_no_duplicates_with_unique_caps() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("extract_metadata", "media:pdf", "media:file-metadata;textable;record", "Extract Metadata")?,
            make_test_cap("extract_outline", "media:pdf", "media:document-outline;textable;record", "Extract Outline")?,
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind PDF")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Should not detect duplicates for unique caps");
        assert_eq!(result.unwrap(), 3, "Should have 3 edges");
        Ok(())
    }

    // TEST751: Tests duplicate detection identifies caps with identical URNs
    // Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn
    #[test]
    fn test751_detects_duplicate_cap_urns() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind PDF")?,
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind PDF Again")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_err(), "Should detect duplicate cap URN");
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Duplicate cap_urn detected"), "Error should mention duplicate: {}", err_msg);
        assert!(err_msg.contains("op=disbind"), "Error should contain the cap URN: {}", err_msg);
        assert!(err_msg.contains("media:pdf"), "Error should contain the input spec: {}", err_msg);
        Ok(())
    }

    // TEST752: Tests caps with different operations but same input/output types are not duplicates
    // Verifies that only the complete URN (including op) is used for duplicate detection
    #[test]
    fn test752_different_ops_same_types_not_duplicates() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("disbind", "media:pdf", "media:disbound-pages;textable;list", "Disbind")?,
            make_test_cap("grind", "media:pdf", "media:disbound-pages;textable;list", "Grind")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Different ops should not be duplicates");
        assert_eq!(result.unwrap(), 2, "Should have 2 edges");
        Ok(())
    }

    // TEST753: Tests caps with same operation but different input types are not duplicates
    // Verifies that input type differences distinguish caps with the same operation name
    #[test]
    fn test753_same_op_different_input_types_not_duplicates() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let caps = vec![
            make_test_cap("extract_metadata", "media:pdf", "media:file-metadata;textable;record", "Extract PDF Metadata")?,
            make_test_cap("extract_metadata", "media:txt;textable", "media:file-metadata;textable;record", "Extract TXT Metadata")?,
        ];

        let result = check_for_duplicate_caps(&caps);
        assert!(result.is_ok(), "Same op with different inputs should not be duplicates");
        assert_eq!(result.unwrap(), 2, "Should have 2 edges");
        Ok(())
    }

    // ==========================================================================
    // ARGUMENT RESOLUTION TESTS
    // ==========================================================================

    fn create_test_plan_builder() -> CapPlanBuilder {
        let cap_registry = CapRegistry::new_for_test();
        let media_registry = MediaUrnRegistry::new_for_test(
            std::env::temp_dir().join(format!("capdag_test_{}", uuid::Uuid::new_v4()))
        ).expect("Failed to create test media registry");
        CapPlanBuilder::new(
            Arc::new(cap_registry),
            Arc::new(media_registry),
        )
    }

    fn create_test_plan_builder_with_registry(cap_registry: CapRegistry) -> CapPlanBuilder {
        let media_registry = MediaUrnRegistry::new_for_test(
            std::env::temp_dir().join(format!("capdag_test_{}", uuid::Uuid::new_v4()))
        ).expect("Failed to create test media registry");
        CapPlanBuilder::new(
            Arc::new(cap_registry),
            Arc::new(media_registry),
        )
    }

    // TEST754: Tests first cap's input argument is automatically resolved from input file
    // Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain
    #[test]
    fn test754_input_arg_first_cap_auto_resolved_from_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST755: Tests subsequent caps' input arguments are automatically resolved from previous output
    // Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first
    #[test]
    fn test755_input_arg_subsequent_cap_auto_resolved_from_previous() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";

        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);

        let resolution = builder.determine_resolution_with_io_check(in_spec, in_spec, out_spec, 2, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST756: Tests output arguments are automatically resolved from previous cap's output
    // Verifies that arguments matching the output spec are always resolved as FromPreviousOutput
    #[test]
    fn test756_output_arg_auto_resolved() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(out_spec, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST757: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap
    // Verifies that generic file-path arguments are bound to input file in the first cap
    #[test]
    fn test757_file_path_type_fallback_first_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);
    }

    // TEST758: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps
    // Verifies that generic file-path arguments are bound to previous cap's output after the first cap
    #[test]
    fn test758_file_path_type_fallback_subsequent_cap() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST759: Tests MEDIA_FILE_PATH_ARRAY argument type resolution for first and subsequent caps
    // Verifies that file-path array arguments follow the same resolution pattern as single file paths
    #[test]
    fn test759_file_path_array_fallback() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH_ARRAY, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromInputFile);

        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_FILE_PATH_ARRAY, in_spec, out_spec, 1, true, &None);
        assert_eq!(resolution, ArgumentResolution::FromPreviousOutput);
    }

    // TEST760: Tests required non-IO arguments with default values are marked as HasDefault
    // Verifies that arguments like integers with defaults don't require user input
    #[test]
    fn test760_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(200));
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_INTEGER, in_spec, out_spec, 0, true, &default);
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST761: Tests required non-IO arguments without defaults require user input
    // Verifies that arguments like strings without defaults are marked as RequiresUserInput
    #[test]
    fn test761_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_STRING, in_spec, out_spec, 0, true, &None);
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST762: Tests optional non-IO arguments with default values are marked as HasDefault
    // Verifies that optional arguments with defaults behave the same as required ones with defaults
    #[test]
    fn test762_optional_non_io_arg_with_default_has_default() {
        let builder = create_test_plan_builder();
        let default = Some(serde_json::json!(300));
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_INTEGER, in_spec, out_spec, 0, false, &default);
        assert_eq!(resolution, ArgumentResolution::HasDefault);
    }

    // TEST763: Tests optional non-IO arguments without defaults still require user input
    // Verifies that optional arguments without defaults must be explicitly provided or skipped
    #[test]
    fn test763_optional_non_io_arg_without_default_requires_user_input() {
        let builder = create_test_plan_builder();
        let in_spec = "media:pdf";
        let out_spec = "media:png";
        let resolution = builder.determine_resolution_with_io_check(crate::MEDIA_BOOLEAN, in_spec, out_spec, 0, false, &None);
        assert_eq!(resolution, ArgumentResolution::RequiresUserInput);
    }

    // TEST764: Tests validation_to_json() returns None for None input
    // Verifies that missing validation metadata is converted to JSON None
    #[test]
    fn test764_validation_to_json_none() {
        let json = CapPlanBuilder::validation_to_json(None);
        assert!(json.is_none(), "None validation should return None");
    }

    // TEST765: Tests validation_to_json() returns None for empty validation constraints
    // Verifies that default MediaValidation with no constraints produces JSON None
    #[test]
    fn test765_validation_to_json_empty() {
        let validation = MediaValidation::default();
        let json = CapPlanBuilder::validation_to_json(Some(&validation));
        assert!(json.is_none(), "Empty validation should return None");
    }

    // TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON
    // Verifies that min/max validation rules are correctly serialized as JSON fields
    #[test]
    fn test766_validation_to_json_with_constraints() {
        let validation = MediaValidation {
            min: Some(50.0),
            max: Some(2000.0),
            min_length: None,
            max_length: None,
            pattern: None,
            allowed_values: None,
        };
        let json = CapPlanBuilder::validation_to_json(Some(&validation));
        assert!(json.is_some(), "Validation with constraints should return Some");
        let json = json.unwrap();
        assert_eq!(json["min"], 50.0);
        assert_eq!(json["max"], 2000.0);
    }

    // TEST767: Tests ArgumentInfo struct serialization to JSON
    // Verifies that argument metadata including resolution status and validation is correctly serialized
    #[test]
    fn test767_argument_info_serialization() {
        let arg_info = ArgumentInfo {
            name: "width".to_string(),
            media_urn: "media:integer".to_string(),
            description: "Width in pixels".to_string(),
            resolution: ArgumentResolution::HasDefault,
            default_value: Some(serde_json::json!(200)),
            is_required: false,
            validation: Some(serde_json::json!({"min": 50, "max": 2000})),
        };

        let json = serde_json::to_string(&arg_info).expect("Should serialize");
        assert!(json.contains("\"name\":\"width\""));
        assert!(json.contains("\"resolution\":\"has_default\""));
        assert!(json.contains("\"default_value\":200"));
    }

    // TEST768: Tests PathArgumentRequirements structure for single-step execution paths
    // Verifies that argument requirements are correctly organized by step with resolution information
    #[test]
    fn test768_path_argument_requirements_structure() {
        let requirements = PathArgumentRequirements {
            source_spec: "media:pdf".to_string(),
            target_spec: "media:png".to_string(),
            steps: vec![
                StepArgumentRequirements {
                    cap_urn: "cap:op=generate_thumbnail;in=pdf;out=png".to_string(),
                    step_index: 0,
                    title: "Generate Thumbnail".to_string(),
                    arguments: vec![
                        ArgumentInfo {
                            name: "file_path".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Path to file".to_string(),
                            resolution: ArgumentResolution::FromInputFile,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                    ],
                    slots: vec![],
                },
            ],
            all_slots: vec![],
            can_execute_without_input: true,
        };

        assert!(requirements.can_execute_without_input);
        assert_eq!(requirements.steps.len(), 1);
        assert_eq!(requirements.steps[0].slots.len(), 0);
        assert_eq!(requirements.steps[0].arguments[0].resolution, ArgumentResolution::FromInputFile);
    }

    // TEST769: Tests PathArgumentRequirements tracking of required user-input slots
    // Verifies that arguments requiring user input are collected in slots and can_execute_without_input is false
    #[test]
    fn test769_path_with_required_slot() {
        let requirements = PathArgumentRequirements {
            source_spec: "media:text".to_string(),
            target_spec: "media:translated".to_string(),
            steps: vec![
                StepArgumentRequirements {
                    cap_urn: "cap:op=translate;in=text;out=translated".to_string(),
                    step_index: 0,
                    title: "Translate".to_string(),
                    arguments: vec![
                        ArgumentInfo {
                            name: "file_path".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Path to file".to_string(),
                            resolution: ArgumentResolution::FromInputFile,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                        ArgumentInfo {
                            name: "target_language".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Target language code".to_string(),
                            resolution: ArgumentResolution::RequiresUserInput,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                    ],
                    slots: vec![
                        ArgumentInfo {
                            name: "target_language".to_string(),
                            media_urn: "media:string".to_string(),
                            description: "Target language code".to_string(),
                            resolution: ArgumentResolution::RequiresUserInput,
                            default_value: None,
                            is_required: true,
                            validation: None,
                        },
                    ],
                },
            ],
            all_slots: vec![
                ArgumentInfo {
                    name: "target_language".to_string(),
                    media_urn: "media:string".to_string(),
                    description: "Target language code".to_string(),
                    resolution: ArgumentResolution::RequiresUserInput,
                    default_value: None,
                    is_required: true,
                    validation: None,
                },
            ],
            can_execute_without_input: false,
        };

        assert!(!requirements.can_execute_without_input);
        assert_eq!(requirements.all_slots.len(), 1);
        assert_eq!(requirements.all_slots[0].name, "target_language");
        assert_eq!(requirements.steps[0].slots.len(), 1);
    }

    // ==========================================================================
    // AVAILABILITY FILTERING TESTS
    // ==========================================================================

    fn create_plan_builder_with_available_caps(available: HashSet<String>) -> CapPlanBuilder {
        create_test_plan_builder().with_available_caps(available)
    }

    // TEST770: Tests is_cap_available() correctly applies availability filter when set
    // Verifies that only caps in the available_caps set are considered available
    #[test]
    fn test770_is_cap_available_with_filter() {
        let mut available = HashSet::new();
        available.insert("cap:in=\"media:a\";op=transform;out=\"media:b\"".to_string());

        let builder = create_plan_builder_with_available_caps(available);

        assert!(builder.is_cap_available("cap:in=\"media:a\";op=transform;out=\"media:b\""));
        assert!(!builder.is_cap_available("cap:in=\"media:b\";op=convert;out=\"media:c\""));
    }

    // TEST771: Tests is_cap_available() treats all caps as available when no filter is set
    // Verifies that without an availability filter, any cap URN is considered available
    #[test]
    fn test771_is_cap_available_without_filter() {
        let builder = create_test_plan_builder();

        assert!(builder.is_cap_available("cap:in=\"media:a\";op=transform;out=\"media:b\""));
        assert!(builder.is_cap_available("cap:in=\"media:x\";op=anything;out=\"media:y\""));
    }

    // TEST772: Tests find_all_paths() excludes unavailable caps from pathfinding
    // Verifies that only paths using available caps are returned when filter is set
    #[tokio::test]
    async fn test772_find_all_paths_filters_by_availability() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;
        let cap3 = make_test_cap("direct", "media:a", "media:c", "A to C Direct")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone(), cap3.clone()]);

        let mut available = HashSet::new();
        available.insert(cap1.urn.to_string());
        available.insert(cap2.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:a", "media:c", 5, 10).await.unwrap();

        assert_eq!(paths.len(), 1, "Should only find one path (through available caps)");
        assert_eq!(paths[0].steps.len(), 2, "Path should have 2 steps (A->B, B->C)");
        assert_eq!(paths[0].steps[0].title, "A to B");
        assert_eq!(paths[0].steps[1].title, "B to C");
        Ok(())
    }

    // TEST773: Tests find_all_paths() returns empty result when all caps are filtered out
    // Verifies that pathfinding returns no paths when the availability filter excludes all relevant caps
    #[tokio::test]
    async fn test773_find_all_paths_returns_empty_when_no_available_caps() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;

        registry.add_caps_to_cache(vec![cap1]);

        let available = HashSet::new();

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:a", "media:b", 5, 10).await.unwrap();

        assert!(paths.is_empty(), "Should find no paths when no caps are available");
        Ok(())
    }

    // TEST774: Tests get_reachable_targets() only considers available caps for reachability
    // Verifies that target specs are only reachable via caps in the availability filter
    #[tokio::test]
    async fn test774_get_reachable_targets_filters_by_availability() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;
        let cap3 = make_test_cap("step3", "media:a", "media:d", "A to D")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone(), cap3.clone()]);

        let mut available = HashSet::new();
        available.insert(cap1.urn.to_string());
        available.insert(cap3.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let targets = builder.get_reachable_targets("media:a").await.unwrap();

        assert_eq!(targets.len(), 2, "Should find 2 reachable targets (B and D)");
        assert!(targets.contains(&"media:b".to_string()), "B should be reachable");
        assert!(targets.contains(&"media:d".to_string()), "D should be reachable");
        assert!(!targets.contains(&"media:c".to_string()), "C should NOT be reachable (cap2 not available)");
        Ok(())
    }

    // TEST775: Tests find_path() selects from available caps when multiple paths exist
    // Verifies that find_path() respects availability filter and prefers available direct paths
    #[tokio::test]
    async fn test775_find_path_filters_by_availability() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;
        let cap3 = make_test_cap("direct", "media:a", "media:c", "A to C Direct")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone(), cap3.clone()]);

        let mut available = HashSet::new();
        available.insert(cap3.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let path = builder.find_path("media:a", "media:c").await.unwrap();

        assert_eq!(path.len(), 1, "Should find path with 1 step (direct)");
        assert!(path[0].contains("op=direct"), "Should use the direct cap: {}", path[0]);
        Ok(())
    }

    // TEST776: Tests find_path() returns error when required caps are filtered out by availability
    // Verifies that "No path found" error is returned when filter blocks the only viable path
    #[tokio::test]
    async fn test776_find_path_returns_error_when_path_unavailable() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let cap1 = make_test_cap("step1", "media:a", "media:b", "A to B")?;
        let cap2 = make_test_cap("step2", "media:b", "media:c", "B to C")?;

        registry.add_caps_to_cache(vec![cap1.clone(), cap2.clone()]);

        let mut available = HashSet::new();
        available.insert(cap1.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let result = builder.find_path("media:a", "media:c").await;

        assert!(result.is_err(), "Should fail when path requires unavailable caps");
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No path found"), "Error should indicate no path found: {}", err);
        Ok(())
    }

    // ==========================================================================
    // TYPE MISMATCH TESTS
    // ==========================================================================

    // TEST777: Tests type checking prevents using PDF-specific cap with PNG input
    // Verifies that media type compatibility is enforced during pathfinding (PNG cannot use PDF cap)
    #[tokio::test]
    async fn test777_type_mismatch_pdf_cap_does_not_match_png_input() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let pdf_to_text = make_test_cap("pdf2text", "media:pdf", "media:textable", "PDF to Text")?;

        registry.add_caps_to_cache(vec![pdf_to_text.clone()]);

        let mut available = HashSet::new();
        available.insert(pdf_to_text.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let result = builder.find_path("media:png", "media:textable").await;

        assert!(result.is_err(), "Should NOT find path from PNG to text via PDF cap");
        Ok(())
    }

    // TEST778: Tests type checking prevents using PNG-specific cap with PDF input
    // Verifies that media type compatibility is enforced during pathfinding (PDF cannot use PNG cap)
    #[tokio::test]
    async fn test778_type_mismatch_png_cap_does_not_match_pdf_input() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let png_to_thumb = make_test_cap("png2thumb", "media:png", "media:thumbnail", "PNG to Thumbnail")?;

        registry.add_caps_to_cache(vec![png_to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(png_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let result = builder.find_path("media:pdf", "media:thumbnail").await;

        assert!(result.is_err(), "Should NOT find path from PDF to thumbnail via PNG cap");
        Ok(())
    }

    // TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps
    // Verifies that PNG and PDF inputs reach different targets based on cap input type requirements
    #[tokio::test]
    async fn test779_get_reachable_targets_respects_type_matching() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let pdf_to_text = make_test_cap("pdf2text", "media:pdf", "media:textable", "PDF to Text")?;
        let png_to_thumb = make_test_cap("png2thumb", "media:png", "media:thumbnail", "PNG to Thumbnail")?;

        registry.add_caps_to_cache(vec![pdf_to_text.clone(), png_to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(pdf_to_text.urn.to_string());
        available.insert(png_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let png_targets = builder.get_reachable_targets("media:png").await.unwrap();
        assert_eq!(png_targets.len(), 1, "PNG should only reach 1 target");
        assert!(png_targets.contains(&"media:thumbnail".to_string()), "PNG should reach thumbnail");
        assert!(!png_targets.contains(&"media:textable".to_string()), "PNG should NOT reach text (type mismatch)");

        let pdf_targets = builder.get_reachable_targets("media:pdf").await.unwrap();
        assert_eq!(pdf_targets.len(), 1, "PDF should only reach 1 target");
        assert!(pdf_targets.contains(&"media:textable".to_string()), "PDF should reach text");
        assert!(!pdf_targets.contains(&"media:thumbnail".to_string()), "PDF should NOT reach thumbnail (type mismatch)");
        Ok(())
    }

    // TEST780: Tests get_reachable_targets_with_metadata() respects type compatibility constraints
    // Verifies that reachable target metadata only includes type-compatible transformations
    #[tokio::test]
    async fn test780_reachable_targets_with_metadata_respects_type_matching() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let pdf_to_text = make_test_cap("pdf2text", "media:pdf", "media:textable", "PDF to Text")?;
        let png_to_thumb = make_test_cap("png2thumb", "media:png", "media:thumbnail", "PNG to Thumbnail")?;

        registry.add_caps_to_cache(vec![pdf_to_text.clone(), png_to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(pdf_to_text.urn.to_string());
        available.insert(png_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let png_targets = builder.get_reachable_targets_with_metadata("media:png", 5).await.unwrap();
        assert_eq!(png_targets.len(), 1, "PNG should only reach 1 target with metadata");
        assert_eq!(png_targets[0].media_spec, "media:thumbnail", "PNG target should be thumbnail");

        let pdf_targets = builder.get_reachable_targets_with_metadata("media:pdf", 5).await.unwrap();
        assert_eq!(pdf_targets.len(), 1, "PDF should only reach 1 target with metadata");
        assert_eq!(pdf_targets[0].media_spec, "media:textable", "PDF target should be text");
        Ok(())
    }

    // TEST781: Tests find_all_paths() enforces type compatibility across multi-step chains
    // Verifies that paths are only found when all intermediate types are compatible
    #[tokio::test]
    async fn test781_find_all_paths_respects_type_chain() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();
        let resize_png = make_test_cap("resize", "media:png", "media:resized-png", "Resize PNG")?;
        let to_thumb = make_test_cap("thumb", "media:resized-png", "media:thumbnail", "To Thumbnail")?;

        registry.add_caps_to_cache(vec![resize_png.clone(), to_thumb.clone()]);

        let mut available = HashSet::new();
        available.insert(resize_png.urn.to_string());
        available.insert(to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let png_paths = builder.find_all_paths("media:png", "media:thumbnail", 5, 10).await.unwrap();
        assert_eq!(png_paths.len(), 1, "Should find 1 path from PNG to thumbnail");
        assert_eq!(png_paths[0].steps.len(), 2, "Path should have 2 steps");

        let pdf_paths = builder.find_all_paths("media:pdf", "media:thumbnail", 5, 10).await.unwrap();
        assert!(pdf_paths.is_empty(), "Should find NO paths from PDF to thumbnail (type mismatch)");
        Ok(())
    }

    // ==========================================================================
    // PATH COHERENCE SCORING
    // ==========================================================================

    // TEST782: Tests coherence scoring gives 0 deviations for direct single-step paths
    // Verifies that paths going directly from source to target without detours have perfect coherence
    #[test]
    fn test782_coherence_score_zero_for_direct_path() {
        let path = CapChainPathInfo {
            steps: vec![CapChainStepInfo {
                cap_urn: "cap:in=\"media:txt;textable\";op=convert;out=\"media:md;textable\"".to_string(),
                from_spec: "media:txt;textable".to_string(),
                to_spec: "media:md;textable".to_string(),
                title: "Convert TXT to MD".to_string(),
                file_path_arg_name: None,
            }],
            source_spec: "media:txt;textable".to_string(),
            target_spec: "media:md;textable".to_string(),
            total_steps: 1,
            description: "txt → md".to_string(),
        };

        let source = MediaUrn::from_string("media:txt;textable").unwrap();
        let target = MediaUrn::from_string("media:md;textable").unwrap();
        let (deviation, steps) = CapPlanBuilder::path_coherence_score(&path, &source, &target);

        assert_eq!(deviation, 0, "Direct path should have 0 deviations");
        assert_eq!(steps, 1, "Path should have 1 step");
    }

    // TEST783: Tests coherence scoring penalizes paths through semantically unrelated intermediates
    // Verifies that going from textable→thumbnail→textable incurs deviation penalty (thumbnail unrelated)
    #[test]
    fn test783_coherence_score_penalizes_unrelated_intermediate() {
        let path = CapChainPathInfo {
            steps: vec![
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:txt;textable\";op=to_thumb;out=\"media:thumbnail\"".to_string(),
                    from_spec: "media:txt;textable".to_string(),
                    to_spec: "media:thumbnail".to_string(),
                    title: "To Thumbnail".to_string(),
                    file_path_arg_name: None,
                },
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:thumbnail\";op=to_rst;out=\"media:rst;textable\"".to_string(),
                    from_spec: "media:thumbnail".to_string(),
                    to_spec: "media:rst;textable".to_string(),
                    title: "To RST".to_string(),
                    file_path_arg_name: None,
                },
            ],
            source_spec: "media:txt;textable".to_string(),
            target_spec: "media:rst;textable".to_string(),
            total_steps: 2,
            description: "txt → thumbnail → rst".to_string(),
        };

        let source = MediaUrn::from_string("media:txt;textable").unwrap();
        let target = MediaUrn::from_string("media:rst;textable").unwrap();
        let (deviation, steps) = CapPlanBuilder::path_coherence_score(&path, &source, &target);

        assert_eq!(deviation, 1, "Path through unrelated thumbnail should have 1 deviation");
        assert_eq!(steps, 2);
    }

    // TEST784: Tests coherence scoring does not penalize paths through semantically related intermediates
    // Verifies that going through a supertype (txt→textable→md) maintains coherence with 0 deviations
    #[test]
    fn test784_coherence_score_related_intermediate_not_penalized() {
        let path = CapChainPathInfo {
            steps: vec![
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:txt;textable\";op=strip;out=\"media:textable\"".to_string(),
                    from_spec: "media:txt;textable".to_string(),
                    to_spec: "media:textable".to_string(),
                    title: "Strip format".to_string(),
                    file_path_arg_name: None,
                },
                CapChainStepInfo {
                    cap_urn: "cap:in=\"media:textable\";op=to_md;out=\"media:md;textable\"".to_string(),
                    from_spec: "media:textable".to_string(),
                    to_spec: "media:md;textable".to_string(),
                    title: "To MD".to_string(),
                    file_path_arg_name: None,
                },
            ],
            source_spec: "media:txt;textable".to_string(),
            target_spec: "media:md;textable".to_string(),
            total_steps: 2,
            description: "txt → textable → md".to_string(),
        };

        let source = MediaUrn::from_string("media:txt;textable").unwrap();
        let target = MediaUrn::from_string("media:md;textable").unwrap();
        let (deviation, _) = CapPlanBuilder::path_coherence_score(&path, &source, &target);

        assert_eq!(deviation, 0, "Path through related supertype (textable) should have 0 deviations");
    }

    // TEST785: Tests find_all_paths() filters out deviating paths when coherent alternatives exist
    // Verifies that semantically wandering paths are excluded if direct coherent paths are available
    #[tokio::test]
    async fn test785_find_all_paths_filters_deviating_when_coherent_exists() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        let direct = make_test_cap("txt2rst", "media:txt;textable", "media:rst;textable", "Direct TXT to RST")?;
        let to_thumb = make_test_cap("txt2thumb", "media:txt;textable", "media:thumbnail", "TXT to Thumbnail")?;
        let thumb_to_rst = make_test_cap("thumb2rst", "media:thumbnail", "media:rst;textable", "Thumbnail to RST")?;

        registry.add_caps_to_cache(vec![direct.clone(), to_thumb.clone(), thumb_to_rst.clone()]);

        let mut available = HashSet::new();
        available.insert(direct.urn.to_string());
        available.insert(to_thumb.urn.to_string());
        available.insert(thumb_to_rst.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:txt;textable", "media:rst;textable", 5, 10).await.unwrap();

        assert_eq!(paths.len(), 1, "Deviating path should be filtered out when coherent path exists");
        assert_eq!(paths[0].steps.len(), 1, "Remaining path should be the direct 1-step path");
        Ok(())
    }

    // TEST786: Tests find_all_paths() keeps all paths when no coherent path exists
    // Verifies that all deviating paths are returned if they're the only viable options
    #[tokio::test]
    async fn test786_find_all_paths_keeps_all_when_all_deviate() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        let txt_to_thumb = make_test_cap("txt2thumb", "media:txt;textable", "media:thumbnail", "TXT to Thumb")?;
        let thumb_to_emb = make_test_cap("thumb2emb", "media:thumbnail", "media:embeddings", "Thumb to Embeddings")?;

        let txt_to_audio = make_test_cap("txt2audio", "media:txt;textable", "media:audio", "TXT to Audio")?;
        let audio_to_thumb = make_test_cap("audio2thumb", "media:audio", "media:thumbnail", "Audio to Thumb")?;

        registry.add_caps_to_cache(vec![
            txt_to_thumb.clone(), thumb_to_emb.clone(),
            txt_to_audio.clone(), audio_to_thumb.clone(),
        ]);

        let mut available = HashSet::new();
        available.insert(txt_to_thumb.urn.to_string());
        available.insert(thumb_to_emb.urn.to_string());
        available.insert(txt_to_audio.urn.to_string());
        available.insert(audio_to_thumb.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:txt;textable", "media:embeddings", 5, 10).await.unwrap();

        assert!(paths.len() >= 2, "When no coherent path exists, all deviating paths should be kept (got {})", paths.len());
        assert_eq!(paths[0].steps.len(), 2, "First path should be the shorter 2-step one");
        Ok(())
    }

    // TEST787: Tests find_all_paths() sorts coherent paths by length, preferring shorter ones
    // Verifies that among multiple coherent paths, the shortest is ranked first
    #[tokio::test]
    async fn test787_find_all_paths_coherent_sorting_prefers_shorter() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        let direct = make_test_cap("txt2md", "media:txt;textable", "media:md;textable", "Direct")?;
        let strip = make_test_cap("strip", "media:txt;textable", "media:textable", "Strip Format")?;
        let to_md = make_test_cap("to_md", "media:textable", "media:md;textable", "To MD")?;

        registry.add_caps_to_cache(vec![direct.clone(), strip.clone(), to_md.clone()]);

        let mut available = HashSet::new();
        available.insert(direct.urn.to_string());
        available.insert(strip.urn.to_string());
        available.insert(to_md.urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        let paths = builder.find_all_paths("media:txt;textable", "media:md;textable", 5, 10).await.unwrap();

        assert!(paths.len() >= 2, "Should find at least 2 paths (got {})", paths.len());
        assert_eq!(paths[0].steps.len(), 1, "Shortest coherent path should be first");
        Ok(())
    }

    // ==========================================================================
    // URN CANONICALIZATION TESTS
    // ==========================================================================

    // TEST1100: Tests that CapUrn normalizes media URN tags to canonical order
    // This is the root cause fix for caps not matching when plugins report URNs with
    // different tag ordering than the registry (e.g., "record;textable" vs "textable;record")
    #[test]
    fn test1100_cap_urn_normalizes_media_urn_tag_order() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        // Create two CapUrns with different tag ordering in the output media URN
        let urn1 = CapUrn::from_string("cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;record;textable\"")?;
        let urn2 = CapUrn::from_string("cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;textable;record\"")?;

        // After normalization, both should produce the same canonical string
        assert_eq!(
            urn1.to_string(),
            urn2.to_string(),
            "URNs with different tag ordering should normalize to the same canonical form"
        );

        // The canonical form should have tags in alphabetical order
        let canonical = urn1.to_string();
        assert!(
            canonical.contains("record;textable") || canonical.contains("textable;record"),
            "Canonical form should contain the tags: {}", canonical
        );

        Ok(())
    }

    // TEST1101: Tests that is_cap_available matches URNs regardless of original tag ordering
    // Verifies that a cap from the registry (with one tag order) matches an available cap
    // from plugins (with different tag order) after normalization
    #[tokio::test]
    async fn test1101_is_cap_available_matches_normalized_urns() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        // Registry has cap with tags in one order
        let registry_cap = make_test_cap(
            "extract_metadata",
            "media:pdf",
            "media:file-metadata;textable;record",  // textable comes first
            "Extract PDF Metadata"
        )?;
        registry.add_caps_to_cache(vec![registry_cap.clone()]);

        // Plugin reports the same cap but with tags in different order
        // After normalization via CapUrn::from_string().to_string(), order should match
        let plugin_urn_str = "cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;record;textable\"";
        let plugin_urn = CapUrn::from_string(plugin_urn_str)?;
        let normalized_plugin_urn = plugin_urn.to_string();

        let mut available = HashSet::new();
        available.insert(normalized_plugin_urn);

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // The cap should be found as available because normalization makes URNs match
        assert!(
            builder.is_cap_available(&registry_cap.urn.to_string()),
            "Cap should be available after URN normalization. Registry: {}, Available set should contain normalized form",
            registry_cap.urn.to_string()
        );

        Ok(())
    }

    // TEST1102: Tests that pathfinding works when plugin and registry have different tag ordering
    // This is an integration test for the full fix: plugins report URNs with one order,
    // registry has another order, but paths are still found because URNs are normalized
    #[tokio::test]
    async fn test1102_pathfinding_works_with_different_tag_ordering() -> Result<(), crate::urn::cap_urn::CapUrnError> {
        let registry = CapRegistry::new_for_test();

        // Registry cap with output tags in one order
        let pdf_to_metadata = make_test_cap(
            "extract_metadata",
            "media:pdf",
            "media:file-metadata;textable;record",  // alphabetical: record, textable -> becomes record;textable
            "Extract PDF Metadata"
        )?;

        registry.add_caps_to_cache(vec![pdf_to_metadata.clone()]);

        // Simulate plugin reporting same cap with different tag order in output
        // The key insight: CapUrn::new() now normalizes, so both produce same canonical form
        let plugin_urn = CapUrn::from_string(
            "cap:in=media:pdf;op=extract_metadata;out=\"media:file-metadata;record;textable\""
        )?;

        let mut available = HashSet::new();
        available.insert(plugin_urn.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // Should find reachable targets because URNs match after normalization
        let targets = builder.get_reachable_targets("media:pdf").await.unwrap();

        assert!(
            !targets.is_empty(),
            "Should find reachable targets when URNs are normalized. \
             This test verifies the fix for plugin/registry tag ordering mismatch."
        );

        Ok(())
    }

    // TEST1103: Tests that is_cap_available uses is_dispatchable correctly
    // The available cap (provider) must be dispatchable for the requested cap (request).
    // This tests the directionality: provider.is_dispatchable(&request)
    #[test]
    fn test1103_is_cap_available_uses_is_dispatchable_correctly() {
        // A more specific provider should be dispatchable for a general request
        let general_request = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text"
        ).unwrap();

        let specific_provider = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text;version=2"
        ).unwrap();

        // provider.is_dispatchable(&request) should be true: specific provider refines general request
        assert!(
            specific_provider.is_dispatchable(&general_request),
            "Specific provider should be dispatchable for general request"
        );

        // request.is_dispatchable(&provider) should be false: general request cannot handle specific provider's requirements
        assert!(
            !general_request.is_dispatchable(&specific_provider),
            "General request should NOT be dispatchable for specific provider (missing version tag)"
        );

        // Now test via is_cap_available
        let registry = CapRegistry::new_for_test();
        // Registry has the general request
        let registry_cap = Cap::new(general_request.clone(), "Extract".to_string(), "extract".to_string());
        registry.add_caps_to_cache(vec![registry_cap]);

        // Plugin provides the specific provider
        let mut available = HashSet::new();
        available.insert(specific_provider.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // is_cap_available checks: does any available cap (provider) dispatch the request?
        // This should be TRUE because specific_provider is dispatchable for general_request
        assert!(
            builder.is_cap_available(&general_request.to_string()),
            "is_cap_available should return true when available provider can dispatch requested cap"
        );
    }

    // TEST1104: Tests that is_cap_available rejects when provider cannot dispatch request
    #[test]
    fn test1104_is_cap_available_rejects_non_dispatchable() {
        // Request requires specific tag that provider doesn't have
        let request = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text;required=yes"
        ).unwrap();

        let provider = CapUrn::from_string(
            "cap:in=media:pdf;op=extract;out=media:text"  // missing required=yes
        ).unwrap();

        // provider is NOT dispatchable for request (missing required tag that request needs)
        assert!(
            !provider.is_dispatchable(&request),
            "Provider missing required tag should not be dispatchable for request"
        );

        let registry = CapRegistry::new_for_test();
        let registry_cap = Cap::new(request.clone(), "Extract".to_string(), "extract".to_string());
        registry.add_caps_to_cache(vec![registry_cap]);

        let mut available = HashSet::new();
        available.insert(provider.to_string());

        let builder = create_test_plan_builder_with_registry(registry)
            .with_available_caps(available);

        // is_cap_available should return FALSE because provider cannot dispatch request
        assert!(
            !builder.is_cap_available(&request.to_string()),
            "is_cap_available should return false when provider cannot dispatch requested cap"
        );
    }
}
