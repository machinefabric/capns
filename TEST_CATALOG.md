# CapDag (Rust) Test Catalog

**Total Tests:** 1008

All test numbers are unique.

This catalog lists all numbered tests in the CapDag (Rust) codebase.

| Test # | Function Name | Description | File |
|--------|---------------|-------------|------|
| test001 | `test001_cap_urn_creation` | TEST001: Test that cap URN is created with tags parsed correctly and direction specs accessible | src/urn/cap_urn.rs:954 |
| test002 | `test002_direction_specs_default_to_wildcard` | TEST002: Test that missing 'in' or 'out' defaults to media: wildcard | src/urn/cap_urn.rs:966 |
| test003 | `test003_direction_matching` | TEST003: Test that direction specs must match exactly, different in/out types don't match, wildcard matches any | src/urn/cap_urn.rs:988 |
| test004 | `test004_unquoted_values_lowercased` | TEST004: Test that unquoted keys and values are normalized to lowercase | src/urn/cap_urn.rs:1017 |
| test005 | `test005_quoted_values_preserve_case` | TEST005: Test that quoted values preserve case while unquoted are lowercased | src/urn/cap_urn.rs:1038 |
| test006 | `test006_quoted_value_special_chars` | TEST006: Test that quoted values can contain special characters (semicolons, equals, spaces) | src/urn/cap_urn.rs:1057 |
| test007 | `test007_quoted_value_escape_sequences` | TEST007: Test that escape sequences in quoted values (\" and \\) are parsed correctly | src/urn/cap_urn.rs:1076 |
| test008 | `test008_mixed_quoted_unquoted` | TEST008: Test that mixed quoted and unquoted values in same URN parse correctly | src/urn/cap_urn.rs:1095 |
| test009 | `test009_unterminated_quote_error` | TEST009: Test that unterminated quote produces UnterminatedQuote error | src/urn/cap_urn.rs:1103 |
| test010 | `test010_invalid_escape_sequence_error` | TEST010: Test that invalid escape sequences (like \n, \x) produce InvalidEscapeSequence error | src/urn/cap_urn.rs:1113 |
| test011 | `test011_serialization_smart_quoting` | TEST011: Test that serialization uses smart quoting (no quotes for simple lowercase, quotes for special chars/uppercase) | src/urn/cap_urn.rs:1130 |
| test012 | `test012_round_trip_simple` | TEST012: Test that simple cap URN round-trips (parse -> serialize -> parse equals original) | src/urn/cap_urn.rs:1165 |
| test013 | `test013_round_trip_quoted` | TEST013: Test that quoted values round-trip preserving case and spaces | src/urn/cap_urn.rs:1175 |
| test014 | `test014_round_trip_escapes` | TEST014: Test that escape sequences round-trip correctly | src/urn/cap_urn.rs:1189 |
| test015 | `test015_cap_prefix_required` | TEST015: Test that cap: prefix is required and case-insensitive | src/urn/cap_urn.rs:1203 |
| test016 | `test016_trailing_semicolon_equivalence` | TEST016: Test that trailing semicolon is equivalent (same hash, same string, matches) | src/urn/cap_urn.rs:1226 |
| test017 | `test017_tag_matching` | TEST017: Test tag matching: exact match, subset match, wildcard match, value mismatch | src/urn/cap_urn.rs:1259 |
| test018 | `test018_matching_case_sensitive_values` | TEST018: Test that quoted values with different case do NOT match (case-sensitive) | src/urn/cap_urn.rs:1286 |
| test019 | `test019_missing_tag_handling` | TEST019: Missing tag in instance causes rejection — pattern's tags are constraints | src/urn/cap_urn.rs:1300 |
| test020 | `test020_specificity` | TEST020: Test specificity calculation (direction specs use MediaUrn tag count, wildcards don't count) | src/urn/cap_urn.rs:1319 |
| test021 | `test021_builder` | TEST021: Test builder creates cap URN with correct tags and direction specs | src/urn/cap_urn.rs:1339 |
| test022 | `test022_builder_requires_direction` | TEST022: Test builder requires both in_spec and out_spec | src/urn/cap_urn.rs:1356 |
| test023 | `test023_builder_preserves_case` | TEST023: Test builder lowercases keys but preserves value case | src/urn/cap_urn.rs:1381 |
| test024 | `test024_directional_accepts` | TEST024: Directional accepts — pattern's tags are constraints, instance must satisfy | src/urn/cap_urn.rs:1395 |
| test025 | `test025_best_match` | TEST025: Test find_best_match returns most specific matching cap | src/urn/cap_urn.rs:1426 |
| test026 | `test026_merge_and_subset` | TEST026: Test merge combines tags from both caps, subset keeps only specified tags | src/urn/cap_urn.rs:1442 |
| test027 | `test027_wildcard_tag` | TEST027: Test with_wildcard_tag sets tag to wildcard, including in/out | src/urn/cap_urn.rs:1466 |
| test028 | `test028_empty_cap_urn_defaults_to_wildcard` | TEST028: Test empty cap URN defaults to media: wildcard | src/urn/cap_urn.rs:1482 |
| test029 | `test029_minimal_cap_urn` | TEST029: Test minimal valid cap URN has just in and out, empty tags | src/urn/cap_urn.rs:1496 |
| test030 | `test030_extended_character_support` | TEST030: Test extended characters (forward slashes, colons) in tag values | src/urn/cap_urn.rs:1507 |
| test031 | `test031_wildcard_restrictions` | TEST031: Test wildcard rejected in keys but accepted in values | src/urn/cap_urn.rs:1520 |
| test032 | `test032_duplicate_key_rejection` | TEST032: Test duplicate keys are rejected with DuplicateKey error | src/urn/cap_urn.rs:1531 |
| test033 | `test033_numeric_key_restriction` | TEST033: Test pure numeric keys rejected, mixed alphanumeric allowed, numeric values allowed | src/urn/cap_urn.rs:1541 |
| test034 | `test034_empty_value_error` | TEST034: Test empty values are rejected | src/urn/cap_urn.rs:1555 |
| test035 | `test035_has_tag_case_sensitive` | TEST035: Test has_tag is case-sensitive for values, case-insensitive for keys, works for in/out | src/urn/cap_urn.rs:1562 |
| test036 | `test036_with_tag_preserves_value` | TEST036: Test with_tag preserves value case | src/urn/cap_urn.rs:1583 |
| test037 | `test037_with_tag_rejects_empty_value` | TEST037: Test with_tag rejects empty value | src/urn/cap_urn.rs:1592 |
| test038 | `test038_semantic_equivalence` | TEST038: Test semantic equivalence of unquoted and quoted simple lowercase values | src/urn/cap_urn.rs:1601 |
| test039 | `test039_get_tag_returns_direction_specs` | TEST039: Test get_tag returns direction specs (in/out) with case-insensitive lookup | src/urn/cap_urn.rs:1614 |
| test040 | `test040_matching_semantics_test1_exact_match` | TEST040: Matching semantics - exact match succeeds | src/urn/cap_urn.rs:1642 |
| test041 | `test041_matching_semantics_test2_cap_missing_tag` | TEST041: Matching semantics - cap missing tag matches (implicit wildcard) | src/urn/cap_urn.rs:1651 |
| test042 | `test042_matching_semantics_test3_cap_has_extra_tag` | TEST042: Pattern rejects instance missing required tags | src/urn/cap_urn.rs:1663 |
| test043 | `test043_matching_semantics_test4_request_has_wildcard` | TEST043: Matching semantics - request wildcard matches specific cap value | src/urn/cap_urn.rs:1674 |
| test044 | `test044_matching_semantics_test5_cap_has_wildcard` | TEST044: Matching semantics - cap wildcard matches specific request value | src/urn/cap_urn.rs:1686 |
| test045 | `test045_matching_semantics_test6_value_mismatch` | TEST045: Matching semantics - value mismatch does not match | src/urn/cap_urn.rs:1695 |
| test046 | `test046_matching_semantics_test7_fallback_pattern` | TEST046: Matching semantics - fallback pattern (cap missing tag = implicit wildcard) | src/urn/cap_urn.rs:1707 |
| test047 | `test047_matching_semantics_test7b_thumbnail_void_input` | TEST047: Matching semantics - thumbnail fallback with void input | src/urn/cap_urn.rs:1728 |
| test048 | `test048_matching_semantics_test8_wildcard_direction_matches_anything` | TEST048: Matching semantics - wildcard direction matches anything | src/urn/cap_urn.rs:1749 |
| test049 | `test049_matching_semantics_test9_cross_dimension_independence` | TEST049: Non-overlapping tags — neither direction accepts | src/urn/cap_urn.rs:1765 |
| test050 | `test050_matching_semantics_test10_direction_mismatch` | TEST050: Matching semantics - direction mismatch prevents matching | src/urn/cap_urn.rs:1775 |
| test051 | `test051_input_validation_success` | TEST051: Test input validation succeeds with valid positional argument | src/cap/validation.rs:1065 |
| test052 | `test052_input_validation_missing_required` | TEST052: Test input validation fails with MissingRequiredArgument when required arg missing | src/cap/validation.rs:1086 |
| test053 | `test053_input_validation_wrong_type` | TEST053: Test input validation fails with InvalidArgumentType when wrong type provided | src/cap/validation.rs:1114 |
| test054 | `test054_xv5_inline_spec_redefinition_detected` | TEST054: XV5 - Test inline media spec redefinition of existing registry spec is detected and rejected | src/cap/validation.rs:1157 |
| test055 | `test055_xv5_new_inline_spec_allowed` | TEST055: XV5 - Test new inline media spec (not in registry) is allowed | src/cap/validation.rs:1192 |
| test056 | `test056_xv5_empty_media_specs_allowed` | TEST056: XV5 - Test empty media_specs (no inline specs) passes XV5 validation | src/cap/validation.rs:1223 |
| test060 | `test060_wrong_prefix_fails` | TEST060: Test wrong prefix fails with InvalidPrefix error showing expected and actual prefix | src/urn/media_urn.rs:614 |
| test061 | `test061_is_binary` | TEST061: Test is_binary returns true when textable tag is absent (binary = not textable) | src/urn/media_urn.rs:627 |
| test062 | `test062_is_record` | TEST062: Test is_record returns true when record marker tag is present indicating key-value structure | src/urn/media_urn.rs:644 |
| test063 | `test063_is_scalar` | TEST063: Test is_scalar returns true when list marker tag is absent (scalar is default) | src/urn/media_urn.rs:657 |
| test064 | `test064_is_list` | TEST064: Test is_list returns true when list marker tag is present indicating ordered collection | src/urn/media_urn.rs:672 |
| test065 | `test065_is_opaque` | TEST065: Test is_opaque returns true when record marker is absent (opaque is default) | src/urn/media_urn.rs:685 |
| test066 | `test066_is_json` | TEST066: Test is_json returns true only when json marker tag is present for JSON representation | src/urn/media_urn.rs:699 |
| test067 | `test067_is_text` | TEST067: Test is_text returns true only when textable marker tag is present | src/urn/media_urn.rs:710 |
| test068 | `test068_is_void` | TEST068: Test is_void returns true when void flag or type=void tag is present | src/urn/media_urn.rs:723 |
| test071 | `test071_to_string_roundtrip` | TEST071: Test to_string roundtrip ensures serialization and deserialization preserve URN structure | src/urn/media_urn.rs:730 |
| test072 | `test072_constants_parse` | TEST072: Test all media URN constants parse successfully as valid media URNs | src/urn/media_urn.rs:740 |
| test073 | `test073_extension_helpers` | TEST073: Test extension helper functions create media URNs with ext tag and correct format | src/urn/media_urn.rs:774 |
| test074 | `test074_media_urn_matching` | TEST074: Test media URN conforms_to using tagged URN semantics with specific and generic requirements | src/urn/media_urn.rs:790 |
| test075 | `test075_matching` | TEST075: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests | src/urn/media_urn.rs:810 |
| test076 | `test076_specificity` | TEST076: Test specificity increases with more tags for ranking conformance | src/urn/media_urn.rs:826 |
| test077 | `test077_serde_roundtrip` | TEST077: Test serde roundtrip serializes to JSON string and deserializes back correctly | src/urn/media_urn.rs:845 |
| test078 | `test078_object_does_not_conform_to_string` | TEST078: conforms_to behavior between MEDIA_OBJECT and MEDIA_STRING | src/urn/media_urn.rs:861 |
| test088 | `test088_resolve_from_registry_str` | TEST088: Test resolving string media URN from registry returns correct media type and profile | src/media/spec.rs:672 |
| test089 | `test089_resolve_from_registry_obj` | TEST089: Test resolving JSON media URN from registry returns JSON media type | src/media/spec.rs:682 |
| test090 | `test090_resolve_from_registry_binary` | TEST090: Test resolving binary media URN returns octet-stream and is_binary true | src/media/spec.rs:691 |
| test091 | `test091_resolve_custom_media_spec` | TEST091: Test resolving custom media URN from local media_specs takes precedence over registry | src/media/spec.rs:716 |
| test092 | `test092_resolve_custom_with_schema` | TEST092: Test resolving custom record media spec with schema from local media_specs | src/media/spec.rs:746 |
| test093 | `test093_resolve_unresolvable_fails_hard` | TEST093: Test resolving unknown media URN fails with UnresolvableMediaUrn error | src/media/spec.rs:781 |
| test094 | `test094_local_overrides_registry` | TEST094: Test local media_specs definition overrides registry definition for same URN | src/media/spec.rs:795 |
| test095 | `test095_media_spec_def_serialize` | TEST095: Test MediaSpecDef serializes with required fields and skips None fields | src/media/spec.rs:828 |
| test096 | `test096_media_spec_def_deserialize` | TEST096: Test deserializing MediaSpecDef from JSON object | src/media/spec.rs:854 |
| test097 | `test097_validate_no_duplicate_urns_catches_duplicates` | TEST097: Test duplicate URN validation catches duplicates | src/media/spec.rs:869 |
| test098 | `test098_validate_no_duplicate_urns_passes_for_unique` | TEST098: Test duplicate URN validation passes for unique URNs | src/media/spec.rs:885 |
| test099 | `test099_resolved_is_binary` | TEST099: Test ResolvedMediaSpec is_binary returns true when textable tag is absent | src/media/spec.rs:900 |
| test100 | `test100_resolved_is_record` | TEST100: Test ResolvedMediaSpec is_record returns true when record marker is present | src/media/spec.rs:920 |
| test101 | `test101_resolved_is_scalar` | TEST101: Test ResolvedMediaSpec is_scalar returns true when list marker is absent | src/media/spec.rs:941 |
| test102 | `test102_resolved_is_list` | TEST102: Test ResolvedMediaSpec is_list returns true when list marker is present | src/media/spec.rs:961 |
| test103 | `test103_resolved_is_json` | TEST103: Test ResolvedMediaSpec is_json returns true when json tag is present | src/media/spec.rs:981 |
| test104 | `test104_resolved_is_text` | TEST104: Test ResolvedMediaSpec is_text returns true when textable tag is present | src/media/spec.rs:1001 |
| test105 | `test105_metadata_propagation` | TEST105: Test metadata propagates from media spec def to resolved media spec | src/media/spec.rs:1025 |
| test106 | `test106_metadata_with_validation` | TEST106: Test metadata and validation can coexist in media spec definition | src/media/spec.rs:1054 |
| test107 | `test107_extensions_propagation` | TEST107: Test extensions field propagates from media spec def to resolved | src/media/spec.rs:1101 |
| test108 | `test108_cap_creation` | TEST108: Test creating new cap with URN, title, and command verifies correct initialization | src/cap/definition.rs:920 |
| test109 | `test109_cap_with_metadata` | TEST109: Test creating cap with metadata initializes and retrieves metadata correctly | src/cap/definition.rs:936 |
| test110 | `test110_cap_matching` | TEST110: Test cap matching with subset semantics for request fulfillment | src/cap/definition.rs:953 |
| test111 | `test111_cap_title` | TEST111: Test getting and setting cap title updates correctly | src/cap/definition.rs:966 |
| test112 | `test112_cap_definition_equality` | TEST112: Test cap equality based on URN and title matching | src/cap/definition.rs:980 |
| test113 | `test113_cap_stdin` | TEST113: Test cap stdin support via args with stdin source and serialization roundtrip | src/cap/definition.rs:995 |
| test114 | `test114_arg_source_types` | TEST114: Test ArgSource type variants stdin, position, and cli_flag with their accessors | src/cap/definition.rs:1028 |
| test115 | `test115_cap_arg_serialization` | TEST115: Test CapArg serialization and deserialization with multiple sources | src/cap/definition.rs:1053 |
| test116 | `test116_cap_arg_constructors` | TEST116: Test CapArg constructor methods basic and with_description create args correctly | src/cap/definition.rs:1078 |
| test117 | `test117_register_and_find_cap_set` | TEST117: Test registering cap set and finding by exact and subset matching | src/urn/cap_matrix.rs:982 |
| test118 | `test118_best_cap_set_selection` | TEST118: Test selecting best cap set based on specificity ranking  With is_dispatchable semantics: - Provider must satisfy ALL request constraints - General request matches specific provider (provider refines request) - Specific request does NOT match general provider (provider lacks constraints) | src/urn/cap_matrix.rs:1031 |
| test119 | `test119_invalid_urn_handling` | TEST119: Test invalid URN returns InvalidUrn error | src/urn/cap_matrix.rs:1095 |
| test120 | `test120_accepts_request` | TEST120: Test accepts_request checks if registry can handle a capability request | src/urn/cap_matrix.rs:1105 |
| test121 | `test121_cap_block_more_specific_wins` | TEST121: Test CapBlock selects more specific cap over less specific regardless of registry order | src/urn/cap_matrix.rs:1168 |
| test122 | `test122_cap_block_tie_goes_to_first` | TEST122: Test CapBlock breaks specificity ties by first registered registry | src/urn/cap_matrix.rs:1219 |
| test123 | `test123_cap_block_polls_all` | TEST123: Test CapBlock polls all registries to find most specific match | src/urn/cap_matrix.rs:1248 |
| test124 | `test124_cap_block_no_match` | TEST124: Test CapBlock returns error when no registries match the request | src/urn/cap_matrix.rs:1284 |
| test125 | `test125_cap_block_fallback_scenario` | TEST125: Test CapBlock prefers specific cartridge over generic provider fallback | src/urn/cap_matrix.rs:1297 |
| test126 | `test126_composite_can_method` | TEST126: Test composite can method returns CapCaller for capability execution | src/urn/cap_matrix.rs:1362 |
| test127 | `test127_cap_graph_basic_construction` | TEST127: Test CapGraph adds nodes and edges from capability definitions | src/urn/cap_matrix.rs:1397 |
| test128 | `test128_cap_graph_outgoing_incoming` | TEST128: Test CapGraph tracks outgoing and incoming edges for spec conversions | src/urn/cap_matrix.rs:1429 |
| test129 | `test129_cap_graph_can_convert` | TEST129: Test CapGraph detects direct and indirect conversion paths between specs | src/urn/cap_matrix.rs:1480 |
| test130 | `test130_cap_graph_find_path` | TEST130: Test CapGraph finds shortest path for spec conversion chain | src/urn/cap_matrix.rs:1535 |
| test131 | `test131_cap_graph_find_all_paths` | TEST131: Test CapGraph finds all conversion paths sorted by length | src/urn/cap_matrix.rs:1593 |
| test132 | `test132_cap_graph_get_direct_edges_sorted` | TEST132: Test CapGraph returns direct edges sorted by specificity | src/urn/cap_matrix.rs:1654 |
| test133 | `test133_cap_block_graph_integration` | TEST133: Test CapBlock graph integration with multiple registries and conversion paths | src/urn/cap_matrix.rs:1698 |
| test134 | `test134_cap_graph_stats` | TEST134: Test CapGraph stats provides counts of nodes and edges | src/urn/cap_matrix.rs:1785 |
| test135 | `test135_registry_creation` | TEST135: Test registry creation with temporary cache directory succeeds | src/cap/registry.rs:682 |
| test136 | `test136_cache_key_generation` | TEST136: Test cache key generation produces consistent hashes for same URN | src/cap/registry.rs:689 |
| test137 | `test137_parse_registry_json` | TEST137: Test parsing registry JSON without stdin args verifies cap structure | src/cap/registry.rs:768 |
| test138 | `test138_parse_registry_json_with_stdin` | TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction | src/cap/registry.rs:781 |
| test139 | `test139_url_keeps_cap_prefix_literal` | / Test that URL construction keeps "cap:" literal and only encodes the tags part / This guards against the bug where encoding "cap:" as "cap%3A" causes 404s TEST139: Test URL construction keeps cap prefix literal and only encodes tags part | src/cap/registry.rs:800 |
| test140 | `test140_url_encodes_quoted_media_urns` | / Test that media URNs in cap URNs are properly URL-encoded TEST140: Test URL encodes media URNs with proper percent encoding for special characters | src/cap/registry.rs:816 |
| test141 | `test141_exact_url_format` | / Test the URL format for a simple cap URN TEST141: Test exact URL format contains properly encoded media URN components | src/cap/registry.rs:836 |
| test142 | `test142_normalize_handles_different_tag_orders` | / Test that normalization handles various input formats TEST142: Test normalize handles different tag orders producing same canonical form | src/cap/registry.rs:853 |
| test143 | `test143_default_config` | TEST143: Test default config uses capdag.com or environment variable values | src/cap/registry.rs:871 |
| test144 | `test144_custom_registry_url` | TEST144: Test custom registry URL updates both registry and schema base URLs | src/cap/registry.rs:883 |
| test145 | `test145_custom_registry_and_schema_url` | TEST145: Test custom registry and schema URLs set independently | src/cap/registry.rs:892 |
| test146 | `test146_schema_url_not_overwritten_when_explicit` | TEST146: Test schema URL not overwritten when set explicitly before registry URL | src/cap/registry.rs:902 |
| test147 | `test147_registry_for_test_with_config` | TEST147: Test registry for test with custom config creates registry with specified URLs | src/cap/registry.rs:913 |
| test148 | `test148_cap_manifest_creation` | TEST148: Test creating cap manifest with name, version, description, and caps | src/bifaci/manifest.rs:102 |
| test149 | `test149_cap_manifest_with_author` | TEST149: Test cap manifest with author field sets author correctly | src/bifaci/manifest.rs:122 |
| test150 | `test150_cap_manifest_json_serialization` | TEST150: Test cap manifest JSON serialization and deserialization roundtrip | src/bifaci/manifest.rs:138 |
| test151 | `test151_cap_manifest_required_fields` | TEST151: Test cap manifest deserialization fails when required fields are missing | src/bifaci/manifest.rs:178 |
| test152 | `test152_cap_manifest_with_multiple_caps` | TEST152: Test cap manifest with multiple caps stores and retrieves all capabilities | src/bifaci/manifest.rs:191 |
| test153 | `test153_cap_manifest_empty_caps` | TEST153: Test cap manifest with empty caps list serializes and deserializes correctly | src/bifaci/manifest.rs:218 |
| test154 | `test154_cap_manifest_optional_author_field` | TEST154: Test cap manifest optional author field skipped in serialization when None | src/bifaci/manifest.rs:236 |
| test155 | `test155_component_metadata_trait` | TEST155: Test ComponentMetadata trait provides manifest and caps accessor methods | src/bifaci/manifest.rs:258 |
| test156 | `test156_stdin_source_data_creation` | TEST156: Test creating StdinSource Data variant with byte vector | src/cap/caller.rs:346 |
| test157 | `test157_stdin_source_file_reference_creation` | TEST157: Test creating StdinSource FileReference variant with all required fields | src/cap/caller.rs:358 |
| test158 | `test158_stdin_source_empty_data` | TEST158: Test StdinSource Data with empty vector stores and retrieves correctly | src/cap/caller.rs:389 |
| test159 | `test159_stdin_source_binary_content` | TEST159: Test StdinSource Data with binary content like PNG header bytes | src/cap/caller.rs:400 |
| test160 | `test160_stdin_source_clone` | TEST160: Test StdinSource Data clone creates independent copy with same data | src/cap/caller.rs:418 |
| test161 | `test161_stdin_source_file_reference_clone` | TEST161: Test StdinSource FileReference clone creates independent copy with same fields | src/cap/caller.rs:431 |
| test162 | `test162_stdin_source_debug` | TEST162: Test StdinSource Debug format displays variant type and relevant fields | src/cap/caller.rs:466 |
| test163 | `test163_argument_schema_validation_success` | TEST163: Test argument schema validation succeeds with valid JSON matching schema | src/cap/schema_validation.rs:233 |
| test164 | `test164_argument_schema_validation_failure` | TEST164: Test argument schema validation fails with JSON missing required fields | src/cap/schema_validation.rs:274 |
| test165 | `test165_output_schema_validation_success` | TEST165: Test output schema validation succeeds with valid JSON matching schema | src/cap/schema_validation.rs:314 |
| test166 | `test166_skip_validation_without_schema` | TEST166: Test validation skipped when resolved media spec has no schema | src/cap/schema_validation.rs:351 |
| test167 | `test167_unresolvable_media_urn_fails_hard` | TEST167: Test validation fails hard when media URN cannot be resolved from any source | src/cap/schema_validation.rs:373 |
| test168 | `test168_json_response` | TEST168: Test ResponseWrapper from JSON deserializes to correct structured type | src/cap/response.rs:253 |
| test169 | `test169_primitive_types` | TEST169: Test ResponseWrapper converts to primitive types integer, float, boolean, string | src/cap/response.rs:267 |
| test170 | `test170_binary_response` | TEST170: Test ResponseWrapper from binary stores and retrieves raw bytes correctly | src/cap/response.rs:287 |
| test171 | `test171_frame_type_roundtrip` | TEST171: Test all FrameType discriminants roundtrip through u8 conversion preserving identity | src/bifaci/frame.rs:988 |
| test172 | `test172_invalid_frame_type` | TEST172: Test FrameType::from_u8 returns None for values outside the valid discriminant range | src/bifaci/frame.rs:1012 |
| test173 | `test173_frame_type_discriminant_values` | TEST173: Test FrameType discriminant values match the wire protocol specification exactly | src/bifaci/frame.rs:1020 |
| test174 | `test174_message_id_uuid` | TEST174: Test MessageId::new_uuid generates valid UUID that roundtrips through string conversion | src/bifaci/frame.rs:1038 |
| test175 | `test175_message_id_uuid_uniqueness` | TEST175: Test two MessageId::new_uuid calls produce distinct IDs (no collisions) | src/bifaci/frame.rs:1047 |
| test176 | `test176_message_id_uint_has_no_uuid_string` | TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns None | src/bifaci/frame.rs:1055 |
| test177 | `test177_message_id_from_invalid_uuid_str` | TEST177: Test MessageId::from_uuid_str rejects invalid UUID strings | src/bifaci/frame.rs:1062 |
| test178 | `test178_message_id_as_bytes` | TEST178: Test MessageId::as_bytes produces correct byte representations for Uuid and Uint variants | src/bifaci/frame.rs:1070 |
| test179 | `test179_message_id_default_is_uuid` | TEST179: Test MessageId::default creates a UUID variant (not Uint) | src/bifaci/frame.rs:1083 |
| test180 | `test180_hello_frame` | TEST180: Test Frame::hello without manifest produces correct HELLO frame for host side | src/bifaci/frame.rs:1090 |
| test181 | `test181_hello_frame_with_manifest` | TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes for cartridge side | src/bifaci/frame.rs:1104 |
| test182 | `test182_req_frame` | TEST182: Test Frame::req stores cap URN, payload, and content_type correctly | src/bifaci/frame.rs:1116 |
| test184 | `test184_chunk_frame` | TEST184: Test Frame::chunk stores seq and payload for streaming (with stream_id) | src/bifaci/frame.rs:1132 |
| test185 | `test185_err_frame` | TEST185: Test Frame::err stores error code and message in metadata | src/bifaci/frame.rs:1148 |
| test186 | `test186_log_frame` | TEST186: Test Frame::log stores level and message in metadata | src/bifaci/frame.rs:1158 |
| test187 | `test187_end_frame_with_payload` | TEST187: Test Frame::end with payload sets eof and optional final payload | src/bifaci/frame.rs:1169 |
| test188 | `test188_end_frame_without_payload` | TEST188: Test Frame::end without payload still sets eof marker | src/bifaci/frame.rs:1179 |
| test189 | `test189_chunk_with_offset` | TEST189: Test chunk_with_offset sets offset on all chunks but len only on seq=0 (with stream_id) | src/bifaci/frame.rs:1189 |
| test190 | `test190_heartbeat_frame` | TEST190: Test Frame::heartbeat creates minimal frame with no payload or metadata | src/bifaci/frame.rs:1215 |
| test191 | `test191_error_accessors_on_non_err_frame` | TEST191: Test error_code and error_message return None for non-Err frame types | src/bifaci/frame.rs:1261 |
| test192 | `test192_log_accessors_on_non_log_frame` | TEST192: Test log_level and log_message return None for non-Log frame types | src/bifaci/frame.rs:1272 |
| test193 | `test193_hello_accessors_on_non_hello_frame` | TEST193: Test hello_max_frame and hello_max_chunk return None for non-Hello frame types | src/bifaci/frame.rs:1280 |
| test194 | `test194_frame_new_defaults` | TEST194: Test Frame::new sets version and defaults correctly, optional fields are None | src/bifaci/frame.rs:1289 |
| test195 | `test195_frame_default` | TEST195: Test Frame::default creates a Req frame (the documented default) | src/bifaci/frame.rs:1307 |
| test196 | `test196_is_eof_when_none` | TEST196: Test is_eof returns false when eof field is None (unset) | src/bifaci/frame.rs:1315 |
| test197 | `test197_is_eof_when_false` | TEST197: Test is_eof returns false when eof field is explicitly Some(false) | src/bifaci/frame.rs:1322 |
| test198 | `test198_limits_default` | TEST198: Test Limits::default provides the documented default values | src/bifaci/frame.rs:1330 |
| test199 | `test199_protocol_version_constant` | TEST199: Test PROTOCOL_VERSION is 2 | src/bifaci/frame.rs:1340 |
| test200 | `test200_key_constants` | TEST200: Test integer key constants match the protocol specification | src/bifaci/frame.rs:1346 |
| test201 | `test201_hello_manifest_binary_data` | TEST201: Test hello_with_manifest preserves binary manifest data (not just JSON text) | src/bifaci/frame.rs:1362 |
| test202 | `test202_message_id_equality_and_hash` | TEST202: Test MessageId Eq/Hash semantics: equal UUIDs are equal, different ones are not | src/bifaci/frame.rs:1370 |
| test203 | `test203_message_id_cross_variant_inequality` | TEST203: Test Uuid and Uint variants of MessageId are never equal even for coincidental byte values | src/bifaci/frame.rs:1393 |
| test204 | `test204_req_frame_empty_payload` | TEST204: Test Frame::req with empty payload stores Some(empty vec) not None | src/bifaci/frame.rs:1401 |
| test205 | `test205_encode_decode_roundtrip` | TEST205: Test REQ frame encode/decode roundtrip preserves all fields | src/bifaci/io.rs:864 |
| test206 | `test206_hello_frame_roundtrip` | TEST206: Test HELLO frame encode/decode roundtrip preserves max_frame, max_chunk, max_reorder_buffer | src/bifaci/io.rs:881 |
| test207 | `test207_err_frame_roundtrip` | TEST207: Test ERR frame encode/decode roundtrip preserves error code and message | src/bifaci/io.rs:894 |
| test208 | `test208_log_frame_roundtrip` | TEST208: Test LOG frame encode/decode roundtrip preserves level and message | src/bifaci/io.rs:907 |
| test210 | `test210_end_frame_roundtrip` | TEST210: Test END frame encode/decode roundtrip preserves eof marker and optional payload | src/bifaci/io.rs:1017 |
| test211 | `test211_hello_with_manifest_roundtrip` | TEST211: Test HELLO with manifest encode/decode roundtrip preserves manifest bytes and limits | src/bifaci/io.rs:1031 |
| test212 | `test212_chunk_with_offset_roundtrip` | TEST212: Test chunk_with_offset encode/decode roundtrip preserves offset, len, eof (with stream_id) | src/bifaci/io.rs:1045 |
| test213 | `test213_heartbeat_roundtrip` | TEST213: Test heartbeat frame encode/decode roundtrip preserves ID with no extra fields | src/bifaci/io.rs:1065 |
| test214 | `test214_frame_io_roundtrip` | TEST214: Test write_frame/read_frame IO roundtrip through length-prefixed wire format | src/bifaci/io.rs:1079 |
| test215 | `test215_multiple_frames` | TEST215: Test reading multiple sequential frames from a single buffer | src/bifaci/io.rs:1102 |
| test216 | `test216_frame_too_large` | TEST216: Test write_frame rejects frames exceeding max_frame limit | src/bifaci/io.rs:1140 |
| test217 | `test217_read_frame_too_large` | TEST217: Test read_frame rejects incoming frames exceeding the negotiated max_frame limit | src/bifaci/io.rs:1158 |
| test218 | `test218_write_chunked` | TEST218: Test write_chunked splits data into chunks respecting max_chunk and reconstructs correctly Chunks from write_chunked have seq=0. SeqAssigner at the output stage assigns final seq. Chunk ordering within a stream is tracked by chunk_index (chunk_index field). | src/bifaci/io.rs:1179 |
| test219 | `test219_write_chunked_empty_data` | TEST219: Test write_chunked with empty data produces a single EOF chunk | src/bifaci/io.rs:1247 |
| test220 | `test220_write_chunked_exact_fit` | TEST220: Test write_chunked with data exactly equal to max_chunk produces exactly one chunk | src/bifaci/io.rs:1265 |
| test221 | `test221_eof_handling` | TEST221: Test read_frame returns Ok(None) on clean EOF (empty stream) | src/bifaci/io.rs:1285 |
| test222 | `test222_truncated_length_prefix` | TEST222: Test read_frame handles truncated length prefix (fewer than 4 bytes available) | src/bifaci/io.rs:1295 |
| test223 | `test223_truncated_frame_body` | TEST223: Test read_frame returns error on truncated frame body (length prefix says more bytes than available) | src/bifaci/io.rs:1314 |
| test224 | `test224_message_id_uint` | TEST224: Test MessageId::Uint roundtrips through encode/decode | src/bifaci/io.rs:1329 |
| test225 | `test225_decode_non_map_value` | TEST225: Test decode_frame rejects non-map CBOR values (e.g., array, integer, string) | src/bifaci/io.rs:1341 |
| test226 | `test226_decode_missing_version` | TEST226: Test decode_frame rejects CBOR map missing required version field | src/bifaci/io.rs:1353 |
| test227 | `test227_decode_invalid_frame_type_value` | TEST227: Test decode_frame rejects CBOR map with invalid frame_type value | src/bifaci/io.rs:1368 |
| test228 | `test228_decode_missing_id` | TEST228: Test decode_frame rejects CBOR map missing required id field | src/bifaci/io.rs:1383 |
| test229 | `test229_frame_reader_writer_set_limits` | TEST229: Test FrameReader/FrameWriter set_limits updates the negotiated limits | src/bifaci/io.rs:1398 |
| test230 | `test230_async_handshake` | TEST230: Test async handshake exchanges HELLO frames and negotiates minimum limits | src/bifaci/io.rs:1415 |
| test231 | `test231_handshake_rejects_non_hello` | TEST231: Test handshake fails when peer sends non-HELLO frame | src/bifaci/io.rs:1445 |
| test232 | `test232_handshake_rejects_missing_manifest` | TEST232: Test handshake fails when cartridge HELLO is missing required manifest | src/bifaci/io.rs:1472 |
| test233 | `test233_binary_payload_all_byte_values` | TEST233: Test binary payload with all 256 byte values roundtrips through encode/decode | src/bifaci/io.rs:1495 |
| test234 | `test234_decode_garbage_bytes` | TEST234: Test decode_frame handles garbage CBOR bytes gracefully with an error | src/bifaci/io.rs:1512 |
| test235 | `test235_response_chunk` | TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly | src/bifaci/host_runtime.rs:1950 |
| test236 | `test236_response_chunk_with_all_fields` | TEST236: Test ResponseChunk with all fields populated preserves offset, len, and eof | src/bifaci/host_runtime.rs:1966 |
| test237 | `test237_cartridge_response_single` | TEST237: Test CartridgeResponse::Single final_payload returns the single payload slice | src/bifaci/host_runtime.rs:1982 |
| test238 | `test238_cartridge_response_single_empty` | TEST238: Test CartridgeResponse::Single with empty payload returns empty slice and empty vec | src/bifaci/host_runtime.rs:1990 |
| test239 | `test239_cartridge_response_streaming` | TEST239: Test CartridgeResponse::Streaming concatenated joins all chunk payloads in order | src/bifaci/host_runtime.rs:1998 |
| test240 | `test240_cartridge_response_streaming_final_payload` | TEST240: Test CartridgeResponse::Streaming final_payload returns the last chunk's payload | src/bifaci/host_runtime.rs:2009 |
| test241 | `test241_cartridge_response_streaming_empty_chunks` | TEST241: Test CartridgeResponse::Streaming with empty chunks vec returns empty concatenation | src/bifaci/host_runtime.rs:2020 |
| test242 | `test242_cartridge_response_streaming_large_payload` | TEST242: Test CartridgeResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads | src/bifaci/host_runtime.rs:2028 |
| test243 | `test243_async_host_error_display` | TEST243: Test AsyncHostError variants display correct error messages | src/bifaci/host_runtime.rs:2044 |
| test244 | `test244_async_host_error_from_cbor` | TEST244: Test AsyncHostError::from converts CborError to Cbor variant | src/bifaci/host_runtime.rs:2058 |
| test245 | `test245_async_host_error_from_io` | TEST245: Test AsyncHostError::from converts io::Error to Io variant | src/bifaci/host_runtime.rs:2069 |
| test246 | `test246_async_host_error_clone` | TEST246: Test AsyncHostError Clone implementation produces equal values | src/bifaci/host_runtime.rs:2080 |
| test247 | `test247_response_chunk_clone` | TEST247: Test ResponseChunk Clone produces independent copy with same data | src/bifaci/host_runtime.rs:2088 |
| test248 | `test248_register_and_find_handler` | TEST248: Test register_op and find_handler by exact cap URN | src/bifaci/cartridge_runtime.rs:3973 |
| test249 | `test249_raw_handler` | TEST249: Test register_op handler echoes bytes directly | src/bifaci/cartridge_runtime.rs:3981 |
| test250 | `test250_typed_handler_deserialization` | TEST250: Test Op handler collects input and processes it | src/bifaci/cartridge_runtime.rs:3999 |
| test251 | `test251_typed_handler_rejects_invalid_json` | TEST251: Test Op handler propagates errors through RuntimeError::Handler | src/bifaci/cartridge_runtime.rs:4042 |
| test252 | `test252_find_handler_unknown_cap` | TEST252: Test find_handler returns None for unregistered cap URNs | src/bifaci/cartridge_runtime.rs:4075 |
| test253 | `test253_handler_is_send_sync` | TEST253: Test OpFactory can be cloned via Arc and sent across tasks (Send + Sync) | src/bifaci/cartridge_runtime.rs:4082 |
| test254 | `test254_no_peer_invoker` | TEST254: Test NoPeerInvoker always returns PeerRequest error | src/bifaci/cartridge_runtime.rs:4127 |
| test255 | `test255_no_peer_invoker_with_arguments` | TEST255: Test NoPeerInvoker call_with_bytes also returns error | src/bifaci/cartridge_runtime.rs:4141 |
| test256 | `test256_with_manifest_json` | TEST256: Test CartridgeRuntime::with_manifest_json stores manifest data and parses when valid | src/bifaci/cartridge_runtime.rs:4149 |
| test257 | `test257_new_with_invalid_json` | TEST257: Test CartridgeRuntime::new with invalid JSON still creates runtime (manifest is None) | src/bifaci/cartridge_runtime.rs:4166 |
| test258 | `test258_with_manifest_struct` | TEST258: Test CartridgeRuntime::with_manifest creates runtime with valid manifest data | src/bifaci/cartridge_runtime.rs:4174 |
| test259 | `test259_extract_effective_payload_non_cbor` | TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged | src/bifaci/cartridge_runtime.rs:4183 |
| test260 | `test260_extract_effective_payload_no_content_type` | TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged | src/bifaci/cartridge_runtime.rs:4193 |
| test261 | `test261_extract_effective_payload_cbor_match` | TEST261: Test extract_effective_payload with CBOR content extracts matching argument value | src/bifaci/cartridge_runtime.rs:4203 |
| test262 | `test262_extract_effective_payload_cbor_no_match` | TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input | src/bifaci/cartridge_runtime.rs:4251 |
| test263 | `test263_extract_effective_payload_invalid_cbor` | TEST263: Test extract_effective_payload with invalid CBOR bytes returns deserialization error | src/bifaci/cartridge_runtime.rs:4280 |
| test264 | `test264_extract_effective_payload_cbor_not_array` | TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error | src/bifaci/cartridge_runtime.rs:4294 |
| test266 | `test266_cli_frame_sender_construction` | TEST266: Test CliFrameSender wraps CliStreamEmitter correctly (basic construction) | src/bifaci/cartridge_runtime.rs:4318 |
| test268 | `test268_runtime_error_display` | TEST268: Test RuntimeError variants display correct messages | src/bifaci/cartridge_runtime.rs:4329 |
| test270 | `test270_multiple_handlers` | TEST270: Test registering multiple Op handlers for different caps and finding each independently | src/bifaci/cartridge_runtime.rs:4351 |
| test271 | `test271_handler_replacement` | TEST271: Test Op handler replacing an existing registration for the same cap URN | src/bifaci/cartridge_runtime.rs:4376 |
| test272 | `test272_extract_effective_payload_multiple_args` | TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one | src/bifaci/cartridge_runtime.rs:4428 |
| test273 | `test273_extract_effective_payload_binary_value` | TEST273: Test extract_effective_payload with binary data in CBOR value (not just text) | src/bifaci/cartridge_runtime.rs:4499 |
| test274 | `test274_cap_argument_value_new` | TEST274: Test CapArgumentValue::new stores media_urn and raw byte value | src/cap/caller.rs:485 |
| test275 | `test275_cap_argument_value_from_str` | TEST275: Test CapArgumentValue::from_str converts string to UTF-8 bytes | src/cap/caller.rs:493 |
| test276 | `test276_cap_argument_value_as_str_valid` | TEST276: Test CapArgumentValue::value_as_str succeeds for UTF-8 data | src/cap/caller.rs:501 |
| test277 | `test277_cap_argument_value_as_str_invalid_utf8` | TEST277: Test CapArgumentValue::value_as_str fails for non-UTF-8 binary data | src/cap/caller.rs:508 |
| test278 | `test278_cap_argument_value_empty` | TEST278: Test CapArgumentValue::new with empty value stores empty vec | src/cap/caller.rs:515 |
| test279 | `test279_cap_argument_value_clone` | TEST279: Test CapArgumentValue Clone produces independent copy with same data | src/cap/caller.rs:523 |
| test280 | `test280_cap_argument_value_debug` | TEST280: Test CapArgumentValue Debug format includes media_urn and value | src/cap/caller.rs:532 |
| test281 | `test281_cap_argument_value_into_string` | TEST281: Test CapArgumentValue::new accepts Into<String> for media_urn (String and &str) | src/cap/caller.rs:540 |
| test282 | `test282_cap_argument_value_unicode` | TEST282: Test CapArgumentValue::from_str with Unicode string preserves all characters | src/cap/caller.rs:551 |
| test283 | `test283_cap_argument_value_large_binary` | TEST283: Test CapArgumentValue with large binary payload preserves all bytes | src/cap/caller.rs:558 |
| test284 | `test284_handshake_host_cartridge` | TEST284: Handshake exchanges HELLO frames, negotiates limits | src/bifaci/integration_tests.rs:776 |
| test285 | `test285_request_response_simple` | TEST285: Simple request-response flow (REQ → END with payload) | src/bifaci/integration_tests.rs:811 |
| test286 | `test286_streaming_chunks` | TEST286: Streaming response with multiple CHUNK frames | src/bifaci/integration_tests.rs:853 |
| test287 | `test287_heartbeat_from_host` | TEST287: Host-initiated heartbeat | src/bifaci/integration_tests.rs:919 |
| test290 | `test290_limits_negotiation` | TEST290: Limit negotiation picks minimum | src/bifaci/integration_tests.rs:958 |
| test291 | `test291_binary_payload_roundtrip` | TEST291: Binary payload roundtrip (all 256 byte values) | src/bifaci/integration_tests.rs:986 |
| test292 | `test292_message_id_uniqueness` | TEST292: Sequential requests get distinct MessageIds | src/bifaci/integration_tests.rs:1038 |
| test293 | `test293_cartridge_runtime_handler_registration` | TEST293: Test CartridgeRuntime Op registration and lookup by exact and non-existent cap URN | src/bifaci/integration_tests.rs:21 |
| test299 | `test299_empty_payload_roundtrip` | TEST299: Empty payload request/response roundtrip | src/bifaci/integration_tests.rs:1091 |
| test304 | `test304_media_availability_output_constant` | TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags | src/urn/media_urn.rs:875 |
| test305 | `test305_media_path_output_constant` | TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags | src/urn/media_urn.rs:887 |
| test306 | `test306_availability_and_path_output_distinct` | TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs | src/urn/media_urn.rs:898 |
| test307 | `test307_model_availability_urn` | TEST307: Test model_availability_urn builds valid cap URN with correct op and media specs | src/standard/caps.rs:801 |
| test308 | `test308_model_path_urn` | TEST308: Test model_path_urn builds valid cap URN with correct op and media specs | src/standard/caps.rs:810 |
| test309 | `test309_model_availability_and_path_are_distinct` | TEST309: Test model_availability_urn and model_path_urn produce distinct URNs | src/standard/caps.rs:819 |
| test310 | `test310_llm_generate_text_urn_shape` |  | src/standard/caps.rs:827 |
| test312 | `test312_all_urn_builders_produce_valid_urns` | TEST312: Test all URN builders produce parseable cap URNs | src/standard/caps.rs:846 |
| test320 | `test320_cartridge_info_construction` | TEST320-335: CartridgeRepoServer and CartridgeRepoClient tests | src/bifaci/cartridge_repo.rs:722 |
| test321 | `test321_cartridge_info_is_signed` |  | src/bifaci/cartridge_repo.rs:760 |
| test322 | `test322_cartridge_info_build_for_platform` |  | src/bifaci/cartridge_repo.rs:791 |
| test323 | `test323_cartridge_repo_server_validate_registry` |  | src/bifaci/cartridge_repo.rs:854 |
| test324 | `test324_cartridge_repo_server_transform_to_array` |  | src/bifaci/cartridge_repo.rs:878 |
| test325 | `test325_cartridge_repo_server_get_cartridges` |  | src/bifaci/cartridge_repo.rs:929 |
| test326 | `test326_cartridge_repo_server_get_cartridge_by_id` |  | src/bifaci/cartridge_repo.rs:975 |
| test327 | `test327_cartridge_repo_server_search_cartridges` |  | src/bifaci/cartridge_repo.rs:1024 |
| test328 | `test328_cartridge_repo_server_get_by_category` |  | src/bifaci/cartridge_repo.rs:1073 |
| test329 | `test329_cartridge_repo_server_get_by_cap` |  | src/bifaci/cartridge_repo.rs:1122 |
| test330 | `test330_cartridge_repo_client_update_cache` |  | src/bifaci/cartridge_repo.rs:1176 |
| test331 | `test331_cartridge_repo_client_get_suggestions` |  | src/bifaci/cartridge_repo.rs:1215 |
| test332 | `test332_cartridge_repo_client_get_cartridge` |  | src/bifaci/cartridge_repo.rs:1257 |
| test333 | `test333_cartridge_repo_client_get_all_caps` |  | src/bifaci/cartridge_repo.rs:1296 |
| test334 | `test334_cartridge_repo_client_needs_sync` |  | src/bifaci/cartridge_repo.rs:1361 |
| test335 | `test335_cartridge_repo_server_client_integration` |  | src/bifaci/cartridge_repo.rs:1380 |
| test336 | `test336_file_path_reads_file_passes_bytes` | TEST336: Single file-path arg with stdin source reads file and passes bytes to handler | src/bifaci/cartridge_runtime.rs:4545 |
| test337 | `test337_file_path_without_stdin_passes_string` | TEST337: file-path arg without stdin source passes path as string (no conversion) | src/bifaci/cartridge_runtime.rs:4610 |
| test338 | `test338_file_path_via_cli_flag` | TEST338: file-path arg reads file via --file CLI flag | src/bifaci/cartridge_runtime.rs:4642 |
| test339 | `test339_file_path_array_glob_expansion` | TEST339: file-path-array reads multiple files with glob pattern | src/bifaci/cartridge_runtime.rs:4674 |
| test340 | `test340_file_not_found_clear_error` | TEST340: File not found error provides clear message | src/bifaci/cartridge_runtime.rs:4717 |
| test341 | `test341_stdin_precedence_over_file_path` | TEST341: stdin takes precedence over file-path in source order | src/bifaci/cartridge_runtime.rs:4758 |
| test342 | `test342_file_path_position_zero_reads_first_arg` | TEST342: file-path with position 0 reads first positional arg as file | src/bifaci/cartridge_runtime.rs:4796 |
| test343 | `test343_non_file_path_args_unaffected` | TEST343: Non-file-path args are not affected by file reading | src/bifaci/cartridge_runtime.rs:4829 |
| test344 | `test344_file_path_array_invalid_json_fails` | TEST344: file-path-array with nonexistent path fails clearly | src/bifaci/cartridge_runtime.rs:4860 |
| test345 | `test345_file_path_array_one_file_missing_fails_hard` | TEST345: file-path-array with literal nonexistent path fails hard | src/bifaci/cartridge_runtime.rs:4901 |
| test346 | `test346_large_file_reads_successfully` | TEST346: Large file (1MB) reads successfully | src/bifaci/cartridge_runtime.rs:4945 |
| test347 | `test347_empty_file_reads_as_empty_bytes` | TEST347: Empty file reads as empty bytes | src/bifaci/cartridge_runtime.rs:4981 |
| test348 | `test348_file_path_conversion_respects_source_order` | TEST348: file-path conversion respects source order | src/bifaci/cartridge_runtime.rs:5013 |
| test349 | `test349_file_path_multiple_sources_fallback` | TEST349: file-path arg with multiple sources tries all in order | src/bifaci/cartridge_runtime.rs:5050 |
| test350 | `test350_full_cli_mode_with_file_path_integration` | TEST350: Integration test - full CLI mode invocation with file-path | src/bifaci/cartridge_runtime.rs:5087 |
| test351 | `test351_file_path_array_empty_array` | TEST351: file-path array with empty CBOR array returns empty (CBOR mode) | src/bifaci/cartridge_runtime.rs:5151 |
| test352 | `test352_file_permission_denied_clear_error` | TEST352: file permission denied error is clear (Unix-specific) | src/bifaci/cartridge_runtime.rs:5201 |
| test353 | `test353_cbor_payload_format_consistency` | TEST353: CBOR payload format matches between CLI and CBOR mode | src/bifaci/cartridge_runtime.rs:5269 |
| test354 | `test354_glob_pattern_no_matches_empty_array` | TEST354: Glob pattern with no matches fails hard (NO FALLBACK) | src/bifaci/cartridge_runtime.rs:5333 |
| test355 | `test355_glob_pattern_skips_directories` | TEST355: Glob pattern skips directories | src/bifaci/cartridge_runtime.rs:5376 |
| test356 | `test356_multiple_glob_patterns_combined` | TEST356: Multiple glob patterns combined | src/bifaci/cartridge_runtime.rs:5420 |
| test357 | `test357_symlinks_followed` | TEST357: Symlinks are followed when reading files | src/bifaci/cartridge_runtime.rs:5504 |
| test358 | `test358_binary_file_non_utf8` | TEST358: Binary file with non-UTF8 data reads correctly | src/bifaci/cartridge_runtime.rs:5547 |
| test359 | `test359_invalid_glob_pattern_fails` | TEST359: Invalid glob pattern fails with clear error | src/bifaci/cartridge_runtime.rs:5582 |
| test360 | `test360_extract_effective_payload_with_file_data` | TEST360: Extract effective payload handles file-path data correctly | src/bifaci/cartridge_runtime.rs:5624 |
| test361 | `test361_cli_mode_file_path` | TEST361: CLI mode with file path - pass file path as command-line argument | src/bifaci/cartridge_runtime.rs:5710 |
| test362 | `test362_cli_mode_piped_binary` | TEST362: CLI mode with binary piped in - pipe binary data via stdin  This test simulates real-world conditions: - Pure binary data piped to stdin (NOT CBOR) - CLI mode detected (command arg present) - Cap accepts stdin source - Binary is chunked on-the-fly and accumulated - Handler receives complete CBOR payload | src/bifaci/cartridge_runtime.rs:5756 |
| test363 | `test363_cbor_mode_chunked_content` | TEST363: CBOR mode with chunked content - send file content streaming as chunks | src/bifaci/cartridge_runtime.rs:5823 |
| test364 | `test364_cbor_mode_file_path` | TEST364: CBOR mode with file path - send file path in CBOR arguments (auto-conversion) | src/bifaci/cartridge_runtime.rs:5869 |
| test365 | `test365_stream_start_frame` | TEST365: Frame::stream_start stores request_id, stream_id, and media_urn | src/bifaci/frame.rs:1408 |
| test366 | `test366_stream_end_frame` | TEST366: Frame::stream_end stores request_id and stream_id | src/bifaci/frame.rs:1425 |
| test367 | `test367_stream_start_with_empty_stream_id` | TEST367: StreamStart frame with empty stream_id still constructs (validation happens elsewhere) | src/bifaci/frame.rs:1441 |
| test368 | `test368_stream_start_with_empty_media_urn` | TEST368: StreamStart frame with empty media_urn still constructs (validation happens elsewhere) | src/bifaci/frame.rs:1452 |
| test389 | `test389_stream_start_roundtrip` | TEST389: StreamStart encode/decode roundtrip preserves stream_id and media_urn | src/bifaci/io.rs:1552 |
| test390 | `test390_stream_end_roundtrip` | TEST390: StreamEnd encode/decode roundtrip preserves stream_id, no media_urn | src/bifaci/io.rs:1569 |
| test394 | `test394_peer_invoke_roundtrip` | TEST394: Test peer invoke round-trip (testcartridge calls itself) Disabled: LocalCartridgeRouter feature not implemented - uses non-existent modules | tests/orchestrator_integration.rs:731 |
| test395 | `test395_build_payload_small` | TEST395: Small payload (< max_chunk) produces correct CBOR arguments | src/bifaci/cartridge_runtime.rs:6029 |
| test396 | `test396_build_payload_large` | TEST396: Large payload (> max_chunk) accumulates across chunks correctly | src/bifaci/cartridge_runtime.rs:6072 |
| test397 | `test397_build_payload_empty` | TEST397: Empty reader produces valid empty CBOR arguments | src/bifaci/cartridge_runtime.rs:6113 |
| test398 | `test398_build_payload_io_error` | TEST398: IO error from reader propagates as RuntimeError::Io | src/bifaci/cartridge_runtime.rs:6151 |
| test399 | `test399_relay_notify_discriminant_roundtrip` | TEST399: Verify RelayNotify frame type discriminant roundtrips through u8 (value 10) | src/bifaci/frame.rs:1463 |
| test400 | `test400_relay_state_discriminant_roundtrip` | TEST400: Verify RelayState frame type discriminant roundtrips through u8 (value 11) | src/bifaci/frame.rs:1472 |
| test401 | `test401_relay_notify_frame` | TEST401: Verify relay_notify factory stores manifest and limits, and accessors extract them | src/bifaci/frame.rs:1481 |
| test402 | `test402_relay_state_frame` | TEST402: Verify relay_state factory stores resource payload in frame payload field | src/bifaci/frame.rs:1496 |
| test403 | `test403_invalid_frame_type_past_relay_state` | TEST403: Verify from_u8 returns None for value 12 (one past RelayState) | src/bifaci/frame.rs:1508 |
| test404 | `test404_slave_sends_relay_notify_on_connect` | TEST404: Slave sends RelayNotify on connect (initial_notify parameter) | src/bifaci/relay.rs:367 |
| test405 | `test405_master_reads_relay_notify` | TEST405: Master reads RelayNotify and extracts manifest + limits | src/bifaci/relay.rs:402 |
| test406 | `test406_slave_stores_relay_state` | TEST406: Slave stores RelayState from master | src/bifaci/relay.rs:432 |
| test407 | `test407_protocol_frames_pass_through` | TEST407: Protocol frames pass through slave transparently (both directions) | src/bifaci/relay.rs:475 |
| test408 | `test408_relay_frames_not_forwarded` | TEST408: RelayNotify/RelayState are NOT forwarded through relay | src/bifaci/relay.rs:567 |
| test409 | `test409_slave_injects_relay_notify_midstream` | TEST409: Slave can inject RelayNotify mid-stream (cap change) | src/bifaci/relay.rs:619 |
| test410 | `test410_master_receives_updated_relay_notify` | TEST410: Master receives updated RelayNotify (cap change callback via read_frame) | src/bifaci/relay.rs:694 |
| test411 | `test411_socket_close_detection` | TEST411: Socket close detection (both directions) | src/bifaci/relay.rs:765 |
| test412 | `test412_bidirectional_concurrent_flow` | TEST412: Bidirectional concurrent frame flow through relay | src/bifaci/relay.rs:806 |
| test413 | `test413_register_cartridge_adds_to_cap_table` | TEST413: Register cartridge adds entries to cap_table | src/bifaci/host_runtime.rs:2100 |
| test414 | `test414_capabilities_empty_initially` | TEST414: capabilities() returns empty JSON initially (no running cartridges) | src/bifaci/host_runtime.rs:2118 |
| test415 | `test415_req_for_known_cap_triggers_spawn` | TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary) | src/bifaci/host_runtime.rs:2131 |
| test416 | `test416_attach_cartridge_handshake_updates_capabilities` | TEST416: Attach cartridge performs HELLO handshake, extracts manifest, updates capabilities | src/bifaci/host_runtime.rs:2186 |
| test417 | `test417_route_req_to_correct_cartridge` | TEST417: Route REQ to correct cartridge by cap_urn (with two attached cartridges) | src/bifaci/host_runtime.rs:2224 |
| test418 | `test418_route_continuation_frames_by_req_id` | TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn) Verifies that after the initial REQ→cartridge routing, all subsequent continuation frames with the same req_id are routed to the same cartridge — even though no cap_urn is present on those frames. | src/bifaci/host_runtime.rs:2549 |
| test419 | `test419_cartridge_heartbeat_handled_locally` | TEST419: Cartridge HEARTBEAT handled locally (not forwarded to relay) | src/bifaci/host_runtime.rs:2354 |
| test420 | `test420_cartridge_frames_forwarded_to_relay` | TEST420: Cartridge non-HELLO/non-HB frames forwarded to relay (pass-through) | src/bifaci/host_runtime.rs:2425 |
| test421 | `test421_cartridge_death_updates_capabilities` | TEST421: Cartridge death updates capability list (caps removed) | src/bifaci/host_runtime.rs:2680 |
| test422 | `test422_cartridge_death_sends_err_for_pending_requests` | TEST422: Cartridge death sends ERR for all pending requests via relay | src/bifaci/host_runtime.rs:2756 |
| test423 | `test423_multiple_cartridges_route_independently` | TEST423: Multiple cartridges registered with distinct caps route independently | src/bifaci/host_runtime.rs:2830 |
| test424 | `test424_concurrent_requests_to_same_cartridge` | TEST424: Concurrent requests to the same cartridge are handled independently | src/bifaci/host_runtime.rs:2975 |
| test425 | `test425_find_cartridge_for_cap_unknown` | TEST425: find_cartridge_for_cap returns None for unregistered cap | src/bifaci/host_runtime.rs:3102 |
| test426 | `test426_single_master_req_response` | TEST426: Single master REQ/response routing | src/bifaci/relay_switch.rs:2098 |
| test427 | `test427_multi_master_cap_routing` | TEST427: Multi-master cap routing | src/bifaci/relay_switch.rs:2144 |
| test428 | `test428_unknown_cap_returns_error` | TEST428: Unknown cap returns error | src/bifaci/relay_switch.rs:2211 |
| test429 | `test429_find_master_for_cap` | TEST429: Cap routing logic (find_master_for_cap) | src/bifaci/relay_switch.rs:2066 |
| test430 | `test430_tie_breaking_same_cap_multiple_masters` | TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent) | src/bifaci/relay_switch.rs:2229 |
| test431 | `test431_continuation_frame_routing` | TEST431: Continuation frame routing (CHUNK, END follow REQ) | src/bifaci/relay_switch.rs:2298 |
| test432 | `test432_empty_masters_allowed` | TEST432: Empty masters list creates empty switch, add_master works | src/bifaci/relay_switch.rs:2344 |
| test433 | `test433_capability_aggregation_deduplicates` | TEST433: Capability aggregation deduplicates caps | src/bifaci/relay_switch.rs:2357 |
| test434 | `test434_limits_negotiation_minimum` | TEST434: Limits negotiation takes minimum | src/bifaci/relay_switch.rs:2387 |
| test435 | `test435_urn_matching_exact_and_accepts` | TEST435: URN matching (exact vs accepts()) | src/bifaci/relay_switch.rs:2412 |
| test436 | `test436_compute_checksum` | TEST436: Verify FNV-1a checksum function produces consistent results | src/bifaci/frame.rs:1515 |
| test437 | `test437_preferred_cap_routes_to_generic` | TEST437: find_master_for_cap with preferred_cap routes to generic handler  With is_dispatchable semantics: - Generic provider (in=media:) CAN dispatch specific request (in="media:pdf") because media: (wildcard) accepts any input type - Preference routes to preferred among dispatchable candidates | src/bifaci/relay_switch.rs:2475 |
| test438 | `test438_preferred_cap_falls_back_when_not_comparable` | TEST438: find_master_for_cap with preference falls back to closest-specificity when preferred cap is not in the comparable set | src/bifaci/relay_switch.rs:2513 |
| test439 | `test439_generic_provider_can_dispatch_specific_request` | TEST439: Generic provider CAN dispatch specific request (but only matches if no more specific provider exists)  With is_dispatchable: generic provider (in=media:) CAN handle specific request (in="media:pdf") because media: accepts any input type. With preference, can route to generic even when more specific exists. | src/bifaci/relay_switch.rs:2540 |
| test440 | `test440_chunk_index_checksum_roundtrip` | TEST440: CHUNK frame with chunk_index and checksum roundtrips through encode/decode | src/bifaci/io.rs:1615 |
| test441 | `test441_stream_end_chunk_count_roundtrip` | TEST441: STREAM_END frame with chunk_count roundtrips through encode/decode | src/bifaci/io.rs:1637 |
| test442 | `test442_seq_assigner_monotonic_same_rid` | TEST442: SeqAssigner assigns seq 0,1,2,3 for consecutive frames with same RID | src/bifaci/frame.rs:1575 |
| test443 | `test443_seq_assigner_independent_rids` | TEST443: SeqAssigner maintains independent counters for different RIDs | src/bifaci/frame.rs:1597 |
| test444 | `test444_seq_assigner_skips_non_flow` | TEST444: SeqAssigner skips non-flow frames (Heartbeat, RelayNotify, RelayState, Hello) | src/bifaci/frame.rs:1623 |
| test445 | `test445_seq_assigner_remove_by_flow_key` | TEST445: SeqAssigner.remove with FlowKey(rid, None) resets that flow; FlowKey(rid, Some(xid)) is unaffected | src/bifaci/frame.rs:1644 |
| test446 | `test446_seq_assigner_mixed_types` | TEST446: SeqAssigner handles mixed frame types (REQ, CHUNK, LOG, END) for same RID | src/bifaci/frame.rs:1715 |
| test447 | `test447_flow_key_with_xid` | TEST447: FlowKey::from_frame extracts (rid, Some(xid)) when routing_id present | src/bifaci/frame.rs:1741 |
| test448 | `test448_flow_key_without_xid` | TEST448: FlowKey::from_frame extracts (rid, None) when routing_id absent | src/bifaci/frame.rs:1754 |
| test449 | `test449_flow_key_equality` | TEST449: FlowKey equality: same rid+xid equal, different xid different key | src/bifaci/frame.rs:1765 |
| test450 | `test450_flow_key_hash_lookup` | TEST450: FlowKey hash: same keys hash equal (HashMap lookup) | src/bifaci/frame.rs:1782 |
| test451 | `test451_reorder_buffer_in_order` | TEST451: ReorderBuffer in-order delivery: seq 0,1,2 delivered immediately | src/bifaci/frame.rs:1808 |
| test452 | `test452_reorder_buffer_out_of_order` | TEST452: ReorderBuffer out-of-order: seq 1 then 0 delivers both in order | src/bifaci/frame.rs:1827 |
| test453 | `test453_reorder_buffer_gap_fill` | TEST453: ReorderBuffer gap fill: seq 0,2,1 delivers 0, buffers 2, then delivers 1+2 | src/bifaci/frame.rs:1842 |
| test454 | `test454_reorder_buffer_stale_seq` | TEST454: ReorderBuffer stale seq is hard error | src/bifaci/frame.rs:1860 |
| test455 | `test455_reorder_buffer_overflow` | TEST455: ReorderBuffer overflow triggers protocol error | src/bifaci/frame.rs:1875 |
| test456 | `test456_reorder_buffer_independent_flows` | TEST456: Multiple concurrent flows reorder independently | src/bifaci/frame.rs:1891 |
| test457 | `test457_reorder_buffer_cleanup` | TEST457: cleanup_flow removes state; new frames start at seq 0 | src/bifaci/frame.rs:1914 |
| test458 | `test458_reorder_buffer_non_flow_bypass` | TEST458: Non-flow frames bypass reorder entirely | src/bifaci/frame.rs:1931 |
| test459 | `test459_reorder_buffer_end_frame` | TEST459: Terminal END frame flows through correctly | src/bifaci/frame.rs:1947 |
| test460 | `test460_reorder_buffer_err_frame` | TEST460: Terminal ERR frame flows through correctly | src/bifaci/frame.rs:1965 |
| test461 | `test461_write_chunked_seq_zero` | TEST461: write_chunked produces frames with seq=0; SeqAssigner assigns at output stage | src/bifaci/io.rs:1654 |
| test472 | `test472_handshake_negotiates_reorder_buffer` | TEST472: Handshake negotiates max_reorder_buffer (minimum of both sides) | src/bifaci/io.rs:1694 |
| test473 | `test473_cap_discard_parses_as_valid_urn` | TEST473: CAP_DISCARD parses as valid CapUrn with in=media: and out=media:void | src/standard/caps.rs:864 |
| test474 | `test474_cap_discard_accepts_specific_void_cap` | TEST474: CAP_DISCARD accepts specific-input/void-output caps | src/standard/caps.rs:875 |
| test475 | `test475_validate_passes_with_identity` | TEST475: CapManifest::validate() passes when CAP_IDENTITY is present | src/bifaci/manifest.rs:295 |
| test476 | `test476_validate_fails_without_identity` | TEST476: CapManifest::validate() fails when CAP_IDENTITY is missing | src/bifaci/manifest.rs:309 |
| test478 | `test478_auto_registers_identity_handler` | TEST478: CartridgeRuntime auto-registers identity and discard handlers on construction | src/bifaci/cartridge_runtime.rs:6181 |
| test479 | `test479_custom_identity_overrides_default` | TEST479: Custom identity Op overrides auto-registered default | src/bifaci/cartridge_runtime.rs:6200 |
| test480 | `test480_parse_caps_rejects_manifest_without_identity` | TEST480: parse_caps_from_manifest rejects manifest without CAP_IDENTITY | src/bifaci/host_runtime.rs:1932 |
| test481 | `test481_verify_identity_succeeds` | TEST481: verify_identity succeeds with standard identity echo handler | src/bifaci/io.rs:1790 |
| test482 | `test482_verify_identity_fails_on_err` | TEST482: verify_identity fails when cartridge returns ERR on identity call | src/bifaci/io.rs:1813 |
| test483 | `test483_verify_identity_fails_on_close` | TEST483: verify_identity fails when connection closes before response | src/bifaci/io.rs:1847 |
| test485 | `test485_attach_cartridge_identity_verification_succeeds` | TEST485: attach_cartridge completes identity verification with working cartridge | src/bifaci/host_runtime.rs:3115 |
| test486 | `test486_attach_cartridge_identity_verification_fails` | TEST486: attach_cartridge rejects cartridge that fails identity verification | src/bifaci/host_runtime.rs:3146 |
| test487 | `test487_relay_switch_identity_verification_succeeds` | TEST487: RelaySwitch construction verifies identity through relay chain | src/bifaci/relay_switch.rs:2570 |
| test488 | `test488_relay_switch_identity_verification_fails` | TEST488: RelaySwitch construction fails when master's identity verification fails | src/bifaci/relay_switch.rs:2588 |
| test489 | `test489_add_master_dynamic` | TEST489: add_master dynamically connects new host to running switch | src/bifaci/relay_switch.rs:2760 |
| test490 | `test490_identity_verification_multiple_cartridges` | TEST490: Identity verification with multiple cartridges through single relay  Both cartridges must pass identity verification independently before any real requests are routed. | src/bifaci/integration_tests.rs:1256 |
| test491 | `test491_chunk_requires_chunk_index_and_checksum` | TEST491: Frame::chunk constructor requires and sets chunk_index and checksum | src/bifaci/frame.rs:1987 |
| test492 | `test492_stream_end_requires_chunk_count` | TEST492: Frame::stream_end constructor requires and sets chunk_count | src/bifaci/frame.rs:2002 |
| test493 | `test493_compute_checksum_fnv1a_test_vectors` | TEST493: compute_checksum produces correct FNV-1a hash for known test vectors | src/bifaci/frame.rs:2014 |
| test494 | `test494_compute_checksum_deterministic` | TEST494: compute_checksum is deterministic | src/bifaci/frame.rs:2023 |
| test495 | `test495_cbor_rejects_chunk_without_chunk_index` | TEST495: CBOR decode REJECTS CHUNK frame missing chunk_index field | src/bifaci/frame.rs:2035 |
| test496 | `test496_cbor_rejects_chunk_without_checksum` | TEST496: CBOR decode REJECTS CHUNK frame missing checksum field | src/bifaci/frame.rs:2061 |
| test497 | `test497_chunk_corrupted_payload_rejected` | TEST497: Verify CHUNK frame with corrupted payload is rejected by checksum | src/bifaci/io.rs:1585 |
| test498 | `test498_routing_id_cbor_roundtrip` | TEST498: routing_id field roundtrips through CBOR encoding | src/bifaci/frame.rs:2108 |
| test499 | `test499_chunk_index_checksum_cbor_roundtrip` | TEST499: chunk_index and checksum roundtrip through CBOR encoding | src/bifaci/frame.rs:2126 |
| test500 | `test500_chunk_count_cbor_roundtrip` | TEST500: chunk_count roundtrips through CBOR encoding | src/bifaci/frame.rs:2145 |
| test501 | `test501_frame_new_initializes_optional_fields_none` | TEST501: Frame::new initializes new fields to None | src/bifaci/frame.rs:2161 |
| test502 | `test502_keys_module_new_field_constants` | TEST502: Keys module has constants for new fields | src/bifaci/frame.rs:2172 |
| test503 | `test503_compute_checksum_empty_data` | TEST503: compute_checksum handles empty data correctly | src/bifaci/frame.rs:2182 |
| test504 | `test504_compute_checksum_large_payload` | TEST504: compute_checksum handles large payloads without overflow | src/bifaci/frame.rs:2189 |
| test505 | `test505_chunk_with_offset_sets_chunk_index` | TEST505: chunk_with_offset sets chunk_index correctly | src/bifaci/frame.rs:2201 |
| test506 | `test506_compute_checksum_different_data_different_hash` | TEST506: Different data produces different checksums | src/bifaci/frame.rs:2225 |
| test507 | `test507_reorder_buffer_xid_isolation` | TEST507: ReorderBuffer isolates flows by XID (routing_id) - same RID different XIDs | src/bifaci/frame.rs:2241 |
| test508 | `test508_reorder_buffer_duplicate_buffered_seq` | TEST508: ReorderBuffer rejects duplicate seq already in buffer | src/bifaci/frame.rs:2269 |
| test509 | `test509_reorder_buffer_large_gap_rejected` | TEST509: ReorderBuffer handles large seq gaps without DOS | src/bifaci/frame.rs:2286 |
| test510 | `test510_reorder_buffer_multiple_gaps` | TEST510: ReorderBuffer with multiple interleaved gaps fills correctly | src/bifaci/frame.rs:2311 |
| test511 | `test511_reorder_buffer_cleanup_with_buffered_frames` | TEST511: ReorderBuffer cleanup with buffered frames discards them | src/bifaci/frame.rs:2344 |
| test512 | `test512_reorder_buffer_burst_delivery` | TEST512: ReorderBuffer delivers burst of consecutive buffered frames | src/bifaci/frame.rs:2367 |
| test513 | `test513_reorder_buffer_mixed_types_same_flow` | TEST513: ReorderBuffer different frame types in same flow maintain order | src/bifaci/frame.rs:2387 |
| test514 | `test514_reorder_buffer_xid_cleanup_isolation` | TEST514: ReorderBuffer with XID cleanup doesn't affect different XID | src/bifaci/frame.rs:2412 |
| test515 | `test515_reorder_buffer_overflow_error_details` | TEST515: ReorderBuffer overflow error includes diagnostic information | src/bifaci/frame.rs:2437 |
| test516 | `test516_reorder_buffer_stale_error_details` | TEST516: ReorderBuffer stale error includes diagnostic information | src/bifaci/frame.rs:2460 |
| test517 | `test517_flow_key_none_vs_some_xid` | TEST517: FlowKey with None XID differs from Some(xid) | src/bifaci/frame.rs:2480 |
| test518 | `test518_reorder_buffer_empty_ready_vec` | TEST518: ReorderBuffer handles zero-length ready vec correctly | src/bifaci/frame.rs:2506 |
| test519 | `test519_reorder_buffer_state_persistence` | TEST519: ReorderBuffer state persists across accept calls | src/bifaci/frame.rs:2518 |
| test520 | `test520_reorder_buffer_per_flow_limit` | TEST520: ReorderBuffer max_buffer_per_flow is per-flow not global | src/bifaci/frame.rs:2536 |
| test521 | `test521_relay_notify_cbor_roundtrip` | TEST521: RelayNotify CBOR roundtrip preserves manifest and limits | src/bifaci/frame.rs:2564 |
| test522 | `test522_relay_state_cbor_roundtrip` | TEST522: RelayState CBOR roundtrip preserves payload | src/bifaci/frame.rs:2590 |
| test523 | `test523_relay_notify_not_flow_frame` | TEST523: is_flow_frame returns false for RelayNotify | src/bifaci/frame.rs:2607 |
| test524 | `test524_relay_state_not_flow_frame` | TEST524: is_flow_frame returns false for RelayState | src/bifaci/frame.rs:2618 |
| test525 | `test525_relay_notify_empty_manifest` | TEST525: RelayNotify with empty manifest is valid | src/bifaci/frame.rs:2628 |
| test526 | `test526_relay_state_empty_payload` | TEST526: RelayState with empty payload is valid | src/bifaci/frame.rs:2639 |
| test527 | `test527_relay_notify_large_manifest` | TEST527: RelayNotify with large manifest roundtrips correctly | src/bifaci/frame.rs:2649 |
| test528 | `test528_relay_frames_use_uint_zero_id` | TEST528: RelayNotify and RelayState use MessageId::Uint(0) | src/bifaci/frame.rs:2676 |
| test529 | `test529_input_stream_recv_order` | TEST529: InputStream recv yields chunks in order | src/bifaci/cartridge_runtime.rs:6264 |
| test530 | `test530_input_stream_collect_bytes` | TEST530: InputStream::collect_bytes concatenates byte chunks | src/bifaci/cartridge_runtime.rs:6284 |
| test531 | `test531_input_stream_collect_bytes_text` | TEST531: InputStream::collect_bytes handles text chunks | src/bifaci/cartridge_runtime.rs:6298 |
| test532 | `test532_input_stream_empty` | TEST532: InputStream empty stream produces empty bytes | src/bifaci/cartridge_runtime.rs:6311 |
| test533 | `test533_input_stream_error_propagation` | TEST533: InputStream propagates errors | src/bifaci/cartridge_runtime.rs:6321 |
| test534 | `test534_input_stream_media_urn` | TEST534: InputStream::media_urn returns correct URN | src/bifaci/cartridge_runtime.rs:6340 |
| test535 | `test535_input_package_iteration` | TEST535: InputPackage recv yields streams | src/bifaci/cartridge_runtime.rs:6349 |
| test536 | `test536_input_package_collect_all_bytes` | TEST536: InputPackage::collect_all_bytes aggregates all streams | src/bifaci/cartridge_runtime.rs:6386 |
| test537 | `test537_input_package_empty` | TEST537: InputPackage empty package produces empty bytes | src/bifaci/cartridge_runtime.rs:6422 |
| test538 | `test538_input_package_error_propagation` | TEST538: InputPackage propagates stream errors | src/bifaci/cartridge_runtime.rs:6437 |
| test539 | `test539_output_stream_sends_stream_start` | TEST539: OutputStream sends STREAM_START on first write | src/bifaci/cartridge_runtime.rs:6495 |
| test540 | `test540_output_stream_close_sends_stream_end` | TEST540: OutputStream::close sends STREAM_END with correct chunk_count | src/bifaci/cartridge_runtime.rs:6518 |
| test541 | `test541_output_stream_chunks_large_data` | TEST541: OutputStream chunks large data correctly | src/bifaci/cartridge_runtime.rs:6546 |
| test542 | `test542_output_stream_empty` | TEST542: OutputStream empty stream sends STREAM_START and STREAM_END only | src/bifaci/cartridge_runtime.rs:6574 |
| test543 | `test543_peer_call_arg_creates_stream` | TEST543: PeerCall::arg creates OutputStream with correct stream_id | src/bifaci/cartridge_runtime.rs:6600 |
| test544 | `test544_peer_call_finish_sends_end` | TEST544: PeerCall::finish sends END frame | src/bifaci/cartridge_runtime.rs:6618 |
| test545 | `test545_peer_call_finish_returns_response_stream` | TEST545: PeerCall::finish returns PeerResponse with data | src/bifaci/cartridge_runtime.rs:6644 |
| test546 | `test546_is_image` | TEST546: is_image returns true only when image marker tag is present | src/urn/media_urn.rs:912 |
| test547 | `test547_is_audio` | TEST547: is_audio returns true only when audio marker tag is present | src/urn/media_urn.rs:925 |
| test548 | `test548_is_video` | TEST548: is_video returns true only when video marker tag is present | src/urn/media_urn.rs:937 |
| test549 | `test549_is_numeric` | TEST549: is_numeric returns true only when numeric marker tag is present | src/urn/media_urn.rs:948 |
| test550 | `test550_is_bool` | TEST550: is_bool returns true only when bool marker tag is present | src/urn/media_urn.rs:961 |
| test551 | `test551_is_file_path` | TEST551: is_file_path returns true for scalar file-path, false for array | src/urn/media_urn.rs:974 |
| test552 | `test552_is_file_path_array` | TEST552: is_file_path_array returns true for list file-path, false for scalar | src/urn/media_urn.rs:985 |
| test553 | `test553_is_any_file_path` | TEST553: is_any_file_path returns true for both scalar and array file-path | src/urn/media_urn.rs:995 |
| test555 | `test555_with_tag_and_without_tag` | TEST555: with_tag adds a tag and without_tag removes it | src/urn/media_urn.rs:1005 |
| test556 | `test556_image_media_urn_for_ext` | TEST556: image_media_urn_for_ext creates valid image media URN | src/urn/media_urn.rs:1022 |
| test557 | `test557_audio_media_urn_for_ext` | TEST557: audio_media_urn_for_ext creates valid audio media URN | src/urn/media_urn.rs:1032 |
| test558 | `test558_predicate_constant_consistency` | TEST558: predicates are consistent with constants — every constant triggers exactly the expected predicates | src/urn/media_urn.rs:1042 |
| test559 | `test559_without_tag` | TEST559: without_tag removes tag, ignores in/out, case-insensitive for keys | src/urn/cap_urn.rs:2073 |
| test560 | `test560_with_in_out_spec` | TEST560: with_in_spec and with_out_spec change direction specs | src/urn/cap_urn.rs:2098 |
| test561 | `test561_in_out_media_urn` | TEST561: in_media_urn and out_media_urn parse direction specs into MediaUrn | src/urn/cap_urn.rs:2122 |
| test562 | `test562_canonical_option` | TEST562: canonical_option returns None for None input, canonical string for Some | src/urn/cap_urn.rs:2144 |
| test563 | `test563_find_all_matches` | TEST563: CapMatcher::find_all_matches returns all matching caps sorted by specificity | src/urn/cap_urn.rs:2166 |
| test564 | `test564_are_compatible` | TEST564: CapMatcher::are_compatible detects bidirectional overlap | src/urn/cap_urn.rs:2185 |
| test565 | `test565_tags_to_string` | TEST565: tags_to_string returns only tags portion without prefix | src/urn/cap_urn.rs:2209 |
| test566 | `test566_with_tag_ignores_in_out` | TEST566: with_tag silently ignores in/out keys | src/urn/cap_urn.rs:2222 |
| test567 | `test567_str_variants` | TEST567: conforms_to_str and accepts_str work with string arguments | src/urn/cap_urn.rs:2236 |
| test568 | `test568_dispatch_output_tag_order` | TEST568: is_dispatchable with different tag order in output spec | src/urn/cap_urn.rs:2255 |
| test569 | `test569_unregister_cap_set` | TEST569: unregister_cap_set removes a host and returns true, false if not found | src/urn/cap_matrix.rs:1876 |
| test570 | `test570_clear` | TEST570: clear removes all registered sets | src/urn/cap_matrix.rs:1896 |
| test571 | `test571_get_all_capabilities` | TEST571: get_all_capabilities returns caps from all hosts | src/urn/cap_matrix.rs:1913 |
| test572 | `test572_get_capabilities_for_host` | TEST572: get_capabilities_for_host returns caps for specific host, None for unknown | src/urn/cap_matrix.rs:1931 |
| test573 | `test573_iter_hosts_and_caps` | TEST573: iter_hosts_and_caps iterates all hosts with their capabilities | src/urn/cap_matrix.rs:1948 |
| test574 | `test574_cap_block_remove_registry` | TEST574: CapBlock::remove_registry removes by name, returns Arc | src/urn/cap_matrix.rs:1967 |
| test575 | `test575_cap_block_get_registry` | TEST575: CapBlock::get_registry returns Arc clone by name | src/urn/cap_matrix.rs:1988 |
| test576 | `test576_cap_block_get_registry_names` | TEST576: CapBlock::get_registry_names returns names in insertion order | src/urn/cap_matrix.rs:2004 |
| test577 | `test577_cap_graph_input_output_specs` | TEST577: CapGraph::get_input_specs and get_output_specs return correct sets | src/urn/cap_matrix.rs:2019 |
| test578 | `test578_rule1_duplicate_media_urns` | TEST578: RULE1 - duplicate media_urns rejected | src/cap/validation.rs:1249 |
| test579 | `test579_rule2_empty_sources` | TEST579: RULE2 - empty sources rejected | src/cap/validation.rs:1262 |
| test580 | `test580_rule3_different_stdin_urns` | TEST580: RULE3 - multiple stdin sources with different URNs rejected | src/cap/validation.rs:1274 |
| test581 | `test581_rule3_same_stdin_urns_ok` | TEST581: RULE3 - multiple stdin sources with same URN is OK | src/cap/validation.rs:1287 |
| test582 | `test582_rule4_duplicate_source_type` | TEST582: RULE4 - duplicate source type in single arg rejected | src/cap/validation.rs:1298 |
| test583 | `test583_rule5_duplicate_position` | TEST583: RULE5 - duplicate position across args rejected | src/cap/validation.rs:1313 |
| test584 | `test584_rule6_position_gap` | TEST584: RULE6 - position gap rejected (0, 2 without 1) | src/cap/validation.rs:1326 |
| test585 | `test585_rule6_sequential_ok` | TEST585: RULE6 - sequential positions (0, 1, 2) pass | src/cap/validation.rs:1339 |
| test586 | `test586_rule7_position_and_cli_flag` | TEST586: RULE7 - arg with both position and cli_flag rejected | src/cap/validation.rs:1350 |
| test587 | `test587_rule9_duplicate_cli_flag` | TEST587: RULE9 - duplicate cli_flag across args rejected | src/cap/validation.rs:1365 |
| test588 | `test588_rule10_reserved_cli_flags` | TEST588: RULE10 - reserved cli_flags rejected | src/cap/validation.rs:1378 |
| test589 | `test589_all_rules_pass` | TEST589: valid cap args with mixed sources pass all rules | src/cap/validation.rs:1392 |
| test590 | `test590_cli_flag_only_args` | TEST590: validate_cap_args accepts cap with only cli_flag sources (no positions) | src/cap/validation.rs:1408 |
| test591 | `test591_is_more_specific_than` | TEST591: is_more_specific_than returns true when self has more tags for same request | src/cap/definition.rs:1104 |
| test592 | `test592_remove_metadata` | TEST592: remove_metadata adds then removes metadata correctly | src/cap/definition.rs:1140 |
| test593 | `test593_registered_by_lifecycle` | TEST593: registered_by lifecycle — set, get, clear | src/cap/definition.rs:1160 |
| test594 | `test594_metadata_json_lifecycle` | TEST594: metadata_json lifecycle — set, get, clear | src/cap/definition.rs:1181 |
| test595 | `test595_with_args_constructor` | TEST595: with_args constructor stores args correctly | src/cap/definition.rs:1200 |
| test596 | `test596_with_full_definition_constructor` | TEST596: with_full_definition constructor stores all fields | src/cap/definition.rs:1217 |
| test597 | `test597_cap_arg_with_full_definition` | TEST597: CapArg::with_full_definition stores all fields including optional ones | src/cap/definition.rs:1245 |
| test598 | `test598_cap_output_lifecycle` | TEST598: CapOutput lifecycle — set_output, set/clear metadata | src/cap/definition.rs:1273 |
| test599 | `test599_is_empty` | TEST599: is_empty returns true for empty response, false for non-empty | src/cap/response.rs:297 |
| test600 | `test600_size` | TEST600: size returns exact byte count for all content types | src/cap/response.rs:313 |
| test601 | `test601_get_content_type` | TEST601: get_content_type returns correct MIME type for each variant | src/cap/response.rs:329 |
| test602 | `test602_as_type_binary_error` | TEST602: as_type on binary response returns error (cannot deserialize binary) | src/cap/response.rs:342 |
| test603 | `test603_as_bool_edge_cases` | TEST603: as_bool handles all accepted truthy/falsy variants and rejects garbage | src/cap/response.rs:352 |
| test605 | `test605_all_coercion_paths_build_valid_urns` | TEST605: all_coercion_paths each entry builds a valid parseable CapUrn | src/standard/caps.rs:899 |
| test606 | `test606_coercion_urn_specs` | TEST606: coercion_urn in/out specs match the type's media URN constant | src/standard/caps.rs:919 |
| test607 | `test607_media_urns_for_extension_unknown` | TEST607: media_urns_for_extension returns error for unknown extension | src/media/registry.rs:791 |
| test608 | `test608_media_urns_for_extension_populated` | TEST608: media_urns_for_extension returns URNs after adding a spec with extensions | src/media/registry.rs:805 |
| test609 | `test609_get_extension_mappings` | TEST609: get_extension_mappings returns all registered extension->URN pairs | src/media/registry.rs:840 |
| test610 | `test610_get_cached_spec` | TEST610: get_cached_spec returns None for unknown and Some for known | src/media/registry.rs:866 |
| test611 | `test611_is_embedded_profile_comprehensive` | TEST611: is_embedded_profile recognizes all 9 embedded profiles and rejects non-embedded | src/media/profile.rs:666 |
| test612 | `test612_clear_cache` | TEST612: clear_cache empties all in-memory schemas | src/media/profile.rs:687 |
| test613 | `test613_validate_cached` | TEST613: validate_cached validates against cached standard schemas | src/media/profile.rs:704 |
| test614 | `test614_registry_creation` | TEST614: Verify registry creation succeeds and cache directory exists | src/media/registry.rs:736 |
| test615 | `test615_cache_key_generation` | TEST615: Verify cache key generation is deterministic and distinct for different URNs | src/media/registry.rs:743 |
| test616 | `test616_stored_media_spec_to_def` | TEST616: Verify StoredMediaSpec converts to MediaSpecDef preserving all fields | src/media/registry.rs:755 |
| test617 | `test617_normalize_media_urn` | TEST617: Verify normalize_media_urn produces consistent non-empty results | src/media/registry.rs:780 |
| test618 | `test618_registry_creation` | TEST618: Verify profile schema registry creation succeeds with temp cache | src/media/profile.rs:542 |
| test619 | `test619_embedded_schemas_loaded` | TEST619: Verify all 9 embedded standard schemas are loaded on creation | src/media/profile.rs:549 |
| test620 | `test620_string_validation` | TEST620: Verify string schema validates strings and rejects non-strings | src/media/profile.rs:566 |
| test621 | `test621_integer_validation` | TEST621: Verify integer schema validates integers and rejects floats and strings | src/media/profile.rs:578 |
| test622 | `test622_number_validation` | TEST622: Verify number schema validates integers and floats, rejects strings | src/media/profile.rs:593 |
| test623 | `test623_boolean_validation` | TEST623: Verify boolean schema validates true/false and rejects string "true" | src/media/profile.rs:608 |
| test624 | `test624_object_validation` | TEST624: Verify object schema validates objects and rejects arrays | src/media/profile.rs:621 |
| test625 | `test625_string_array_validation` | TEST625: Verify string array schema validates string arrays and rejects mixed arrays | src/media/profile.rs:633 |
| test626 | `test626_unknown_profile_skips_validation` | TEST626: Verify unknown profile URL skips validation and returns Ok | src/media/profile.rs:648 |
| test627 | `test627_is_embedded_profile` | TEST627: Verify is_embedded_profile recognizes standard and rejects custom URLs | src/media/profile.rs:658 |
| test628 | `test628_media_urn_constants_format` | TEST628: Verify media URN constants all start with "media:" prefix | src/standard/media.rs:69 |
| test629 | `test629_profile_constants_format` | TEST629: Verify profile URL constants all start with capdag.com schema prefix | src/standard/media.rs:79 |
| test630 | `test630_cartridge_repo_creation` | TEST630: Verify CartridgeRepo creation starts with empty cartridge list | src/bifaci/cartridge_repo.rs:584 |
| test631 | `test631_needs_sync_empty_cache` | TEST631: Verify needs_sync returns true with empty cache and non-empty URLs | src/bifaci/cartridge_repo.rs:591 |
| test632 | `test632_deserialize_cap_summary_with_null_description` | TEST632: Verify CartridgeCapSummary deserializes null description as empty string | src/bifaci/cartridge_repo.rs:599 |
| test633 | `test633_deserialize_cap_summary_with_missing_description` | TEST633: Verify CartridgeCapSummary deserializes missing description as empty string | src/bifaci/cartridge_repo.rs:609 |
| test634 | `test634_deserialize_cap_summary_with_present_description` | TEST634: Verify CartridgeCapSummary deserializes present description correctly | src/bifaci/cartridge_repo.rs:617 |
| test635 | `test635_deserialize_cartridge_info_with_null_fields` | TEST635: Verify CartridgeInfo deserializes null version/description/author as empty strings | src/bifaci/cartridge_repo.rs:625 |
| test636 | `test636_deserialize_registry_with_null_descriptions` | TEST636: Verify CartridgeRegistryResponse deserializes with mixed null/present descriptions | src/bifaci/cartridge_repo.rs:649 |
| test637 | `test637_deserialize_full_cartridge_with_signature` | TEST637: Verify full CartridgeInfo deserialization with signature and package fields | src/bifaci/cartridge_repo.rs:672 |
| test638 | `test638_no_peer_router_rejects_all` | TEST638: Verify NoPeerRouter rejects all requests with PeerInvokeNotSupported | src/bifaci/router.rs:95 |
| test639 | `test639_wildcard_001_empty_cap_defaults_to_media_wildcard` | TEST639: cap: (empty) defaults to in=media:;out=media: | src/urn/cap_urn.rs:1885 |
| test640 | `test640_wildcard_002_in_only_defaults_out_to_media` | TEST640: cap:in defaults out to media: | src/urn/cap_urn.rs:1894 |
| test641 | `test641_wildcard_003_out_only_defaults_in_to_media` | TEST641: cap:out defaults in to media: | src/urn/cap_urn.rs:1902 |
| test642 | `test642_wildcard_004_in_out_no_values_become_media` | TEST642: cap:in;out both become media: | src/urn/cap_urn.rs:1910 |
| test643 | `test643_wildcard_005_explicit_asterisk_becomes_media` | TEST643: cap:in=*;out=* becomes media: | src/urn/cap_urn.rs:1918 |
| test644 | `test644_wildcard_006_specific_in_wildcard_out` | TEST644: cap:in=media:;out=* has specific in, wildcard out | src/urn/cap_urn.rs:1926 |
| test645 | `test645_wildcard_007_wildcard_in_specific_out` | TEST645: cap:in=*;out=media:text has wildcard in, specific out | src/urn/cap_urn.rs:1934 |
| test646 | `test646_wildcard_008_invalid_in_spec_fails` | TEST646: cap:in=foo fails (invalid media URN) | src/urn/cap_urn.rs:1942 |
| test647 | `test647_wildcard_009_invalid_out_spec_fails` | TEST647: cap:in=media:;out=bar fails (invalid media URN) | src/urn/cap_urn.rs:1951 |
| test648 | `test648_wildcard_010_wildcard_accepts_specific` | TEST648: Wildcard in/out match specific caps | src/urn/cap_urn.rs:1960 |
| test649 | `test649_wildcard_011_specificity_scoring` | TEST649: Specificity - wildcard has 0, specific has tag count | src/urn/cap_urn.rs:1970 |
| test650 | `test650_wildcard_012_preserve_other_tags` | TEST650: cap:in;out;op=test preserves other tags | src/urn/cap_urn.rs:1980 |
| test651 | `test651_wildcard_013_identity_forms_equivalent` | TEST651: All identity forms produce the same CapUrn | src/urn/cap_urn.rs:1989 |
| test652 | `test652_wildcard_014_cap_identity_constant_works` | TEST652: CAP_IDENTITY constant matches identity caps regardless of string form | src/urn/cap_urn.rs:2014 |
| test653 | `test653_wildcard_015_identity_routing_isolation` | TEST653: Identity (no tags) does not match specific requests via routing | src/urn/cap_urn.rs:2044 |
| test654 | `test654_routes_req_to_handler` | TEST654: InProcessCartridgeHost routes REQ to matching handler and returns response | src/bifaci/in_process_host.rs:913 |
| test655 | `test655_identity_verification` | TEST655: InProcessCartridgeHost handles identity verification (echo nonce) | src/bifaci/in_process_host.rs:993 |
| test656 | `test656_no_handler_returns_err` | TEST656: InProcessCartridgeHost returns NO_HANDLER for unregistered cap | src/bifaci/in_process_host.rs:1064 |
| test657 | `test657_manifest_includes_all_caps` | TEST657: InProcessCartridgeHost manifest includes identity cap and handler caps | src/bifaci/in_process_host.rs:1104 |
| test658 | `test658_heartbeat_response` | TEST658: InProcessCartridgeHost handles heartbeat by echoing same ID | src/bifaci/in_process_host.rs:1122 |
| test659 | `test659_handler_error_returns_err_frame` | TEST659: InProcessCartridgeHost handler error returns ERR frame | src/bifaci/in_process_host.rs:1154 |
| test660 | `test660_closest_specificity_routing` | TEST660: InProcessCartridgeHost closest-specificity routing prefers specific over identity | src/bifaci/in_process_host.rs:1226 |
| test661 | `test661_cartridge_death_keeps_known_caps_advertised` | TEST661: Cartridge death keeps known_caps advertised for on-demand respawn | src/bifaci/host_runtime.rs:3182 |
| test662 | `test662_rebuild_capabilities_includes_non_running_cartridges` | TEST662: rebuild_capabilities includes non-running cartridges' known_caps | src/bifaci/host_runtime.rs:3213 |
| test663 | `test663_hello_failed_cartridge_removed_from_capabilities` | TEST663: Cartridge with hello_failed is permanently removed from capabilities | src/bifaci/host_runtime.rs:3246 |
| test664 | `test664_running_cartridge_uses_manifest_caps` | TEST664: Running cartridge uses manifest caps, not known_caps | src/bifaci/host_runtime.rs:3282 |
| test665 | `test665_cap_table_mixed_running_and_non_running` | TEST665: Cap table uses manifest caps for running, known_caps for non-running | src/bifaci/host_runtime.rs:3335 |
| test666 | `test666_preferred_cap_routing` | TEST666: Preferred cap routing - routes to exact equivalent when multiple masters match | src/bifaci/relay_switch.rs:2915 |
| test667 | `test667_verify_chunk_checksum_detects_corruption` | TEST667: verify_chunk_checksum detects corrupted payload | src/bifaci/frame.rs:2692 |
| test668 | `test668_resolve_slot_with_populated_byte_slot_values` |  | src/planner/argument_binding.rs:726 |
| test669 | `test669_resolve_slot_falls_back_to_default` |  | src/planner/argument_binding.rs:757 |
| test670 | `test670_resolve_required_slot_no_value_returns_err` |  | src/planner/argument_binding.rs:780 |
| test671 | `test671_resolve_optional_slot_no_value_returns_none` |  | src/planner/argument_binding.rs:802 |
| test675 | `test675_build_request_frames_preserves_media_urn_in_stream_start` | TEST675: build_request_frames with full media URN preserves it in STREAM_START frame | src/cap/caller.rs:567 |
| test676 | `test676_build_request_frames_round_trip_find_stream_succeeds` | TEST676: Full round-trip: build_request_frames → extract streams → find_stream succeeds | src/cap/caller.rs:590 |
| test677 | `test677_base_urn_does_not_match_full_urn_in_find_stream` | TEST677: build_request_frames with BASE URN → find_stream with FULL URN FAILS This documents the root cause of the cartridge_client.rs bug: sender used "media:llm-generation-request" (base), receiver looked for "media:llm-generation-request;json;record" (full). is_equivalent requires exact tag set match, so base != full. | src/cap/caller.rs:643 |
| test678 | `test678_find_stream_equivalent_urn_different_tag_order` | TEST678: find_stream with exact equivalent URN (same tags, different order) succeeds | src/bifaci/cartridge_runtime.rs:6869 |
| test679 | `test679_find_stream_base_urn_does_not_match_full_urn` | TEST679: find_stream with base URN vs full URN fails — is_equivalent is strict This is the root cause of the cartridge_client.rs bug. Sender sent "media:llm-generation-request" but receiver looked for "media:llm-generation-request;json;record". | src/bifaci/cartridge_runtime.rs:6884 |
| test680 | `test680_require_stream_missing_urn_returns_error` | TEST680: require_stream with missing URN returns hard StreamError | src/bifaci/cartridge_runtime.rs:6897 |
| test681 | `test681_find_stream_multiple_streams_returns_correct` | TEST681: find_stream with multiple streams returns the correct one | src/bifaci/cartridge_runtime.rs:6913 |
| test682 | `test682_require_stream_str_returns_utf8` | TEST682: require_stream_str returns UTF-8 string for text data | src/bifaci/cartridge_runtime.rs:6926 |
| test683 | `test683_find_stream_invalid_urn_returns_none` | TEST683: find_stream returns None for invalid media URN string (not a parse error — just None) | src/bifaci/cartridge_runtime.rs:6936 |
| test688 | `test688_is_multiple` | TEST688: Tests is_multiple method correctly identifies multi-value cardinalities Verifies Single returns false while Sequence and AtLeastOne return true | src/planner/cardinality.rs:511 |
| test689 | `test689_accepts_single` | TEST689: Tests accepts_single method identifies cardinalities that accept single values Verifies Single and AtLeastOne accept singles while Sequence does not | src/planner/cardinality.rs:520 |
| test690 | `test690_compatibility_single_to_single` | TEST690: Tests cardinality compatibility for single-to-single data flow Verifies Direct compatibility when both input and output are Single | src/planner/cardinality.rs:531 |
| test691 | `test691_compatibility_single_to_vector` | TEST691: Tests cardinality compatibility when wrapping single value into array Verifies WrapInArray compatibility when Sequence expects Single input | src/planner/cardinality.rs:538 |
| test692 | `test692_compatibility_vector_to_single` | TEST692: Tests cardinality compatibility when unwrapping array to singles Verifies RequiresFanOut compatibility when Single expects Sequence input | src/planner/cardinality.rs:545 |
| test693 | `test693_compatibility_vector_to_vector` | TEST693: Tests cardinality compatibility for sequence-to-sequence data flow Verifies Direct compatibility when both input and output are Sequence | src/planner/cardinality.rs:552 |
| test697 | `test697_cap_shape_info_one_to_one` | TEST697: Tests CapShapeInfo correctly identifies one-to-one pattern Verifies Single input and Single output result in OneToOne pattern | src/planner/cardinality.rs:561 |
| test698 | `test698_cap_shape_info_cardinality_always_single_from_urn` | TEST698: CapShapeInfo cardinality is always Single when derived from URN Cardinality comes from context (is_sequence), not from URN tags. The list tag is a semantic type property, not a cardinality indicator. | src/planner/cardinality.rs:572 |
| test699 | `test699_cap_shape_info_list_urn_still_single_cardinality` | TEST699: CapShapeInfo cardinality is Single even for list-typed URNs | src/planner/cardinality.rs:581 |
| test700 | `test700_filepath_conversion_scalar` | TEST700: File-path conversion with test-edge1 (scalar file input) | testcartridge/tests/integration_tests.rs:18 |
| test701 | `test701_filepath_array_glob` | TEST701: File-path array with glob expansion (test-edge3) | testcartridge/tests/integration_tests.rs:46 |
| test702 | `test702_large_payload_1mb` | TEST702: Large payload auto-chunking (1MB response) | testcartridge/tests/integration_tests.rs:72 |
| test703 | `test703_peer_invoke_chain` | TEST703: Cartridge chain via PeerInvoker This test is run via macino's integration test suite using --dev-bins Macino spawns testcartridge and routes peer invoke requests through its router The test-peer cap in testcartridge invokes test-edge1 and test-edge2 via PeerInvoker See macino/tests/ for the actual integration test | testcartridge/tests/integration_tests.rs:96 |
| test704 | `test704_multi_argument` | TEST704: Multi-argument cap (test-edge5) | testcartridge/tests/integration_tests.rs:103 |
| test705 | `test705_piped_stdin` | TEST705: Piped stdin input (no file-path conversion) | testcartridge/tests/integration_tests.rs:132 |
| test706 | `test706_empty_file` | TEST706: Empty file handling | testcartridge/tests/integration_tests.rs:155 |
| test707 | `test707_utf8_file` | TEST707: UTF-8 file handling (textable constraint) | testcartridge/tests/integration_tests.rs:177 |
| test708 | `test708_missing_file` | TEST708: Missing file error handling | testcartridge/tests/integration_tests.rs:200 |
| test709 | `test709_pattern_produces_vector` | TEST709: Tests CardinalityPattern correctly identifies patterns that produce vectors Verifies OneToMany and ManyToMany return true, others return false | src/planner/cardinality.rs:593 |
| test710 | `test710_pattern_requires_vector` | TEST710: Tests CardinalityPattern correctly identifies patterns that require vectors Verifies ManyToOne and ManyToMany return true, others return false | src/planner/cardinality.rs:603 |
| test711 | `test711_strand_shape_analysis_simple_linear` | TEST711: Tests shape chain analysis for simple linear one-to-one capability chains Verifies chains with no fan-out are valid and require no transformation | src/planner/cardinality.rs:615 |
| test712 | `test712_strand_shape_analysis_with_fan_out` | TEST712: Tests shape chain analysis detects fan-out points in capability chains Verifies chains with one-to-many transitions are marked for transformation | src/planner/cardinality.rs:629 |
| test713 | `test713_strand_shape_analysis_empty` | TEST713: Tests shape chain analysis handles empty capability chains correctly Verifies empty chains are valid and require no transformation | src/planner/cardinality.rs:643 |
| test714 | `test714_cardinality_serialization` | TEST714: Tests InputCardinality serializes and deserializes correctly to/from JSON Verifies JSON round-trip preserves cardinality values | src/planner/cardinality.rs:654 |
| test715 | `test715_pattern_serialization` | TEST715: Tests CardinalityPattern serializes and deserializes correctly to/from JSON Verifies JSON round-trip preserves pattern values with snake_case formatting | src/planner/cardinality.rs:665 |
| test716 | `test716_empty_collection` | TEST716: Tests CapInputCollection empty collection has zero files and folders Verifies is_empty() returns true and counts are zero for new collection | src/planner/collection_input.rs:161 |
| test717 | `test717_collection_with_files` | TEST717: Tests CapInputCollection correctly counts files in flat collection Verifies total_file_count() returns 2 for collection with 2 files, no folders | src/planner/collection_input.rs:174 |
| test718 | `test718_nested_collection` | TEST718: Tests CapInputCollection correctly counts files and folders in nested structure Verifies total_file_count() includes subfolder files and total_folder_count() counts subfolders | src/planner/collection_input.rs:198 |
| test719 | `test719_flatten_to_files` | TEST719: Tests CapInputCollection flatten_to_files recursively collects all files Verifies flatten() extracts files from root and all subfolders into flat list | src/planner/collection_input.rs:233 |
| test720 | `test720_from_media_urn_opaque` | TEST720: Tests InputStructure correctly identifies opaque media URNs Verifies that URNs without record marker are parsed as Opaque | src/planner/cardinality.rs:678 |
| test721 | `test721_from_media_urn_record` | TEST721: Tests InputStructure correctly identifies record media URNs Verifies that URNs with record marker tag are parsed as Record | src/planner/cardinality.rs:689 |
| test722 | `test722_structure_compatibility_opaque_to_opaque` | TEST722: Tests structure compatibility for opaque-to-opaque data flow | src/planner/cardinality.rs:699 |
| test723 | `test723_structure_compatibility_record_to_record` | TEST723: Tests structure compatibility for record-to-record data flow | src/planner/cardinality.rs:708 |
| test724 | `test724_structure_incompatibility_opaque_to_record` | TEST724: Tests structure incompatibility for opaque-to-record flow | src/planner/cardinality.rs:717 |
| test725 | `test725_structure_incompatibility_record_to_opaque` | TEST725: Tests structure incompatibility for record-to-opaque flow | src/planner/cardinality.rs:725 |
| test726 | `test726_apply_structure_add_record` | TEST726: Tests applying Record structure adds record marker to URN | src/planner/cardinality.rs:733 |
| test727 | `test727_apply_structure_remove_record` | TEST727: Tests applying Opaque structure removes record marker from URN | src/planner/cardinality.rs:740 |
| test728 | `test728_cap_node_helpers` | TEST728: Tests MachineNode helper methods for identifying node types (cap, fan-out, fan-in) Verifies is_cap(), is_fan_out(), is_fan_in(), and cap_urn() correctly classify node types | src/planner/plan.rs:1113 |
| test729 | `test729_edge_types` | TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, JsonField) Verifies that edge constructors produce correct EdgeType variants | src/planner/plan.rs:1135 |
| test730 | `test730_media_shape_from_urn_all_combinations` | TEST730: Tests MediaShape correctly parses all four combinations | src/planner/cardinality.rs:749 |
| test731 | `test731_media_shape_compatible_direct` | TEST731: Tests MediaShape compatibility for matching shapes | src/planner/cardinality.rs:773 |
| test732 | `test732_media_shape_cardinality_changes` | TEST732: Tests MediaShape compatibility for cardinality changes with matching structure | src/planner/cardinality.rs:788 |
| test733 | `test733_media_shape_structure_mismatch` | TEST733: Tests MediaShape incompatibility when structures don't match | src/planner/cardinality.rs:805 |
| test734 | `test734_topological_order_self_loop` | TEST734: Tests topological sort detects self-referencing cycles (A→A) Verifies that self-loops are recognized as cycles and produce an error | src/planner/plan.rs:1225 |
| test735 | `test735_topological_order_multiple_entry_points` | TEST735: Tests topological sort handles graphs with multiple independent starting nodes Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering | src/planner/plan.rs:1241 |
| test736 | `test736_topological_order_complex_dag` | TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes Verifies that all dependency constraints are satisfied in a graph with multiple converging paths | src/planner/plan.rs:1271 |
| test737 | `test737_linear_chain_single_cap` | TEST737: Tests linear_chain() with exactly one capability Verifies that a single-element chain produces a valid plan with input_slot, cap, and output | src/planner/plan.rs:1317 |
| test738 | `test738_linear_chain_empty` | TEST738: Tests linear_chain() with empty capability list Verifies that an empty chain produces a plan with zero nodes and edges | src/planner/plan.rs:1332 |
| test739 | `test739_node_execution_result_success` | TEST739: Tests NodeExecutionResult structure for successful node execution Verifies that success status, outputs (binary and text), and error fields work correctly | src/planner/plan.rs:1346 |
| test740 | `test740_cap_shape_info_from_specs` | TEST740: Tests CapShapeInfo correctly parses cap specs | src/planner/cardinality.rs:826 |
| test741 | `test741_cap_shape_info_pattern` | TEST741: Tests CapShapeInfo pattern detection | src/planner/cardinality.rs:840 |
| test742 | `test742_edge_type_serialization` | TEST742: Tests EdgeType enum serialization and deserialization to/from JSON Verifies that edge types like Direct and JsonField correctly round-trip through serde_json | src/planner/plan.rs:1408 |
| test743 | `test743_execution_node_type_serialization` | TEST743: Tests ExecutionNodeType enum serialization and deserialization to/from JSON Verifies that node types like Cap and ForEach correctly serialize with their fields | src/planner/plan.rs:1425 |
| test744 | `test744_plan_serialization` | TEST744: Tests MachinePlan serialization and deserialization to/from JSON Verifies that complete plans with nodes and edges correctly round-trip through JSON | src/planner/plan.rs:1447 |
| test745 | `test745_merge_strategy_serialization` | TEST745: Tests MergeStrategy enum serialization to JSON Verifies that merge strategies like Concat and ZipWith serialize to correct string values | src/planner/plan.rs:1468 |
| test746 | `test746_cap_node_output` | TEST746: Tests creation of Output node type that references a source node Verifies that MachineNode::output() correctly constructs an Output node with name and source | src/planner/plan.rs:1481 |
| test747 | `test747_cap_node_merge` | TEST747: Tests creation and validation of Merge node that combines multiple inputs Verifies that Merge nodes with multiple input nodes and a strategy can be added to plans | src/planner/plan.rs:1495 |
| test748 | `test748_cap_node_split` | TEST748: Tests creation of Split node that distributes input to multiple outputs Verifies that Split nodes correctly specify an input node and output count | src/planner/plan.rs:1520 |
| test749 | `test749_get_node` | TEST749: Tests get_node() method for looking up nodes by ID in a plan Verifies that existing nodes are found and non-existent nodes return None | src/planner/plan.rs:1542 |
| test750 | `test750_strand_shape_valid` | TEST750: Tests shape chain analysis for valid chain with matching structures | src/planner/cardinality.rs:853 |
| test751 | `test751_strand_shape_structure_mismatch` | TEST751: Tests shape chain analysis detects structure mismatch | src/planner/cardinality.rs:865 |
| test752 | `test752_strand_shape_with_fanout` | TEST752: Tests shape chain analysis with fan-out (matching structures) | src/planner/cardinality.rs:879 |
| test753 | `test753_strand_shape_list_record_to_list_record` | TEST753: Tests shape chain analysis correctly handles list-to-list record flow | src/planner/cardinality.rs:892 |
| test754 | `test754_extract_prefix_nonexistent` | TEST754: extract_prefix_to with nonexistent node returns error | src/planner/plan.rs:1674 |
| test755 | `test755_extract_foreach_body` | TEST755: extract_foreach_body extracts body as standalone plan | src/planner/plan.rs:1682 |
| test756 | `test756_extract_foreach_body_unclosed` | TEST756: extract_foreach_body for unclosed ForEach (single body cap) | src/planner/plan.rs:1718 |
| test757 | `test757_extract_foreach_body_wrong_type` | TEST757: extract_foreach_body fails for non-ForEach node | src/planner/plan.rs:1734 |
| test758 | `test758_extract_suffix_from` | TEST758: extract_suffix_from extracts collect → cap_post → output | src/planner/plan.rs:1744 |
| test759 | `test759_extract_suffix_nonexistent` | TEST759: extract_suffix_from fails for nonexistent node | src/planner/plan.rs:1764 |
| test760 | `test760_decomposition_covers_all_caps` | TEST760: Full decomposition roundtrip — prefix + body + suffix cover all cap nodes | src/planner/plan.rs:1772 |
| test761 | `test761_prefix_is_dag` | TEST761: Prefix sub-plan can be topologically sorted (is a valid DAG) | src/planner/plan.rs:1811 |
| test762 | `test762_body_is_dag` | TEST762: Body sub-plan can be topologically sorted (is a valid DAG) | src/planner/plan.rs:1819 |
| test763 | `test763_suffix_is_dag` | TEST763: Suffix sub-plan can be topologically sorted (is a valid DAG) | src/planner/plan.rs:1827 |
| test764 | `test764_extract_prefix_to_input_slot` | TEST764: extract_prefix_to with InputSlot as target (trivial prefix) | src/planner/plan.rs:1835 |
| test765 | `test765_validation_to_json_empty` | TEST765: Tests validation_to_json() returns None for empty validation constraints Verifies that default MediaValidation with no constraints produces JSON None | src/planner/plan_builder.rs:991 |
| test766 | `test766_validation_to_json_with_constraints` | TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON Verifies that min/max validation rules are correctly serialized as JSON fields | src/planner/plan_builder.rs:1000 |
| test767 | `test767_argument_info_serialization` | TEST767: Tests ArgumentInfo struct serialization to JSON Verifies that argument metadata including resolution status and validation is correctly serialized | src/planner/plan_builder.rs:1019 |
| test768 | `test768_path_argument_requirements_structure` | TEST768: Tests PathArgumentRequirements structure for single-step execution paths Verifies that argument requirements are correctly organized by step with resolution information | src/planner/plan_builder.rs:1040 |
| test769 | `test769_path_with_required_slot` | TEST769: Tests PathArgumentRequirements tracking of required user-input slots Verifies that arguments requiring user input are collected in slots and can_execute_without_input is false | src/planner/plan_builder.rs:1076 |
| test770 | `test770_rejects_foreach` | TEST770: plan_to_resolved_graph rejects plans containing ForEach nodes | src/orchestrator/plan_converter.rs:274 |
| test771 | `test771_rejects_collect` | TEST771: plan_to_resolved_graph rejects plans containing Collect nodes | src/orchestrator/plan_converter.rs:301 |
| test772 | `test772_find_paths_finds_multi_step_paths` | TEST772: Tests find_paths_to_exact_target() finds multi-step paths Verifies that paths through intermediate nodes are found correctly | src/planner/live_cap_graph.rs:1354 |
| test773 | `test773_find_paths_returns_empty_when_no_path` | TEST773: Tests find_paths_to_exact_target() returns empty when no path exists Verifies that pathfinding returns no paths when target is unreachable | src/planner/live_cap_graph.rs:1377 |
| test774 | `test774_get_reachable_targets_finds_all_targets` | TEST774: Tests get_reachable_targets() returns all reachable targets Verifies that reachable targets include direct cap targets and cardinality variants (list versions via Collect) | src/planner/live_cap_graph.rs:1396 |
| test777 | `test777_type_mismatch_pdf_cap_does_not_match_png_input` | TEST777: Tests type checking prevents using PDF-specific cap with PNG input Verifies that media type compatibility is enforced during pathfinding | src/planner/live_cap_graph.rs:1427 |
| test778 | `test778_type_mismatch_png_cap_does_not_match_pdf_input` | TEST778: Tests type checking prevents using PNG-specific cap with PDF input Verifies that media type compatibility is enforced during pathfinding | src/planner/live_cap_graph.rs:1446 |
| test779 | `test779_get_reachable_targets_respects_type_matching` | TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps Verifies that PNG and PDF inputs reach different cap targets (not each other's) | src/planner/live_cap_graph.rs:1465 |
| test780 | `test780_split_integer_array` | TEST780: split_cbor_array splits a simple array of integers | src/orchestrator/cbor_util.rs:152 |
| test781 | `test781_find_paths_respects_type_chain` | TEST781: Tests find_paths_to_exact_target() enforces type compatibility across multi-step chains Verifies that paths are only found when all intermediate types are compatible | src/planner/live_cap_graph.rs:1516 |
| test782 | `test782_split_non_array` | TEST782: split_cbor_array rejects non-array input | src/orchestrator/cbor_util.rs:193 |
| test783 | `test783_split_empty_array` | TEST783: split_cbor_array rejects empty array | src/orchestrator/cbor_util.rs:203 |
| test784 | `test784_split_invalid_cbor` | TEST784: split_cbor_array rejects invalid CBOR bytes | src/orchestrator/cbor_util.rs:213 |
| test785 | `test785_assemble_integer_array` | TEST785: assemble_cbor_array creates array from individual items | src/orchestrator/cbor_util.rs:220 |
| test786 | `test786_roundtrip_split_assemble` | TEST786: split then assemble roundtrip preserves data | src/orchestrator/cbor_util.rs:244 |
| test787 | `test787_find_paths_sorting_prefers_shorter` | TEST787: Tests find_paths_to_exact_target() sorts paths by length, preferring shorter ones Verifies that among multiple paths, the shortest is ranked first | src/planner/live_cap_graph.rs:1685 |
| test788 | `test788_foreach_only_with_sequence_input` | TEST788: ForEach is only synthesized when is_sequence=true With scalar input (is_sequence=false), disbind output goes directly to choose since media:page;textable conforms to media:textable. With sequence input (is_sequence=true), ForEach splits the sequence so each item can be processed by disbind individually, then choose. | src/planner/live_cap_graph.rs:1544 |
| test789 | `test789_cap_from_json_has_valid_specs` | TEST789: Tests that caps loaded from JSON have correct in_spec/out_spec | src/planner/live_cap_graph.rs:1658 |
| test790 | `test790_identity_urn_is_specific` | TEST790: Tests identity_urn is specific and doesn't match everything | src/planner/live_cap_graph.rs:1630 |
| test791 | `test791_sync_from_cap_urns_adds_edges` | TEST791: Tests sync_from_cap_urns actually adds edges | src/planner/live_cap_graph.rs:1585 |
| test792 | `test792_argument_binding_requires_input` | TEST792: Tests ArgumentBinding requires_input distinguishes Slots from Literals Verifies Slot returns true (needs user input) while Literal returns false | src/planner/argument_binding.rs:559 |
| test793 | `test793_argument_binding_serialization` | TEST793: Tests ArgumentBinding PreviousOutput serializes/deserializes correctly Verifies JSON round-trip preserves node_id and output_field values | src/planner/argument_binding.rs:569 |
| test794 | `test794_argument_bindings_add_file_path` | TEST794: Tests ArgumentBindings add_file_path adds InputFilePath binding Verifies add_file_path() creates binding map entry with InputFilePath variant | src/planner/argument_binding.rs:589 |
| test795 | `test795_argument_bindings_unresolved_slots` | TEST795: Tests ArgumentBindings identifies unresolved Slot bindings Verifies has_unresolved_slots() and get_unresolved_slots() detect Slots needing values | src/planner/argument_binding.rs:599 |
| test796 | `test796_resolve_input_file_path` | TEST796: Tests resolve_binding resolves InputFilePath to current file path Verifies InputFilePath binding resolves to file path bytes with InputFile source | src/planner/argument_binding.rs:610 |
| test797 | `test797_resolve_literal` | TEST797: Tests resolve_binding resolves Literal to JSON-encoded bytes Verifies Literal binding serializes value to bytes with Literal source | src/planner/argument_binding.rs:630 |
| test798 | `test798_resolve_previous_output` | TEST798: Tests resolve_binding extracts value from previous node output Verifies PreviousOutput binding fetches field from earlier execution results | src/planner/argument_binding.rs:650 |
| test799 | `test799_machine_input_single` | TEST799: Tests StrandInput single constructor creates valid Single cardinality input Verifies single() wraps one file with Single cardinality and validates correctly | src/planner/argument_binding.rs:674 |
| test800 | `test800_machine_input_vector` | TEST800: Tests StrandInput sequence constructor creates valid Sequence cardinality input Verifies sequence() wraps multiple files with Sequence cardinality | src/planner/argument_binding.rs:685 |
| test801 | `test801_cap_input_file_deserialization_from_dry_context` | TEST801: Tests CapInputFile deserializes from JSON with source metadata fields Verifies JSON with source_id and source_type deserializes to CapInputFile correctly | src/planner/argument_binding.rs:699 |
| test802 | `test802_cap_input_file_deserialization_via_value` | TEST802: Tests CapInputFile deserializes from compact JSON via serde_json::Value Verifies deserialization through Value intermediate works correctly | src/planner/argument_binding.rs:718 |
| test803 | `test803_machine_input_invalid_single` | TEST803: Tests StrandInput validation detects mismatched Single cardinality with multiple files Verifies is_valid() returns false when Single cardinality has more than one file | src/planner/argument_binding.rs:824 |
| test804 | `test804_extract_json_path_simple` | TEST804: Tests basic JSON path extraction with dot notation for nested objects Verifies that simple paths like "data.message" correctly extract values from nested JSON structures | src/planner/executor.rs:603 |
| test805 | `test805_extract_json_path_with_array` | TEST805: Tests JSON path extraction with array indexing syntax Verifies that bracket notation like "items[0].name" correctly accesses array elements and their nested fields | src/planner/executor.rs:617 |
| test806 | `test806_extract_json_path_missing_field` | TEST806: Tests error handling when JSON path references non-existent fields Verifies that accessing missing fields returns an appropriate error message | src/planner/executor.rs:632 |
| test807 | `test807_apply_edge_type_direct` | TEST807: Tests EdgeType::Direct passes JSON values through unchanged Verifies that Direct edge type acts as a transparent passthrough without transformation | src/planner/executor.rs:643 |
| test808 | `test808_apply_edge_type_json_field` | TEST808: Tests EdgeType::JsonField extracts specific top-level fields from JSON objects Verifies that JsonField edge type correctly isolates a single named field from the source output | src/planner/executor.rs:653 |
| test809 | `test809_apply_edge_type_json_field_missing` | TEST809: Tests EdgeType::JsonField error handling for missing fields Verifies that attempting to extract a non-existent field returns an error | src/planner/executor.rs:663 |
| test810 | `test810_apply_edge_type_json_path` | TEST810: Tests EdgeType::JsonPath extracts values using nested path expressions Verifies that JsonPath edge type correctly navigates through multiple levels like "data.nested.value" | src/planner/executor.rs:672 |
| test811 | `test811_apply_edge_type_iteration` | TEST811: Tests EdgeType::Iteration preserves array values for iterative processing Verifies that Iteration edge type passes through arrays unchanged to enable ForEach patterns | src/planner/executor.rs:682 |
| test812 | `test812_apply_edge_type_collection` | TEST812: Tests EdgeType::Collection preserves collected values without transformation Verifies that Collection edge type maintains structure for aggregation patterns | src/planner/executor.rs:692 |
| test813 | `test813_extract_json_path_deeply_nested` | TEST813: Tests JSON path extraction through deeply nested object hierarchies (4+ levels) Verifies that paths can traverse multiple nested levels like "level1.level2.level3.level4.value" | src/planner/executor.rs:702 |
| test814 | `test814_extract_json_path_array_out_of_bounds` | TEST814: Tests error handling when array index exceeds available elements Verifies that out-of-bounds array access returns a descriptive error message | src/planner/executor.rs:722 |
| test815 | `test815_extract_json_path_single_segment` | TEST815: Tests JSON path extraction with single-level paths (no nesting) Verifies that simple field names without dots correctly extract top-level values | src/planner/executor.rs:735 |
| test816 | `test816_extract_json_path_with_special_characters` | TEST816: Tests JSON path extraction preserves special characters in string values Verifies that quotes, backslashes, and other special characters are correctly maintained | src/planner/executor.rs:745 |
| test817 | `test817_extract_json_path_with_null_value` | TEST817: Tests JSON path extraction correctly handles explicit null values Verifies that null is returned as serde_json::Value::Null rather than an error | src/planner/executor.rs:759 |
| test818 | `test818_extract_json_path_with_empty_array` | TEST818: Tests JSON path extraction correctly returns empty arrays Verifies that zero-length arrays are extracted as valid empty array values | src/planner/executor.rs:769 |
| test819 | `test819_extract_json_path_with_numeric_types` | TEST819: Tests JSON path extraction handles various numeric types correctly Verifies extraction of integers, floats, negative numbers, and zero | src/planner/executor.rs:779 |
| test820 | `test820_extract_json_path_with_boolean` | TEST820: Tests JSON path extraction correctly handles boolean values Verifies that true and false are extracted as proper boolean JSON values | src/planner/executor.rs:795 |
| test821 | `test821_extract_json_path_with_nested_arrays` | TEST821: Tests JSON path extraction with multi-dimensional arrays (matrix access) Verifies that nested array structures like "matrix[1]" correctly extract inner arrays | src/planner/executor.rs:809 |
| test822 | `test822_extract_json_path_invalid_array_index` | TEST822: Tests error handling for non-numeric array indices Verifies that invalid indices like "items[abc]" return a descriptive parse error | src/planner/executor.rs:824 |
| test823 | `test823_dispatch_exact_match` | TEST823: is_dispatchable — exact match provider dispatches request | src/urn/cap_urn.rs:2276 |
| test824 | `test824_dispatch_contravariant_input` | TEST824: is_dispatchable — provider with broader input handles specific request (contravariance) | src/urn/cap_urn.rs:2288 |
| test825 | `test825_dispatch_request_unconstrained_input` | TEST825: is_dispatchable — request with unconstrained input dispatches to specific provider media: on the request input axis means "unconstrained" — vacuously true | src/urn/cap_urn.rs:2301 |
| test826 | `test826_dispatch_covariant_output` | TEST826: is_dispatchable — provider output must satisfy request output (covariance) | src/urn/cap_urn.rs:2314 |
| test827 | `test827_dispatch_generic_output_fails` | TEST827: is_dispatchable — provider with generic output cannot satisfy specific request | src/urn/cap_urn.rs:2327 |
| test828 | `test828_dispatch_wildcard_requires_tag_presence` | TEST828: is_dispatchable — wildcard * tag in request, provider missing tag → reject | src/urn/cap_urn.rs:2340 |
| test829 | `test829_dispatch_wildcard_with_tag_present` | TEST829: is_dispatchable — wildcard * tag in request, provider has tag → accept | src/urn/cap_urn.rs:2353 |
| test830 | `test830_dispatch_provider_extra_tags` | TEST830: is_dispatchable — provider extra tags are refinement, always OK | src/urn/cap_urn.rs:2366 |
| test831 | `test831_dispatch_cross_backend_mismatch` | TEST831: is_dispatchable — cross-backend mismatch prevented | src/urn/cap_urn.rs:2379 |
| test832 | `test832_dispatch_asymmetric` | TEST832: is_dispatchable is NOT symmetric | src/urn/cap_urn.rs:2392 |
| test833 | `test833_comparable_symmetric` | TEST833: is_comparable — both directions checked | src/urn/cap_urn.rs:2412 |
| test834 | `test834_comparable_unrelated` | TEST834: is_comparable — unrelated caps are NOT comparable | src/urn/cap_urn.rs:2425 |
| test835 | `test835_equivalent_identical` | TEST835: is_equivalent — identical caps | src/urn/cap_urn.rs:2438 |
| test836 | `test836_equivalent_non_equivalent` | TEST836: is_equivalent — non-equivalent comparable caps | src/urn/cap_urn.rs:2451 |
| test837 | `test837_dispatch_op_mismatch` | TEST837: is_dispatchable — op tag mismatch rejects | src/urn/cap_urn.rs:2464 |
| test838 | `test838_dispatch_request_wildcard_output` | TEST838: is_dispatchable — request with wildcard output accepts any provider output | src/urn/cap_urn.rs:2476 |
| test839 | `test839_peer_response_delivers_logs_before_stream_start` | TEST839: LOG frames arriving BEFORE StreamStart are delivered immediately  This tests the critical fix: during a peer call, the peer (e.g., modelcartridge) sends LOG frames for minutes during model download BEFORE sending any data (StreamStart + Chunk). The handler must receive these LOGs in real-time so it can re-emit progress and keep the engine's activity timer alive.  Previously, demux_single_stream blocked on awaiting StreamStart before returning PeerResponse, which meant the handler couldn't call recv() until data arrived — causing 120s activity timeouts during long downloads. | src/bifaci/cartridge_runtime.rs:6699 |
| test840 | `test840_peer_response_collect_bytes_discards_logs` | TEST840: PeerResponse::collect_bytes discards LOG frames | src/bifaci/cartridge_runtime.rs:6780 |
| test841 | `test841_peer_response_collect_value_discards_logs` | TEST841: PeerResponse::collect_value discards LOG frames | src/bifaci/cartridge_runtime.rs:6825 |
| test842 | `test842_run_with_keepalive_returns_result` | TEST842: run_with_keepalive returns closure result (fast operation, no keepalive frames) | src/bifaci/cartridge_runtime.rs:6947 |
| test843 | `test843_run_with_keepalive_returns_result_type` | TEST843: run_with_keepalive returns Ok/Err from closure | src/bifaci/cartridge_runtime.rs:6972 |
| test844 | `test844_run_with_keepalive_propagates_error` | TEST844: run_with_keepalive propagates errors from closure | src/bifaci/cartridge_runtime.rs:6991 |
| test845 | `test845_progress_sender_emits_frames` | TEST845: ProgressSender emits progress and log frames independently of OutputStream | src/bifaci/cartridge_runtime.rs:7015 |
| test846 | `test846_progress_frame_roundtrip` | TEST846: Test progress LOG frame encode/decode roundtrip preserves progress float | src/bifaci/io.rs:920 |
| test847 | `test847_progress_double_roundtrip` | TEST847: Double roundtrip (modelcartridge → relay → candlecartridge) | src/bifaci/io.rs:976 |
| test848 | `test848_relay_notify_roundtrip` | TEST848: RelayNotify encode/decode roundtrip preserves manifest and limits | src/bifaci/io.rs:1520 |
| test849 | `test849_relay_state_roundtrip` | TEST849: RelayState encode/decode roundtrip preserves resource payload | src/bifaci/io.rs:1538 |
| test850 | `test850_all_format_conversion_paths_build_valid_urns` | TEST850: all_format_conversion_paths each entry builds a valid parseable CapUrn | src/standard/caps.rs:938 |
| test851 | `test851_format_conversion_urn_specs` | TEST851: format_conversion_urn in/out specs match the input constants | src/standard/caps.rs:958 |
| test852 | `test852_lub_identical` | TEST852: LUB of identical URNs returns the same URN | src/urn/media_urn.rs:1079 |
| test853 | `test853_lub_no_common_tags` | TEST853: LUB of URNs with no common tags returns media: (universal) | src/urn/media_urn.rs:1087 |
| test854 | `test854_lub_partial_overlap` | TEST854: LUB keeps common tags, drops differing ones | src/urn/media_urn.rs:1098 |
| test855 | `test855_lub_list_vs_scalar` | TEST855: LUB of list and non-list drops list tag | src/urn/media_urn.rs:1109 |
| test856 | `test856_lub_empty` | TEST856: LUB of empty input returns universal type | src/urn/media_urn.rs:1120 |
| test857 | `test857_lub_single` | TEST857: LUB of single input returns that input | src/urn/media_urn.rs:1128 |
| test858 | `test858_lub_three_inputs` | TEST858: LUB with three+ inputs narrows correctly | src/urn/media_urn.rs:1136 |
| test859 | `test859_lub_valued_tags` | TEST859: LUB with valued tags (non-marker) that differ | src/urn/media_urn.rs:1148 |
| test860 | `test860_seq_assigner_same_rid_different_xids_independent` | TEST860: Same RID with different XIDs get independent seq counters | src/bifaci/frame.rs:1683 |
| test880 | `test880_no_duplicates_with_unique_caps` | TEST887: Tests duplicate detection passes for caps with unique URN combinations Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations | src/planner/plan_builder.rs:775 |
| test881 | `test881_pdf_full_intelligence_pipeline` | TEST016: Complete PDF intelligence pipeline with cross-cartridge image embedding | tests/cartridge_scenarios.rs:1008 |
| test882 | `test882_candle_describe_image` | TEST022: Generate image description with BLIP via candlecartridge | tests/cartridge_scenarios.rs:1335 |
| test883 | `test883_text_embedding` | TEST021: Generate text embedding with BERT via candlecartridge | tests/cartridge_scenarios.rs:1285 |
| test884 | `test884_model_availability_plus_status` | TEST020: Model spec fan-out to availability and status checks | tests/cartridge_scenarios.rs:1243 |
| test885 | `test885_model_plus_dimensions` | TEST019: Fan-out from model spec to availability check and embedding dimensions | tests/cartridge_scenarios.rs:1189 |
| test886 | `test886_optional_non_io_arg_with_default_has_default` | TEST886: Tests optional non-IO arguments with default values are marked as HasDefault Verifies that optional arguments with defaults behave the same as required ones with defaults | src/planner/plan_builder.rs:960 |
| test887 | `test887_execute_with_file_input` | TEST004: Execute with file-path input | tests/orchestrator_integration.rs:343 |
| test888 | `test888_execute_edge1_to_edge2_chain` | TEST003: Execute two-edge chain (test-edge1 -> test-edge2) | tests/orchestrator_integration.rs:302 |
| test889 | `test889_execute_single_edge_dag` | TEST002: Execute single-edge DAG (test-edge1) | tests/orchestrator_integration.rs:259 |
| test890 | `test890_direction_semantic_matching` | TEST890: Semantic direction matching - generic provider matches specific request | src/urn/cap_urn.rs:1797 |
| test891 | `test891_direction_semantic_specificity` | TEST891: Semantic direction specificity - more media URN tags = higher specificity | src/urn/cap_urn.rs:1854 |
| test892 | `test892_extensions_serialization` | TEST892: Test extensions serializes/deserializes correctly in MediaSpecDef | src/media/spec.rs:1124 |
| test893 | `test893_extensions_with_metadata_and_validation` | TEST893: Test extensions can coexist with metadata and validation | src/media/spec.rs:1147 |
| test894 | `test894_multiple_extensions` | TEST894: Test multiple extensions in a media spec | src/media/spec.rs:1183 |
| test895 | `test895_cap_output_media_specs_have_extensions` | TEST895: All cap output media specs must have file extensions defined. This is a regression guard: every media URN used as a cap output (out= in cap TOML) produces user-facing files. If a spec lacks extensions, save_cap_output and FinderImportService will fail at runtime. | src/media/registry.rs:894 |
| test896 | `test896_cap_input_media_specs_have_extensions` | TEST896: All cap input media specs that represent user files must have extensions. These are the entry points — the file types users can right-click on. | src/media/registry.rs:949 |
| test897 | `test897_cap_output_extension_values_correct` | TEST897: Verify that specific cap output URNs resolve to the correct extension. This catches misconfigurations where a spec exists but has the wrong extension. | src/media/registry.rs:996 |
| test898 | `test898_binary_integrity_through_relay` | TEST898: Binary data integrity through full relay path (256 byte values) | src/bifaci/integration_tests.rs:340 |
| test899 | `test899_streaming_chunks_through_relay` | TEST899: Streaming chunks flow through relay without accumulation | src/bifaci/integration_tests.rs:454 |
| test900 | `test900_two_cartridges_routed_independently` | TEST900: Two cartridges routed independently by cap_urn | src/bifaci/integration_tests.rs:550 |
| test901 | `test901_req_for_unknown_cap_returns_err_frame` | TEST901: REQ for unknown cap returns ERR frame (not fatal) | src/bifaci/integration_tests.rs:683 |
| test902 | `test902_compute_checksum_empty` | TEST902: Verify FNV-1a checksum handles empty data | src/bifaci/frame.rs:1531 |
| test903 | `test903_chunk_with_chunk_index_and_checksum` | TEST903: Verify CHUNK frame can store chunk_index and checksum fields | src/bifaci/frame.rs:1539 |
| test904 | `test904_stream_end_with_chunk_count` | TEST904: Verify STREAM_END frame can store chunk_count field | src/bifaci/frame.rs:1557 |
| test905 | `test905_send_to_master_build_request_frames_roundtrip` | TEST905: send_to_master + build_request_frames through RelaySwitch → RelaySlave → InProcessCartridgeHost roundtrip | src/bifaci/relay_switch.rs:2620 |
| test906 | `test906_full_path_identity_verification` | TEST489: Full path identity verification: engine → host (attach_cartridge) → cartridge  This verifies that attach_cartridge completes identity verification end-to-end and the cartridge is ready to handle subsequent requests. | src/bifaci/integration_tests.rs:1138 |
| test907 | `test907_offline_blocks_fetch` | TEST907: Offline flag blocks fetch_from_registry without making HTTP request | src/cap/registry.rs:702 |
| test908 | `test908_cached_caps_accessible_when_offline` | TEST908: Cached caps remain accessible when offline | src/cap/registry.rs:721 |
| test909 | `test909_set_offline_false_restores_fetch` | TEST909: set_offline(false) restores fetch ability (would fail with HTTP error, not NetworkBlocked) | src/cap/registry.rs:740 |
| test910 | `test910_map_progress_monotonic` | TEST910: map_progress output is monotonic for monotonically increasing input | src/orchestrator/executor.rs:1475 |
| test911 | `test911_map_progress_bounded` | TEST911: map_progress output is bounded within [base, base+weight] | src/orchestrator/executor.rs:1491 |
| test912 | `test912_progress_mapper_reports_through_parent` | TEST912: ProgressMapper correctly maps through a CapProgressFn | src/orchestrator/executor.rs:1507 |
| test913 | `test913_progress_mapper_as_cap_progress_fn` | TEST913: ProgressMapper.as_cap_progress_fn produces same mapping | src/orchestrator/executor.rs:1528 |
| test914 | `test914_progress_mapper_sub_mapper` | TEST914: ProgressMapper.sub_mapper chains correctly | src/orchestrator/executor.rs:1551 |
| test915 | `test915_per_group_subdivision_monotonic_bounded` | TEST915: Per-group subdivision produces monotonic, bounded progress for N groups  Uses pre-computed boundaries (same pattern as production code) to guarantee monotonicity regardless of f32 rounding. | src/orchestrator/executor.rs:1578 |
| test916 | `test916_foreach_item_subdivision` | TEST916: ForEach item subdivision produces correct, monotonic ranges  Mirrors the production code in interpreter.rs: pre-compute item boundaries from the same formula so the end of item N and the start of item N+1 are the same f32 value (no divergent accumulation paths). | src/orchestrator/executor.rs:1634 |
| test917 | `test917_high_frequency_progress_bounded` | TEST917: High-frequency progress emission does not violate bounds (Regression test for the deadlock scenario — verifies computation stays bounded) | src/orchestrator/executor.rs:1681 |
| test918 | `test918_activity_timeout_error_display` | TEST918: ActivityTimeout error formats correctly | src/orchestrator/executor.rs:1714 |
| test919 | `test919_parse_simple_testcartridge_graph` | TEST001: Parse simple machine notation graph with test-edge1 | tests/orchestrator_integration.rs:235 |
| test920 | `test920_single_cap_plan` | TEST920: Tests creation of a simple execution plan with a single capability Verifies that single_cap() generates a valid plan with input_slot, cap node, and output node | src/planner/plan.rs:989 |
| test921 | `test921_linear_chain_plan` | TEST921: Tests creation of a linear chain of capabilities connected in sequence Verifies that linear_chain() correctly links multiple caps with proper edges and topological order | src/planner/plan.rs:1005 |
| test922 | `test922_empty_plan` | TEST922: Tests creation and validation of an empty execution plan with no nodes Verifies that plans without capabilities are valid and handle zero nodes correctly | src/planner/plan.rs:1023 |
| test923 | `test923_plan_with_metadata` | TEST923: Tests storing and retrieving metadata attached to an execution plan Verifies that arbitrary JSON metadata can be associated with a plan for context preservation | src/planner/plan.rs:1032 |
| test924 | `test924_validate_invalid_edge` | TEST924: Tests plan validation detects edges pointing to non-existent nodes Verifies that validate() returns an error when an edge references a missing to_node | src/planner/plan.rs:1049 |
| test925 | `test925_topological_order_diamond` | TEST925: Tests topological sort correctly orders a diamond-shaped DAG (A->B,C->D) Verifies that nodes with multiple paths respect dependency constraints (A first, D last) | src/planner/plan.rs:1066 |
| test926 | `test926_topological_order_detects_cycle` | TEST926: Tests topological sort detects and rejects cyclic dependencies (A->B->C->A) Verifies that circular references produce a "Cycle detected" error | src/planner/plan.rs:1092 |
| test927 | `test927_execution_result` | TEST927: Tests MachineResult structure for successful execution outcomes Verifies that success status, outputs, and primary_output() accessor work correctly | src/planner/plan.rs:1152 |
| test928 | `test928_validate_invalid_from_node` | TEST928: Tests plan validation detects edges originating from non-existent nodes Verifies that validate() returns an error when an edge references a missing from_node | src/planner/plan.rs:1172 |
| test929 | `test929_validate_invalid_entry_node` | TEST929: Tests plan validation detects invalid entry node references Verifies that validate() returns an error when entry_nodes contains a non-existent node ID | src/planner/plan.rs:1189 |
| test930 | `test930_validate_invalid_output_node` | TEST930: Tests plan validation detects invalid output node references Verifies that validate() returns an error when output_nodes contains a non-existent node ID | src/planner/plan.rs:1207 |
| test931 | `test931_node_execution_result_failure` | TEST931: Tests NodeExecutionResult structure for failed node execution Verifies that failure status, error message, and absence of outputs are correctly represented | src/planner/plan.rs:1368 |
| test932 | `test932_execution_result_failure` | TEST932: Tests MachineResult structure for failed chain execution Verifies that failure status, error message, and absence of outputs are correctly represented | src/planner/plan.rs:1390 |
| test933 | `test933_serialization_roundtrip` | TEST933: Tests CapInputCollection serializes to JSON and deserializes correctly Verifies JSON round-trip preserves folder_id, folder_name, files and file metadata | src/planner/collection_input.rs:265 |
| test934 | `test934_find_first_foreach` | TEST934: find_first_foreach detects ForEach in a plan | src/planner/plan.rs:1603 |
| test935 | `test935_find_first_foreach_linear` | TEST935: find_first_foreach returns None for linear plans | src/planner/plan.rs:1611 |
| test936 | `test936_has_foreach` | TEST936: has_foreach detects ForEach nodes | src/planner/plan.rs:1623 |
| test937 | `test937_extract_prefix_to` | TEST937: extract_prefix_to extracts input_slot -> cap_0 as a standalone plan | src/planner/plan.rs:1652 |
| test944 | `test944_six_machine` | TEST013: 6-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9 -> edge10 Full cycle: node1 -> node2 -> node3 -> node6 -> node7 -> node8 -> node1 Completes the round trip: unwrap markers + lowercase | tests/orchestrator_integration.rs:651 |
| test945 | `test945_five_machine` | TEST012: 5-machine: edge1 -> edge2 -> edge7 -> edge8 -> edge9 node1 -> node2 -> node3 -> node6 -> node7 -> node8 adds <<...>> wrapping around the reversed string | tests/orchestrator_integration.rs:602 |
| test946 | `test946_four_machine` | TEST011: 4-machine: edge1 -> edge2 -> edge7 -> edge8 node1 -> node2 -> node3 -> node6 -> node7 "hello" -> "[PREPEND]hello" -> "[PREPEND]hello[APPEND]" -> "[PREPEND]HELLO[APPEND]" -> "]DNEPPA[OLLEH]DNEPERP[" | tests/orchestrator_integration.rs:553 |
| test947 | `test947_cap_not_found` | TEST010: Cap not found in registry | tests/orchestrator_integration.rs:526 |
| test948 | `test948_invalid_cap_urn` | TEST009: Invalid cap URN in machine notation | tests/orchestrator_integration.rs:512 |
| test949 | `test949_empty_graph` | TEST008: Empty machine notation (no edges) | tests/orchestrator_integration.rs:494 |
| test950 | `test950_reject_cycles` | TEST007: Validate that cycles are rejected | tests/orchestrator_integration.rs:472 |
| test951 | `test951_fan_in_pattern` | TEST006: Multi-input DAG (fan-in pattern) | tests/orchestrator_integration.rs:426 |
| test952 | `test952_execute_large_payload` | TEST005: Execute large payload (test-large cap) | tests/orchestrator_integration.rs:384 |
| test953 | `test953_linear_plan_still_works` | TEST953: Linear plans (no ForEach/Collect) still convert successfully | src/orchestrator/plan_converter.rs:331 |
| test954 | `test954_standalone_collect_passthrough` | TEST954: Standalone Collect nodes are handled as pass-through Plan: input → cap_0 → Collect → cap_1 → output The standalone Collect is transparent — the resolved edge from Collect to cap_1 should be rewritten to go from cap_0 to cap_1 directly. | src/orchestrator/plan_converter.rs:352 |
| test955 | `test955_split_map_array` | TEST955: split_cbor_array with nested maps | src/orchestrator/cbor_util.rs:172 |
| test956 | `test956_roundtrip_assemble_split` | TEST956: assemble then split roundtrip preserves data | src/orchestrator/cbor_util.rs:263 |
| test957 | `test957_cap_input_file_new` | TEST957: Tests CapInputFile constructor creates file with correct path and media URN Verifies new() initializes file_path, media_urn and leaves metadata/source_id as None | src/planner/argument_binding.rs:519 |
| test958 | `test958_cap_input_file_from_listing` | TEST958: Tests CapInputFile from_listing sets source metadata correctly Verifies from_listing() populates source_id and source_type as Listing | src/planner/argument_binding.rs:530 |
| test959 | `test959_cap_input_file_filename` | TEST959: Tests CapInputFile extracts filename from full path correctly Verifies filename() returns just the basename without directory path | src/planner/argument_binding.rs:539 |
| test960 | `test960_argument_binding_literal_string` | TEST960: Tests ArgumentBinding literal_string creates Literal variant with string value Verifies literal_string() wraps string in JSON Value::String | src/planner/argument_binding.rs:547 |
| test961 | `test961_assemble_empty` | TEST961: assemble empty list produces empty CBOR array | src/orchestrator/cbor_util.rs:279 |
| test962 | `test962_assemble_invalid_item` | TEST962: assemble rejects invalid CBOR item | src/orchestrator/cbor_util.rs:287 |
| test963 | `test963_split_binary_items` | TEST963: split preserves CBOR byte strings (binary data — the common case in bifaci) | src/orchestrator/cbor_util.rs:299 |
| test964 | `test964_split_sequence_bytes` | TEST964: split_cbor_sequence splits concatenated CBOR Bytes values | src/orchestrator/cbor_util.rs:333 |
| test965 | `test965_split_sequence_text` | TEST965: split_cbor_sequence splits concatenated CBOR Text values | src/orchestrator/cbor_util.rs:357 |
| test966 | `test966_split_sequence_mixed` | TEST966: split_cbor_sequence handles mixed types | src/orchestrator/cbor_util.rs:374 |
| test967 | `test967_split_sequence_single` | TEST967: split_cbor_sequence single-item sequence | src/orchestrator/cbor_util.rs:395 |
| test968 | `test968_roundtrip_assemble_split_sequence` | TEST968: roundtrip — assemble then split preserves items | src/orchestrator/cbor_util.rs:408 |
| test969 | `test969_roundtrip_split_assemble_sequence` | TEST969: roundtrip — split then assemble preserves byte-for-byte | src/orchestrator/cbor_util.rs:427 |
| test970 | `test970_split_sequence_empty` | TEST970: split_cbor_sequence rejects empty data | src/orchestrator/cbor_util.rs:441 |
| test971 | `test971_split_sequence_truncated` | TEST971: split_cbor_sequence rejects truncated CBOR | src/orchestrator/cbor_util.rs:448 |
| test972 | `test972_assemble_sequence_invalid_item` | TEST972: assemble_cbor_sequence rejects invalid CBOR item | src/orchestrator/cbor_util.rs:464 |
| test973 | `test973_assemble_sequence_empty` | TEST973: assemble_cbor_sequence with empty items list produces empty bytes | src/orchestrator/cbor_util.rs:476 |
| test974 | `test974_sequence_is_not_array` | TEST974: CBOR sequence is NOT a CBOR array — split_cbor_array rejects a sequence | src/orchestrator/cbor_util.rs:483 |
| test975 | `test975_single_value_sequence` | TEST975: split_cbor_sequence works on data that is also a valid CBOR array (single top-level value) | src/orchestrator/cbor_util.rs:498 |
| test976 | `test976_cap_graph_find_best_path` | TEST976: CapGraph::find_best_path returns highest-specificity path over shortest | src/urn/cap_matrix.rs:1828 |
| test977 | `test977_os_files_excluded_integration` | TEST977 (integration): OS files excluded in resolve_paths | src/input_resolver/resolver.rs:469 |
| test978 | `test978_resolve_json_object` | TEST978 (integration): JSON object via resolve_paths | src/input_resolver/resolver.rs:395 |
| test979 | `test979_resolve_json_array_of_objects` | TEST979 (integration): JSON array of objects via resolve_paths | src/input_resolver/resolver.rs:410 |
| test980 | `test980_resolve_ndjson` | TEST980 (integration): NDJSON via resolve_paths | src/input_resolver/resolver.rs:426 |
| test981 | `test981_resolve_yaml_mapping` | TEST981 (integration): YAML mapping via resolve_paths | src/input_resolver/resolver.rs:441 |
| test982 | `test982_resolve_yaml_sequence` | TEST982 (integration): YAML sequence via resolve_paths | src/input_resolver/resolver.rs:455 |
| test983 | `test983_json_detection_via_adapter_registry` | TEST983 (registry integration): JSON detection via MediaAdapterRegistry | src/input_resolver/adapters/registry.rs:225 |
| test984 | `test984_pdf_thumbnail_to_gguf_describe_fanin` | / TEST050: PDF thumbnail to GGUF describe with model_spec fan-in / Flow: CHAIN + FAN-IN (thumbnail and model_spec both feed into description) / Tests: Multiple inputs converging on single output node | tests/cartridge_scenarios.rs:2823 |
| test985 | `test985_audio_transcribe_to_embed` | / TEST051: Audio transcription (single cap test for whisper) / Flow: single cap / Tests: candlecartridge transcribe cap | tests/cartridge_scenarios.rs:2869 |
| test986 | `test986_pdf_fanout_with_chain` | / TEST052: PDF fan-out with chain: metadata + outline + thumbnail → image embedding / Flow: FAN-OUT (3 outputs) + CHAIN (thumbnail → embedding) / Tests: Single input fanning out with one branch continuing to ML | tests/cartridge_scenarios.rs:2910 |
| test987 | `test987_multi_format_parallel_chains` | / TEST053: Multi-format parallel chains: PDF + MD both get thumbnails and embeddings / Flow: PARALLEL CHAINS (2 independent chains running in parallel) / Tests: Parallel processing of different input formats | tests/cartridge_scenarios.rs:2956 |
| test988 | `test988_deep_chain_with_parallel` | / TEST054: Deep chain with parallel branches from intermediate node / Flow: FAN-OUT from input + FAN-OUT from intermediate + CHAIN / Tests: Complex graph with branching at multiple levels | tests/cartridge_scenarios.rs:3002 |
| test989 | `test989_five_cartridge_chain` | / TEST055: Multi-cartridge stress test with parallel independent paths / Flow: Two independent FAN-OUT paths (model management + PDF processing) / Tests: 3 cartridges working in parallel on independent data | tests/cartridge_scenarios.rs:3052 |
| test990 | `test990_all_text_formats_to_image_embeds` | / TEST056: All text formats → thumbnails → parallel image embeddings (8 edges) / Flow: 4 PARALLEL CHAINS (one for each text format) / Tests: Maximum parallelism with 4 independent chains | tests/cartridge_scenarios.rs:3099 |
| test991 | `test991_detects_duplicate_cap_urns` | TEST991: Tests duplicate detection identifies caps with identical URNs Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn | src/planner/plan_builder.rs:791 |
| test992 | `test992_different_ops_same_types_not_duplicates` | TEST992: Tests caps with different operations but same input/output types are not duplicates Verifies that only the complete URN (including op) is used for duplicate detection | src/planner/plan_builder.rs:809 |
| test993 | `test993_same_op_different_input_types_not_duplicates` | TEST993: Tests caps with same operation but different input types are not duplicates Verifies that input type differences distinguish caps with the same operation name | src/planner/plan_builder.rs:824 |
| test994 | `test994_input_arg_first_cap_auto_resolved_from_input` | TEST994: Tests first cap's input argument is automatically resolved from input file Verifies that determine_resolution_with_io_check() returns FromInputFile for the first cap in a chain | src/planner/plan_builder.rs:864 |
| test995 | `test995_input_arg_subsequent_cap_auto_resolved_from_previous` | TEST995: Tests subsequent caps' input arguments are automatically resolved from previous output Verifies that determine_resolution_with_io_check() returns FromPreviousOutput for caps after the first | src/planner/plan_builder.rs:875 |
| test996 | `test996_output_arg_auto_resolved` | TEST996: Tests output arguments are automatically resolved from previous cap's output Verifies that arguments matching the output spec are always resolved as FromPreviousOutput | src/planner/plan_builder.rs:890 |
| test997 | `test997_file_path_type_fallback_first_cap` | TEST997: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap Verifies that generic file-path arguments are bound to input file in the first cap | src/planner/plan_builder.rs:901 |
| test998 | `test998_file_path_type_fallback_subsequent_cap` | TEST998: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps Verifies that generic file-path arguments are bound to previous cap's output after the first cap | src/planner/plan_builder.rs:912 |
| test999 | `test999_file_path_array_fallback` | TEST999: Tests MEDIA_FILE_PATH_ARRAY argument type resolution for first and subsequent caps Verifies that file-path array arguments follow the same resolution pattern as single file paths | src/planner/plan_builder.rs:923 |
| test1000 | `test1000_single_existing_file` | TEST1000: Single existing file | src/input_resolver/path_resolver.rs:256 |
| test1001 | `test1001_nonexistent_file` | TEST1001: Single non-existent file | src/input_resolver/path_resolver.rs:268 |
| test1002 | `test1002_empty_directory` | TEST1002: Empty directory | src/input_resolver/path_resolver.rs:275 |
| test1003 | `test1003_directory_with_files` | TEST1003: Directory with files | src/input_resolver/path_resolver.rs:284 |
| test1004 | `test1004_directory_with_subdirs` | TEST1004: Directory with subdirs (recursive) | src/input_resolver/path_resolver.rs:296 |
| test1005 | `test1005_glob_matching_files` | TEST1005: Glob matching files | src/input_resolver/path_resolver.rs:308 |
| test1006 | `test1006_glob_matching_nothing` | TEST1006: Glob matching nothing | src/input_resolver/path_resolver.rs:321 |
| test1007 | `test1007_recursive_glob` | TEST1007: Recursive glob | src/input_resolver/path_resolver.rs:332 |
| test1008 | `test1008_mixed_file_dir` | TEST1008: Mixed file + dir | src/input_resolver/path_resolver.rs:345 |
| test1009 | `test1009_non_io_arg_with_default_has_default` | TEST1009: Tests required non-IO arguments with default values are marked as HasDefault Verifies that arguments like integers with defaults don't require user input | src/planner/plan_builder.rs:937 |
| test1010 | `test1010_duplicate_paths` | TEST1010: Duplicate paths are deduplicated | src/input_resolver/path_resolver.rs:363 |
| test1011 | `test1011_invalid_glob` | TEST1011: Invalid glob syntax | src/input_resolver/path_resolver.rs:379 |
| test1012 | `test1012_non_io_arg_without_default_requires_user_input` | TEST1012: Tests required non-IO arguments without defaults require user input Verifies that arguments like strings without defaults are marked as RequiresUserInput | src/planner/plan_builder.rs:949 |
| test1013 | `test1013_empty_input` | TEST1013: Empty input array | src/input_resolver/path_resolver.rs:386 |
| test1014 | `test1014_symlink_to_file` | TEST1014: Symlink to file | src/input_resolver/path_resolver.rs:394 |
| test1015 | `test1015_optional_non_io_arg_without_default_requires_user_input` | TEST1015: Tests optional non-IO arguments without defaults still require user input Verifies that optional arguments without defaults must be explicitly provided or skipped | src/planner/plan_builder.rs:972 |
| test1016 | `test1016_path_with_spaces` | TEST1016: Path with spaces | src/input_resolver/path_resolver.rs:409 |
| test1017 | `test1017_path_with_unicode` | TEST1017: Path with unicode | src/input_resolver/path_resolver.rs:420 |
| test1018 | `test1018_relative_path` | TEST1018: Relative path | src/input_resolver/path_resolver.rs:431 |
| test1019 | `test1019_validation_to_json_none` | TEST1019: Tests validation_to_json() returns None for None input Verifies that missing validation metadata is converted to JSON None | src/planner/plan_builder.rs:983 |
| test1020 | `test1020_ds_store_excluded` | TEST1020: macOS .DS_Store is excluded | src/input_resolver/os_filter.rs:162 |
| test1021 | `test1021_thumbs_db_excluded` | TEST1021: Windows Thumbs.db is excluded | src/input_resolver/os_filter.rs:169 |
| test1022 | `test1022_resource_fork_excluded` | TEST1022: macOS resource fork files are excluded | src/input_resolver/os_filter.rs:176 |
| test1023 | `test1023_office_lock_excluded` | TEST1023: Office lock files are excluded | src/input_resolver/os_filter.rs:183 |
| test1024 | `test1024_git_dir_excluded` | TEST1024: .git directory is excluded | src/input_resolver/os_filter.rs:190 |
| test1025 | `test1025_macosx_dir_excluded` | TEST1025: __MACOSX archive artifact is excluded | src/input_resolver/os_filter.rs:197 |
| test1026 | `test1026_temp_files_excluded` | TEST1026: Temp files are excluded | src/input_resolver/os_filter.rs:204 |
| test1027 | `test1027_localized_excluded` | TEST1027: .localized is excluded | src/input_resolver/os_filter.rs:213 |
| test1028 | `test1028_desktop_ini_excluded` | TEST1028: desktop.ini is excluded | src/input_resolver/os_filter.rs:219 |
| test1029 | `test1029_normal_files_not_excluded` | TEST1029: Normal files are NOT excluded | src/input_resolver/os_filter.rs:225 |
| test1030 | `test1030_json_empty_object` | TEST1030: Empty object | src/input_resolver/adapters/data.rs:425 |
| test1031 | `test1031_json_simple_object` | TEST1031: Simple object | src/input_resolver/adapters/data.rs:437 |
| test1032 | `test1032_audio_transcription` | TEST023: Transcribe audio with Whisper via candlecartridge | tests/cartridge_scenarios.rs:1379 |
| test1033 | `test1033_json_empty_array` | TEST1033: Empty array | src/input_resolver/adapters/data.rs:448 |
| test1034 | `test1034_pdf_complete_analysis` | TEST024: All 4 pdfcartridge ops on a single PDF — full document analysis pipeline | tests/cartridge_scenarios.rs:1427 |
| test1035 | `test1035_model_full_inspection` | TEST025: All 4 modelcartridge inspection ops on a single model spec | tests/cartridge_scenarios.rs:1486 |
| test1036 | `test1036_json_array_of_objects` | TEST1036: Array of objects | src/input_resolver/adapters/data.rs:460 |
| test1037 | `test1037_two_format_full_analysis` | TEST026: 7-cap parallel analysis — all pdf ops + all md ops on two documents | tests/cartridge_scenarios.rs:1554 |
| test1038 | `test1038_model_plus_pdf_combined` | TEST027: 5-cap cross-domain pipeline — model inspection + PDF document analysis | tests/cartridge_scenarios.rs:1622 |
| test1039 | `test1039_json_number_primitive` | TEST1039: Number primitive | src/input_resolver/adapters/data.rs:472 |
| test1040 | `test1040_three_cartridge_pipeline` | TEST028: 6-cap three-cartridge pipeline — model + PDF + markdown analysis | tests/cartridge_scenarios.rs:1683 |
| test1041 | `test1041_txt_document_intelligence` | TEST029: Plain text fan-out produces metadata, outline, and thumbnail from txt input | tests/cartridge_scenarios.rs:1806 |
| test1042 | `test1042_rst_document_intelligence` | TEST030: RST document fan-out produces metadata, outline (with headers), and thumbnail | tests/cartridge_scenarios.rs:1848 |
| test1043 | `test1043_log_document_intelligence` | TEST031: Log file fan-out produces metadata, outline, and thumbnail from log input | tests/cartridge_scenarios.rs:1894 |
| test1044 | `test1044_all_text_formats_intelligence` | TEST032: 12-cap DAG processing all four text formats simultaneously | tests/cartridge_scenarios.rs:1935 |
| test1045 | `test1045_ndjson_objects` | TEST1045: Objects only | src/input_resolver/adapters/data.rs:486 |
| test1046 | `test1046_model_list_models` | TEST033: List all locally cached models via modelcartridge | tests/cartridge_scenarios.rs:2000 |
| test1047 | `test1047_ndjson_primitives` | TEST1047: Primitives only | src/input_resolver/adapters/data.rs:498 |
| test1048 | `test1048_gguf_embeddings_dimensions` | TEST034: Query GGUF embedding model dimensions via ggufcartridge | tests/cartridge_scenarios.rs:2042 |
| test1049 | `test1049_gguf_llm_model_info` | TEST035: Query GGUF model metadata via llm_model_info cap | tests/cartridge_scenarios.rs:2089 |
| test1050 | `test1050_gguf_llm_vocab` | TEST036: Extract vocabulary tokens from a GGUF model via llm_vocab cap | tests/cartridge_scenarios.rs:2138 |
| test1051 | `test1051_gguf_model_info_plus_vocab` | TEST037: Fan-out from one LLM request to both model_info and vocab outputs | tests/cartridge_scenarios.rs:2187 |
| test1052 | `test1052_gguf_llm_inference` | TEST038: Generate text with a small GGUF LLM via llm_inference cap | tests/cartridge_scenarios.rs:2239 |
| test1053 | `test1053_gguf_llm_inference_constrained` | TEST039: Generate JSON-constrained output with GGUF LLM via llm_inference_constrained cap | tests/cartridge_scenarios.rs:2286 |
| test1054 | `test1054_gguf_generate_embeddings` | TEST040: Generate GGUF text embeddings with fan-in of text and model-spec inputs | tests/cartridge_scenarios.rs:2336 |
| test1055 | `test1055_csv_multi_column` | TEST1055: Multi-column with header | src/input_resolver/adapters/data.rs:512 |
| test1056 | `test1056_csv_single_column` | TEST1056: Single column | src/input_resolver/adapters/data.rs:524 |
| test1057 | `test1057_gguf_describe_image` | TEST041: Describe image with GGUF vision model via fan-in of image and model-spec | tests/cartridge_scenarios.rs:2391 |
| test1058 | `test1058_pdf_thumbnail_to_gguf_vision` | TEST042: Cross-cartridge chain: PDF thumbnail piped to GGUF vision analysis | tests/cartridge_scenarios.rs:2440 |
| test1059 | `test1059_gguf_all_llm_ops` | TEST043: Fan-out from one LLM request to all 4 ggufcartridge LLM operations | tests/cartridge_scenarios.rs:2496 |
| test1060 | `test1060_mlx_generate_text` | / TEST044: MLX text generation / Flow: single cap / Tests: mlxcartridge generate_text cap | tests/cartridge_scenarios.rs:2568 |
| test1061 | `test1061_mlx_describe_image` | / TEST045: MLX describe image / Flow: single cap / Tests: mlxcartridge describe_image cap (vision) | tests/cartridge_scenarios.rs:2609 |
| test1062 | `test1062_mlx_generate_embeddings` | / TEST046: MLX generate embeddings / Flow: single cap / Tests: mlxcartridge generate_embeddings cap | tests/cartridge_scenarios.rs:2650 |
| test1063 | `test1063_mlx_embeddings_dimensions` | / TEST047: MLX embeddings dimensions / Flow: single cap / Tests: mlxcartridge embeddings_dimensions cap | tests/cartridge_scenarios.rs:2691 |
| test1064 | `test1064_model_download` | / TEST048: Model download / Flow: single cap / Tests: modelcartridge download-model cap | tests/cartridge_scenarios.rs:2737 |
| test1065 | `test1065_yaml_mapping` | TEST1065: Simple mapping | src/input_resolver/adapters/data.rs:538 |
| test1066 | `test1066_pdf_to_thumbnail_to_describe_to_embed` | / TEST049: 3-step chain: PDF → thumbnail → candle describe → text embeddings / Flow: CHAIN (3 steps across 2 cartridges + ML inference) / Tests: Sequential data transformation across multiple cartridges | tests/cartridge_scenarios.rs:2776 |
| test1067 | `test1067_yaml_sequence_of_scalars` | TEST1067: Sequence of scalars | src/input_resolver/adapters/data.rs:550 |
| test1068 | `test1068_yaml_sequence_of_mappings` | TEST1068: Sequence of mappings | src/input_resolver/adapters/data.rs:562 |
| test1069 | `test1069_pdf_document_intelligence` | TEST014: PDF fan-out produces metadata, outline, and thumbnail from a single PDF input | tests/cartridge_scenarios.rs:906 |
| test1070 | `test1070_pdf_thumbnail_to_image_embedding` | TEST015: Cross-cartridge chain: PDF thumbnail piped to CLIP image embedding | tests/cartridge_scenarios.rs:959 |
| test1071 | `test1071_text_document_intelligence` | TEST017: Markdown fan-out produces metadata, outline, and thumbnail | tests/cartridge_scenarios.rs:1071 |
| test1072 | `test1072_multi_format_document_processing` | TEST018: Parallel processing of PDF and markdown through independent fan-outs | tests/cartridge_scenarios.rs:1122 |
| test1080 | `test1080_pdf_extension` | TEST1080: PDF extension mapping | src/input_resolver/adapters/documents.rs:228 |
| test1081 | `test1081_png_extension` | TEST1081: PNG extension mapping | src/input_resolver/adapters/images.rs:338 |
| test1082 | `test1082_mp3_extension` | TEST1082: MP3 extension mapping | src/input_resolver/adapters/audio.rs:243 |
| test1083 | `test1083_mp4_extension` | TEST1083: MP4 extension mapping | src/input_resolver/adapters/video.rs:266 |
| test1084 | `test1084_rust_extension` | TEST1084: Rust code extension mapping | src/input_resolver/adapters/code.rs:734 |
| test1085 | `test1085_python_extension` | TEST1085: Python code extension mapping | src/input_resolver/adapters/code.rs:745 |
| test1087 | `test1087_toml_always_record` | TEST1087: TOML always record | src/input_resolver/adapters/data.rs:574 |
| test1089 | `test1089_unknown_extension` | TEST1089: Unknown extension fallback | src/input_resolver/adapters/other.rs:499 |
| test1090 | `test1090_single_file_scalar` | TEST1090: 1 file scalar content | src/input_resolver/resolver.rs:290 |
| test1091 | `test1091_single_file_list_content` | TEST1091: 1 file list content (CSV) | src/input_resolver/resolver.rs:302 |
| test1092 | `test1092_two_files` | TEST1092: 2 files | src/input_resolver/resolver.rs:321 |
| test1093 | `test1093_dir_single_file` | TEST1093: 1 dir with 1 file | src/input_resolver/resolver.rs:338 |
| test1094 | `test1094_dir_multiple_files` | TEST1094: 1 dir with 3 files | src/input_resolver/resolver.rs:350 |
| test1095 | `test1095_glob_with_detection` | TEST1095/1096 (integration): Glob with detection | src/input_resolver/resolver.rs:482 |
| test1098 | `test1098_common_media` | TEST1098: Common media (all same type) | src/input_resolver/resolver.rs:367 |
| test1099 | `test1099_heterogeneous` | TEST1099: Heterogeneous (mixed types) | src/input_resolver/resolver.rs:380 |
| test1100 | `test1100_cap_urn_normalizes_media_urn_tag_order` | TEST1100: Tests that CapUrn normalizes media URN tags to canonical order This is the root cause fix for caps not matching when cartridges report URNs with different tag ordering than the registry (e.g., "record;textable" vs "textable;record") | src/planner/plan_builder.rs:1141 |
| test1103 | `test1103_is_dispatchable_uses_correct_directionality` | TEST1103: Tests that is_dispatchable has correct directionality The available cap (provider) must be dispatchable for the requested cap (request). This tests the directionality: provider.is_dispatchable(&request) NOTE: This now tests CapUrn::is_dispatchable directly, not via MachinePlanBuilder | src/planner/plan_builder.rs:1168 |
| test1104 | `test1104_is_dispatchable_rejects_non_dispatchable` | TEST1104: Tests that is_dispatchable rejects when provider cannot dispatch request | src/planner/plan_builder.rs:1193 |
| test1105 | `test1105_two_steps_same_cap_urn_different_slot_values` | TEST1105: Two steps with the same cap_urn get distinct slot values via different node_ids. This is the core disambiguation scenario that step-index keying was designed to solve. | src/planner/argument_binding.rs:840 |
| test1106 | `test1106_slot_falls_through_to_cap_settings_shared` | TEST1106: Slot resolution falls through to cap_settings when no slot_value exists. cap_settings are keyed by cap_urn (shared across steps), so both steps get the same value. | src/planner/argument_binding.rs:881 |
| test1107 | `test1107_slot_value_overrides_cap_settings_per_step` | TEST1107: step_0 has a slot_value override, step_1 falls through to cap_settings. Proves per-step override works while shared settings remain as fallback. | src/planner/argument_binding.rs:918 |
| test1108 | `test1108_resolve_all_passes_node_id` | TEST1108: ResolveAll with node_id threads correctly through to each binding. | src/planner/argument_binding.rs:961 |
| test1109 | `test1109_slot_key_uses_node_id_not_cap_urn` | TEST1109: Slot key uses node_id, NOT cap_urn — a slot_value keyed by cap_urn must not match. | src/planner/argument_binding.rs:998 |
| test1110 | `test1110_strand_round_trips_through_serde_without_losing_step_types` |  | src/planner/live_cap_graph.rs:1709 |
| test1111 | `test1111_foreach_for_user_provided_list_source` | TEST1111: ForEach works for user-provided list sources not in the graph. This is the original bug — media:list;textable;txt is a user import source, not a cap output. Previously, no ForEach edge existed for it because insert_cardinality_transitions() only pre-computed edges for cap outputs. With dynamic synthesis, ForEach is available for ANY list source. | src/planner/live_cap_graph.rs:1771 |
| test1112 | `test1112_no_collect_in_path_finding` | TEST1112: Collect is not synthesized during path finding. Reaching a list target type requires the cap itself to output a list type. | src/planner/live_cap_graph.rs:1821 |
| test1113 | `test1113_multi_cap_path_no_collect` | TEST1113: Multi-cap path without Collect — Collect is not synthesized | src/planner/live_cap_graph.rs:1843 |
| test1114 | `test1114_graph_stores_only_cap_edges` | TEST1114: Graph stores only Cap edges after sync | src/planner/live_cap_graph.rs:1871 |
| test1115 | `test1115_dynamic_foreach_with_is_sequence` | TEST1115: ForEach is synthesized when is_sequence=true AND caps can consume items | src/planner/live_cap_graph.rs:1895 |
| test1116 | `test1116_collect_never_synthesized` | TEST1116: Collect is never synthesized during path finding | src/planner/live_cap_graph.rs:1916 |
| test1117 | `test1117_no_foreach_when_not_sequence` | TEST1117: ForEach is NOT synthesized when is_sequence=false | src/planner/live_cap_graph.rs:1933 |
| test1118 | `test1118_no_foreach_without_cap_consumers` | TEST1118: ForEach not synthesized without cap consumers even with is_sequence=true | src/planner/live_cap_graph.rs:1949 |
| test1119 | `test1119_strand_knit_with_registry_returns_single_strand_machine` | TEST1119: Strand::knit returns a single-strand Machine via the new resolver. Smoke test the registry-threaded API end-to-end. | src/planner/live_cap_graph.rs:1963 |
| test1120 | `test1120_strand_knit_unknown_cap_fails_hard` | TEST1120: Strand::knit fails hard when the cap is not in the registry — the planner produces strands referencing caps that must be present in the cap registry's cache for resolution to succeed. | src/planner/live_cap_graph.rs:2014 |
| test1121 | `test1121_cbor_array_file_paths_in_cbor_mode` | TEST1121: CBOR Array of file-paths in CBOR mode (validates new Array support) | src/bifaci/cartridge_runtime.rs:5941 |
| test1122 | `test1122_full_path_engine_req_to_cartridge_response` | TEST1122: Full path: engine REQ → runtime → cartridge → response back through relay | src/bifaci/integration_tests.rs:159 |
| test1123 | `test1123_cartridge_error_flows_to_engine` | TEST1123: Cartridge ERR frame flows back to engine through relay | src/bifaci/integration_tests.rs:268 |
| test1124 | `test1124_cbor_rejects_stream_end_without_chunk_count` | TEST1124: CBOR decode REJECTS STREAM_END frame missing chunk_count field | src/bifaci/frame.rs:2086 |
| test1125 | `test1125_map_progress_basic_mapping` | TEST1125: map_progress clamps child to [0.0, 1.0] and maps to [base, base+weight] | src/orchestrator/executor.rs:1446 |
| test1126 | `test1126_map_progress_deterministic` | TEST1126: map_progress is deterministic — same inputs always produce same output | src/orchestrator/executor.rs:1464 |
| test1127 | `test1127_cap_documentation_round_trip_with_markdown_body` | TEST1127: Documentation field round-trips through JSON serialize/deserialize.  The documentation field carries an arbitrary markdown body authored in the source TOML via the triple-quoted literal string syntax. The round-trip must preserve every character — including newlines, backticks, double quotes, and Unicode — because consumers (info panels, capdag.com, etc.) render it directly. JSON.stringify on the capgraph side and the Rust serializer on this side must agree on escaping; this test fails hard if they don't. | src/cap/definition.rs:1313 |
| test1128 | `test1128_cap_documentation_omitted_when_none` | TEST1128: When documentation is None, the serializer must skip the field entirely. This matches the behaviour of the JS toJSON, the ObjC toDictionary, and the schema's "if present" semantics — there is no null sentinel, only absence. A bug here would silently start emitting `"documentation":null` and break consumers that distinguish between absent and explicit null. | src/cap/definition.rs:1348 |
| test1129 | `test1129_cap_documentation_parses_from_capgraph_json` | TEST1129: A JSON document produced by capgraph (the canonical source) with a `documentation` field must deserialize into a Cap with the body intact. Models the actual on-disk shape — not a synthetic round-trip — to catch a mismatch between the JSON schema and the Rust struct field naming. | src/cap/definition.rs:1371 |
| test1130 | `test1130_cap_documentation_set_and_clear_lifecycle` | TEST1130: documentation set/clear lifecycle parallels cap_description. Catches a regression where the setter or clearer is wired to the wrong field — for example, set_documentation accidentally writing to cap_description. | src/cap/definition.rs:1393 |
| test1131 | `test1131_media_documentation_propagates_through_resolve` | TEST1131: Documentation propagates from MediaSpecDef through resolve_media_urn into ResolvedMediaSpec.  This is the resolution path used by every consumer that asks the registry for a media spec — info panels, the cap navigator, the UI — so a regression here makes the new field invisible everywhere. | src/media/spec.rs:1212 |
| test1132 | `test1132_media_spec_def_documentation_round_trip` | TEST1132: MediaSpecDef serializes documentation only when present and round-trips losslessly. Mirrors TEST1127/1128 for the cap side. | src/media/spec.rs:1245 |
| test1133 | `test1133_media_spec_def_documentation_lifecycle` | TEST1133: MediaSpecDef set/clear lifecycle for documentation. Catches a regression where the setter or clearer accidentally writes to or reads from `description` (the short field) instead of `documentation` (the long markdown body). | src/media/spec.rs:1289 |

---

*Generated from CapDag (Rust) source tree*
*Total numbered tests: 1008*
