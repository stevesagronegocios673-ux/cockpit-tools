use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodexLocalAccessRoutingStrategy {
    Auto,
    QuotaHighFirst,
    QuotaLowFirst,
    PlanHighFirst,
    PlanLowFirst,
    ExpirySoonFirst,
}

impl Default for CodexLocalAccessRoutingStrategy {
    fn default() -> Self {
        Self::Auto
    }
}

fn default_restrict_free_accounts() -> bool {
    true
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessApiKey {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub key: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub monthly_token_limit: Option<u64>,
    #[serde(default)]
    pub allowed_account_ids: Option<Vec<String>>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub last_used_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessCollection {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub api_keys: Vec<CodexLocalAccessApiKey>,
    #[serde(default)]
    pub default_api_key_id: Option<String>,
    #[serde(default, rename = "apiKey", skip_serializing)]
    pub legacy_api_key: Option<String>,
    #[serde(default)]
    pub routing_strategy: CodexLocalAccessRoutingStrategy,
    #[serde(default = "default_restrict_free_accounts")]
    pub restrict_free_accounts: bool,
    #[serde(default)]
    pub account_ids: Vec<String>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessUsageStats {
    #[serde(default)]
    pub request_count: u64,
    #[serde(default)]
    pub success_count: u64,
    #[serde(default)]
    pub failure_count: u64,
    #[serde(default)]
    pub total_latency_ms: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessAccountStats {
    pub account_id: String,
    pub email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url_host: Option<String>,
    #[serde(default)]
    pub usage: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessConfig {
    #[serde(default)]
    pub services: Vec<CodexLocalAccessCollection>,
    #[serde(default)]
    pub selected_service_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessModelStats {
    #[serde(default)]
    pub model_id: String,
    #[serde(default)]
    pub usage: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessApiKeyStats {
    pub api_key_id: String,
    pub api_key_name: String,
    #[serde(default)]
    pub usage: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub models: Vec<CodexLocalAccessModelStats>,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessStatsWindow {
    #[serde(default)]
    pub since: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub totals: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub accounts: Vec<CodexLocalAccessAccountStats>,
    #[serde(default)]
    pub api_keys: Vec<CodexLocalAccessApiKeyStats>,
    #[serde(default)]
    pub models: Vec<CodexLocalAccessModelStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessUsageEvent {
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub model_id: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url_host: Option<String>,
    #[serde(default)]
    pub api_key_id: String,
    #[serde(default)]
    pub api_key_name: String,
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub latency_ms: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessDiagnosticEvent {
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub severity: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub api_key_id: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub status_code: Option<u16>,
    #[serde(default)]
    pub base_url_host: Option<String>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessUpstreamHealth {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub source_type: String,
    #[serde(default)]
    pub provider_name: Option<String>,
    #[serde(default)]
    pub base_url_host: Option<String>,
    #[serde(default)]
    pub selected: bool,
    #[serde(default)]
    pub eligible: bool,
    #[serde(default)]
    pub authorized_api_key_count: usize,
    #[serde(default)]
    pub healthy: bool,
    #[serde(default)]
    pub cooling_down: bool,
    #[serde(default)]
    pub cooldown_until: Option<i64>,
    #[serde(default)]
    pub last_success_at: Option<i64>,
    #[serde(default)]
    pub last_failure_at: Option<i64>,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub average_latency_ms: u64,
    #[serde(default)]
    pub last_failure_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessApiKeyHealth {
    #[serde(default)]
    pub api_key_id: String,
    #[serde(default)]
    pub api_key_name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default)]
    pub authorized_account_count: usize,
    #[serde(default)]
    pub available_account_count: usize,
    #[serde(default)]
    pub monthly_token_limit: Option<u64>,
    #[serde(default)]
    pub monthly_tokens_used: u64,
    #[serde(default)]
    pub monthly_usage_ratio: Option<f64>,
    #[serde(default)]
    pub last_failure_at: Option<i64>,
    #[serde(default)]
    pub last_failure_reason: Option<String>,
    #[serde(default)]
    pub warning_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessAlert {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub severity: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub api_key_id: Option<String>,
    #[serde(default)]
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessDiagnostics {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub alerts: Vec<CodexLocalAccessAlert>,
    #[serde(default)]
    pub upstreams: Vec<CodexLocalAccessUpstreamHealth>,
    #[serde(default)]
    pub api_keys: Vec<CodexLocalAccessApiKeyHealth>,
    #[serde(default)]
    pub events: Vec<CodexLocalAccessDiagnosticEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessStats {
    #[serde(default)]
    pub since: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub totals: CodexLocalAccessUsageStats,
    #[serde(default)]
    pub accounts: Vec<CodexLocalAccessAccountStats>,
    #[serde(default)]
    pub api_keys: Vec<CodexLocalAccessApiKeyStats>,
    #[serde(default)]
    pub daily: CodexLocalAccessStatsWindow,
    #[serde(default)]
    pub weekly: CodexLocalAccessStatsWindow,
    #[serde(default)]
    pub monthly: CodexLocalAccessStatsWindow,
    #[serde(default)]
    pub events: Vec<CodexLocalAccessUsageEvent>,
    #[serde(default)]
    pub upstream_health: Vec<CodexLocalAccessUpstreamHealth>,
    #[serde(default)]
    pub diagnostic_events: Vec<CodexLocalAccessDiagnosticEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessStatsStore {
    #[serde(default)]
    pub stats_by_service_id: HashMap<String, CodexLocalAccessStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessUpstreamSource {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub source_type: String,
    #[serde(default)]
    pub provider_name: Option<String>,
    #[serde(default)]
    pub base_url_host: Option<String>,
    #[serde(default)]
    pub selected: bool,
    #[serde(default)]
    pub eligible: bool,
    #[serde(default)]
    pub disabled_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessServiceSummary {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub running: bool,
    pub port: u16,
    pub api_port_url: String,
    pub base_url: String,
    pub member_count: usize,
    pub api_key_count: usize,
    pub default_api_key_name: Option<String>,
    pub health_status: String,
    pub alert_count: usize,
    pub last_error: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessState {
    pub services: Vec<CodexLocalAccessServiceSummary>,
    pub selected_service_id: Option<String>,
    pub collection: Option<CodexLocalAccessCollection>,
    pub running: bool,
    pub api_port_url: Option<String>,
    pub base_url: Option<String>,
    pub model_ids: Vec<String>,
    pub last_error: Option<String>,
    pub member_count: usize,
    pub upstream_sources: Vec<CodexLocalAccessUpstreamSource>,
    pub stats: CodexLocalAccessStats,
    pub diagnostics: CodexLocalAccessDiagnostics,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLocalAccessPortCleanupResult {
    pub killed_count: u32,
    pub state: CodexLocalAccessState,
}
