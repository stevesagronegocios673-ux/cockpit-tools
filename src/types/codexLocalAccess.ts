export type CodexLocalAccessRoutingStrategy =
  | 'auto'
  | 'quota_high_first'
  | 'quota_low_first'
  | 'plan_high_first'
  | 'plan_low_first'
  | 'expiry_soon_first';

export interface CodexLocalAccessApiKey {
  id: string;
  name: string;
  key: string;
  enabled: boolean;
  monthlyTokenLimit: number | null;
  allowedAccountIds: string[] | null;
  createdAt: number;
  updatedAt: number;
  lastUsedAt: number | null;
}

export interface CodexLocalAccessCollection {
  id: string;
  name: string;
  enabled: boolean;
  port: number;
  apiKeys: CodexLocalAccessApiKey[];
  defaultApiKeyId: string | null;
  routingStrategy: CodexLocalAccessRoutingStrategy;
  restrictFreeAccounts: boolean;
  accountIds: string[];
  createdAt: number;
  updatedAt: number;
}

export interface CodexLocalAccessApiKeyInput {
  name: string;
  monthlyTokenLimit: number | null;
  upstreamScope: 'all' | 'selected';
  allowedAccountIds: string[];
}

export interface CodexLocalAccessServiceSummary {
  id: string;
  name: string;
  enabled: boolean;
  running: boolean;
  port: number;
  apiPortUrl: string;
  baseUrl: string;
  memberCount: number;
  apiKeyCount: number;
  defaultApiKeyName: string | null;
  healthStatus: string;
  alertCount: number;
  lastError: string | null;
  updatedAt: number;
}

export interface CodexLocalAccessUsageStats {
  requestCount: number;
  successCount: number;
  failureCount: number;
  totalLatencyMs: number;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  cachedTokens: number;
  reasoningTokens: number;
}

export interface CodexLocalAccessAccountStats {
  accountId: string;
  email: string;
  sourceType?: string;
  providerName?: string | null;
  baseUrlHost?: string | null;
  usage: CodexLocalAccessUsageStats;
  updatedAt: number;
}

export interface CodexLocalAccessApiKeyStats {
  apiKeyId: string;
  apiKeyName: string;
  usage: CodexLocalAccessUsageStats;
  models: CodexLocalAccessModelStats[];
  updatedAt: number;
}

export interface CodexLocalAccessModelStats {
  modelId: string;
  usage: CodexLocalAccessUsageStats;
  updatedAt: number;
}

export interface CodexLocalAccessUsageEvent {
  timestamp: number;
  modelId: string;
  accountId: string;
  email: string;
  sourceType?: string;
  providerName?: string | null;
  baseUrlHost?: string | null;
  apiKeyId: string;
  apiKeyName: string;
  success: boolean;
  latencyMs: number;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  cachedTokens: number;
  reasoningTokens: number;
}

export interface CodexLocalAccessDiagnosticEvent {
  timestamp: number;
  severity: string;
  category: string;
  apiKeyId: string | null;
  accountId: string | null;
  modelId: string | null;
  statusCode: number | null;
  baseUrlHost: string | null;
  message: string;
  retryable: boolean;
}

export interface CodexLocalAccessUpstreamHealth {
  accountId: string;
  email: string;
  sourceType: string;
  providerName: string | null;
  baseUrlHost: string | null;
  selected: boolean;
  eligible: boolean;
  authorizedApiKeyCount: number;
  healthy: boolean;
  coolingDown: boolean;
  cooldownUntil: number | null;
  lastSuccessAt: number | null;
  lastFailureAt: number | null;
  consecutiveFailures: number;
  averageLatencyMs: number;
  lastFailureReason: string | null;
}

export interface CodexLocalAccessApiKeyHealth {
  apiKeyId: string;
  apiKeyName: string;
  enabled: boolean;
  isDefault: boolean;
  authorizedAccountCount: number;
  availableAccountCount: number;
  monthlyTokenLimit: number | null;
  monthlyTokensUsed: number;
  monthlyUsageRatio: number | null;
  lastFailureAt: number | null;
  lastFailureReason: string | null;
  warningCount: number;
}

export interface CodexLocalAccessAlert {
  id: string;
  severity: string;
  category: string;
  message: string;
  accountId: string | null;
  apiKeyId: string | null;
  createdAt: number;
}

export interface CodexLocalAccessDiagnostics {
  status: string;
  alerts: CodexLocalAccessAlert[];
  upstreams: CodexLocalAccessUpstreamHealth[];
  apiKeys: CodexLocalAccessApiKeyHealth[];
  events: CodexLocalAccessDiagnosticEvent[];
}

export interface CodexLocalAccessStatsWindow {
  since: number;
  updatedAt: number;
  totals: CodexLocalAccessUsageStats;
  accounts: CodexLocalAccessAccountStats[];
  apiKeys: CodexLocalAccessApiKeyStats[];
  models: CodexLocalAccessModelStats[];
}

export interface CodexLocalAccessStats {
  since: number;
  updatedAt: number;
  totals: CodexLocalAccessUsageStats;
  accounts: CodexLocalAccessAccountStats[];
  apiKeys: CodexLocalAccessApiKeyStats[];
  daily: CodexLocalAccessStatsWindow;
  weekly: CodexLocalAccessStatsWindow;
  monthly: CodexLocalAccessStatsWindow;
  events: CodexLocalAccessUsageEvent[];
  upstreamHealth?: CodexLocalAccessUpstreamHealth[];
  diagnosticEvents?: CodexLocalAccessDiagnosticEvent[];
}

export interface CodexLocalAccessUpstreamSource {
  accountId: string;
  email: string;
  sourceType: string;
  providerName: string | null;
  baseUrlHost: string | null;
  selected: boolean;
  eligible: boolean;
  disabledReason: string | null;
}

export interface CodexLocalAccessState {
  services: CodexLocalAccessServiceSummary[];
  selectedServiceId: string | null;
  collection: CodexLocalAccessCollection | null;
  running: boolean;
  apiPortUrl: string | null;
  baseUrl: string | null;
  modelIds: string[];
  lastError: string | null;
  memberCount: number;
  upstreamSources: CodexLocalAccessUpstreamSource[];
  stats: CodexLocalAccessStats;
  diagnostics: CodexLocalAccessDiagnostics;
}

export interface CodexLocalAccessPortCleanupResult {
  killedCount: number;
  state: CodexLocalAccessState;
}
