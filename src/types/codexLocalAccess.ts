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
  collection: CodexLocalAccessCollection | null;
  running: boolean;
  apiPortUrl: string | null;
  baseUrl: string | null;
  modelIds: string[];
  lastError: string | null;
  memberCount: number;
  upstreamSources: CodexLocalAccessUpstreamSource[];
  stats: CodexLocalAccessStats;
}

export interface CodexLocalAccessPortCleanupResult {
  killedCount: number;
  state: CodexLocalAccessState;
}
