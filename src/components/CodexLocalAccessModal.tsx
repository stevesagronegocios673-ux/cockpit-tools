import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  Check,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  Copy,
  DollarSign,
  Eye,
  EyeOff,
  ExternalLink,
  FolderPlus,
  Gauge,
  KeyRound,
  Power,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  Trash2,
  Wrench,
  X,
} from 'lucide-react';
import { confirm as confirmDialog } from '@tauri-apps/plugin-dialog';
import { openUrl } from '@tauri-apps/plugin-opener';
import { useTranslation } from 'react-i18next';
import type { CodexAccount } from '../types/codex';
import type { CodexAccountGroup } from '../services/codexAccountGroupService';
import { fetchOpenAiPricingMarkdown } from '../services/codexLocalAccessService';
import type {
  CodexLocalAccessApiKey,
  CodexLocalAccessRoutingStrategy,
  CodexLocalAccessState,
  CodexLocalAccessModelStats,
  CodexLocalAccessStatsWindow,
  CodexLocalAccessUsageEvent,
  CodexLocalAccessUsageStats,
} from '../types/codexLocalAccess';
import {
  getCodexPlanFilterKey,
  isCodexApiKeyAccount,
  isCodexExplicitFreePlanType,
} from '../types/codex';
import {
  buildCodexAccountPresentation,
  buildQuotaPreviewLines,
} from '../presentation/platformAccountPresentation';
import { buildValidAccountsFilterOption, splitValidityFilterValues } from '../utils/accountValidityFilter';
import {
  formatCodexQuotaPoolPercent,
  summarizeCodexQuotaPool,
  type CodexQuotaPoolItem,
} from '../utils/codexQuotaPool';
import { AccountTagFilterDropdown } from './AccountTagFilterDropdown';
import {
  MultiSelectFilterDropdown,
  type MultiSelectFilterOption,
} from './MultiSelectFilterDropdown';
import { SingleSelectDropdown } from './SingleSelectDropdown';
import './GroupAccountPickerModal.css';
import './CodexLocalAccessModal.css';

interface CodexLocalAccessModalProps {
  isOpen: boolean;
  mode: 'panel' | 'members';
  state: CodexLocalAccessState | null;
  accounts: CodexAccount[];
  accountGroups: CodexAccountGroup[];
  initialSelectedIds: string[];
  maskAccountText: (value?: string | null) => string;
  onClose: () => void;
  onSaveAccounts: (payload: {
    accountIds: string[];
    restrictFreeAccounts: boolean;
  }) => Promise<unknown> | unknown;
  onClearStats: () => Promise<unknown> | unknown;
  onRefreshStats: () => Promise<unknown> | unknown;
  onUpdatePort: (port: number) => Promise<unknown> | unknown;
  onUpdateRoutingStrategy: (
    strategy: CodexLocalAccessRoutingStrategy,
  ) => Promise<unknown> | unknown;
  onCreateApiKey: (payload: {
    name: string;
    monthlyTokenLimit: number | null;
    upstreamScope: 'all' | 'selected';
    allowedAccountIds: string[];
  }) => Promise<unknown> | unknown;
  onUpdateApiKey: (
    apiKeyId: string,
    payload: {
      name: string;
      enabled: boolean;
      monthlyTokenLimit: number | null;
      upstreamScope: 'all' | 'selected';
      allowedAccountIds: string[];
    },
  ) => Promise<unknown> | unknown;
  onSetDefaultApiKey: (apiKeyId: string) => Promise<unknown> | unknown;
  onRotateApiKey: (apiKeyId: string) => Promise<unknown> | unknown;
  onDeleteApiKey: (apiKeyId: string) => Promise<unknown> | unknown;
  onKillPort: () => Promise<unknown> | unknown;
  onToggleEnabled: () => Promise<unknown> | unknown;
  onTest: () => Promise<number> | number;
  saving: boolean;
  testing: boolean;
  starting: boolean;
  portCleanupBusy: boolean;
}

type StatsRangeKey = 'daily' | 'weekly' | 'monthly';
type CopyableField = 'apiPortUrl' | 'baseUrl' | 'modelId' | `apiKey:${string}`;
type ApiKeyDraft = {
  name: string;
  enabled: boolean;
  monthlyTokenLimit: string;
  upstreamScope: 'all' | 'selected';
  allowedAccountIds: string[];
};
type ModelPriceSource = 'builtin' | 'openai' | 'manual';
type ModelPriceField =
  | 'inputUsdPerMillion'
  | 'cachedInputUsdPerMillion'
  | 'outputUsdPerMillion';

interface CodexModelPrice {
  modelId: string;
  inputUsdPerMillion: number | null;
  cachedInputUsdPerMillion: number | null;
  outputUsdPerMillion: number | null;
  source: ModelPriceSource;
  updatedAt: number;
}

interface CostEstimate {
  usd: number;
  unknownModelIds: string[];
}

const CODEX_LOCAL_ACCESS_STATS_RANGE_STORAGE_KEY =
  'agtools.codex.local_access.stats_range.v1';
const CODEX_LOCAL_ACCESS_MODEL_PRICES_STORAGE_KEY =
  'agtools.codex.local_access.model_prices.v1';
const OPENAI_PRICING_SOURCE_URL = 'https://developers.openai.com/api/docs/pricing';
const TOKEN_PRICE_DENOMINATOR = 1_000_000;
const LEGACY_UNKNOWN_MODEL_PRICE_ID = 'unknown';
const MODEL_STATS_COLLAPSED_LIMIT = 8;

const DEFAULT_MODEL_PRICES: CodexModelPrice[] = [
  { modelId: LEGACY_UNKNOWN_MODEL_PRICE_ID, inputUsdPerMillion: 5, cachedInputUsdPerMillion: 0.5, outputUsdPerMillion: 30, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5-codex', inputUsdPerMillion: 5, cachedInputUsdPerMillion: 0.5, outputUsdPerMillion: 30, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5-codex-mini', inputUsdPerMillion: 0.75, cachedInputUsdPerMillion: 0.075, outputUsdPerMillion: 4.5, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.3-codex', inputUsdPerMillion: 2.5, cachedInputUsdPerMillion: 0.25, outputUsdPerMillion: 15, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.3-codex-spark', inputUsdPerMillion: 0.75, cachedInputUsdPerMillion: 0.075, outputUsdPerMillion: 4.5, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.2-codex', inputUsdPerMillion: 1.75, cachedInputUsdPerMillion: 0.175, outputUsdPerMillion: 14, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.1-codex-max', inputUsdPerMillion: 1.25, cachedInputUsdPerMillion: 0.125, outputUsdPerMillion: 10, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.1-codex-mini', inputUsdPerMillion: 0.25, cachedInputUsdPerMillion: 0.025, outputUsdPerMillion: 2, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.5', inputUsdPerMillion: 5, cachedInputUsdPerMillion: 0.5, outputUsdPerMillion: 30, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.5-pro', inputUsdPerMillion: 30, cachedInputUsdPerMillion: null, outputUsdPerMillion: 180, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.4', inputUsdPerMillion: 2.5, cachedInputUsdPerMillion: 0.25, outputUsdPerMillion: 15, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.4-mini', inputUsdPerMillion: 0.75, cachedInputUsdPerMillion: 0.075, outputUsdPerMillion: 4.5, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.4-nano', inputUsdPerMillion: 0.2, cachedInputUsdPerMillion: 0.02, outputUsdPerMillion: 1.25, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.4-pro', inputUsdPerMillion: 30, cachedInputUsdPerMillion: null, outputUsdPerMillion: 180, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.2', inputUsdPerMillion: 1.75, cachedInputUsdPerMillion: 0.175, outputUsdPerMillion: 14, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.2-pro', inputUsdPerMillion: 21, cachedInputUsdPerMillion: null, outputUsdPerMillion: 168, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5.1', inputUsdPerMillion: 1.25, cachedInputUsdPerMillion: 0.125, outputUsdPerMillion: 10, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5', inputUsdPerMillion: 1.25, cachedInputUsdPerMillion: 0.125, outputUsdPerMillion: 10, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5-mini', inputUsdPerMillion: 0.25, cachedInputUsdPerMillion: 0.025, outputUsdPerMillion: 2, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5-nano', inputUsdPerMillion: 0.05, cachedInputUsdPerMillion: 0.005, outputUsdPerMillion: 0.4, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-5-pro', inputUsdPerMillion: 15, cachedInputUsdPerMillion: null, outputUsdPerMillion: 120, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-4.1', inputUsdPerMillion: 2, cachedInputUsdPerMillion: 0.5, outputUsdPerMillion: 8, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-4.1-mini', inputUsdPerMillion: 0.4, cachedInputUsdPerMillion: 0.1, outputUsdPerMillion: 1.6, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-4.1-nano', inputUsdPerMillion: 0.1, cachedInputUsdPerMillion: 0.025, outputUsdPerMillion: 0.4, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-4o', inputUsdPerMillion: 2.5, cachedInputUsdPerMillion: 1.25, outputUsdPerMillion: 10, source: 'builtin', updatedAt: 0 },
  { modelId: 'gpt-4o-mini', inputUsdPerMillion: 0.15, cachedInputUsdPerMillion: 0.075, outputUsdPerMillion: 0.6, source: 'builtin', updatedAt: 0 },
];

function normalizeStatsRangeKey(value: string | null | undefined): StatsRangeKey {
  if (value === 'weekly' || value === 'monthly') {
    return value;
  }
  return 'daily';
}

function readStoredStatsRange(): StatsRangeKey {
  try {
    return normalizeStatsRangeKey(localStorage.getItem(CODEX_LOCAL_ACCESS_STATS_RANGE_STORAGE_KEY));
  } catch {
    return 'daily';
  }
}

function persistStatsRange(value: StatsRangeKey): void {
  try {
    localStorage.setItem(CODEX_LOCAL_ACCESS_STATS_RANGE_STORAGE_KEY, value);
  } catch {
    // ignore storage write failures
  }
}

function normalizeModelPriceId(value: string): string {
  return value
    .trim()
    .replace(/\s*\([^)]*\)\s*/g, '')
    .toLowerCase();
}

function isDateSnapshotSuffix(value: string): boolean {
  return /^-\d{4}-\d{2}-\d{2}$/.test(value);
}

function normalizeNullablePrice(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
    return value;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed || trimmed === '-') return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
  }
  return null;
}

function normalizeModelPrice(value: unknown): CodexModelPrice | null {
  if (!value || typeof value !== 'object') return null;
  const item = value as Partial<CodexModelPrice>;
  const modelId = typeof item.modelId === 'string' ? item.modelId.trim() : '';
  if (!modelId) return null;
  const source: ModelPriceSource =
    item.source === 'openai' || item.source === 'manual' || item.source === 'builtin'
      ? item.source
      : 'manual';
  const updatedAt =
    typeof item.updatedAt === 'number' && Number.isFinite(item.updatedAt) ? item.updatedAt : 0;

  return {
    modelId,
    inputUsdPerMillion: normalizeNullablePrice(item.inputUsdPerMillion),
    cachedInputUsdPerMillion: normalizeNullablePrice(item.cachedInputUsdPerMillion),
    outputUsdPerMillion: normalizeNullablePrice(item.outputUsdPerMillion),
    source,
    updatedAt,
  };
}

function mergeModelPrices(
  basePrices: CodexModelPrice[],
  overridePrices: CodexModelPrice[],
): CodexModelPrice[] {
  const next = new Map<string, CodexModelPrice>();
  basePrices.forEach((price) => next.set(normalizeModelPriceId(price.modelId), price));
  overridePrices.forEach((price) => next.set(normalizeModelPriceId(price.modelId), price));
  return Array.from(next.values()).sort((left, right) =>
    normalizeModelPriceId(left.modelId).localeCompare(normalizeModelPriceId(right.modelId)),
  );
}

function readStoredModelPrices(): CodexModelPrice[] {
  try {
    const raw = localStorage.getItem(CODEX_LOCAL_ACCESS_MODEL_PRICES_STORAGE_KEY);
    if (!raw) return [...DEFAULT_MODEL_PRICES];
    const parsed = JSON.parse(raw) as unknown;
    const source = Array.isArray(parsed)
      ? parsed
      : parsed && typeof parsed === 'object' && Array.isArray((parsed as { prices?: unknown }).prices)
        ? (parsed as { prices: unknown[] }).prices
        : [];
    const storedPrices = source
      .map(normalizeModelPrice)
      .filter((price): price is CodexModelPrice => Boolean(price));
    return mergeModelPrices(DEFAULT_MODEL_PRICES, storedPrices);
  } catch {
    return [...DEFAULT_MODEL_PRICES];
  }
}

function persistModelPrices(prices: CodexModelPrice[]): void {
  try {
    localStorage.setItem(
      CODEX_LOCAL_ACCESS_MODEL_PRICES_STORAGE_KEY,
      JSON.stringify({ prices, updatedAt: Date.now() }),
    );
  } catch {
    // ignore storage write failures
  }
}

function parseOpenAiPricingCell(rawValue: string): number | null {
  const value = rawValue.trim();
  if (!value || value === 'null' || value === 'undefined') return null;
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    return normalizeNullablePrice(value.slice(1, -1));
  }
  return normalizeNullablePrice(value);
}

function parseOpenAiPricingMarkdown(markdown: string): CodexModelPrice[] {
  const standardStart = markdown.indexOf('data-value="standard"');
  if (standardStart < 0) return [];
  const nextPaneStart = markdown.indexOf('data-value="batch"', standardStart + 1);
  const standardBlock =
    nextPaneStart > standardStart ? markdown.slice(standardStart, nextPaneStart) : markdown.slice(standardStart);
  const rowPattern =
    /\[\s*"([^"]+)"\s*,\s*([^,\]]+)\s*,\s*([^,\]]+)\s*,\s*([^,\]]+)\s*\]/g;
  const parsedAt = Date.now();
  const prices: CodexModelPrice[] = [];
  for (const match of standardBlock.matchAll(rowPattern)) {
    const modelId = match[1].replace(/\s*\([^)]*\)\s*/g, '').trim();
    if (!modelId) continue;
    const inputUsdPerMillion = parseOpenAiPricingCell(match[2]);
    const cachedInputUsdPerMillion = parseOpenAiPricingCell(match[3]);
    const outputUsdPerMillion = parseOpenAiPricingCell(match[4]);
    if (inputUsdPerMillion == null || outputUsdPerMillion == null) continue;
    prices.push({
      modelId,
      inputUsdPerMillion,
      cachedInputUsdPerMillion,
      outputUsdPerMillion,
      source: 'openai',
      updatedAt: parsedAt,
    });
  }
  return prices;
}

function resolveModelPrice(
  modelId: string | undefined,
  pricesByModelId: Map<string, CodexModelPrice>,
): CodexModelPrice | null {
  const normalized = normalizeModelPriceId(modelId || '');
  if (!normalized) return null;
  const exact = pricesByModelId.get(normalized);
  if (exact) return exact;

  const datedMatch = Array.from(pricesByModelId.keys())
    .filter((candidate) => normalized.startsWith(`${candidate}-`))
    .filter((candidate) => isDateSnapshotSuffix(normalized.slice(candidate.length)))
    .sort((left, right) => right.length - left.length)[0];
  return datedMatch ? pricesByModelId.get(datedMatch) ?? null : null;
}

function estimateEventsCost(
  events: CodexLocalAccessUsageEvent[],
  pricesByModelId: Map<string, CodexModelPrice>,
): CostEstimate {
  let usd = 0;
  const unknownModels = new Set<string>();

  events.forEach((event) => {
    const inputTokens = Math.max(0, event.inputTokens || 0);
    const cachedTokens = Math.min(inputTokens, Math.max(0, event.cachedTokens || 0));
    const outputTokens = Math.max(event.outputTokens || 0, event.reasoningTokens || 0);
    if (inputTokens === 0 && outputTokens === 0) return;

    const modelPriceId = event.modelId?.trim() || LEGACY_UNKNOWN_MODEL_PRICE_ID;
    const price = resolveModelPrice(modelPriceId, pricesByModelId);
    if (price?.inputUsdPerMillion == null || price.outputUsdPerMillion == null) {
      unknownModels.add(modelPriceId);
      return;
    }

    const uncachedInputTokens = Math.max(0, inputTokens - cachedTokens);
    const cachedInputPrice = price.cachedInputUsdPerMillion ?? price.inputUsdPerMillion;
    usd +=
      (uncachedInputTokens * price.inputUsdPerMillion +
        cachedTokens * cachedInputPrice +
        outputTokens * price.outputUsdPerMillion) /
      TOKEN_PRICE_DENOMINATOR;
  });

  return {
    usd,
    unknownModelIds: Array.from(unknownModels).sort((left, right) => left.localeCompare(right)),
  };
}

function estimateUsageStatsCost(
  usage: CodexLocalAccessUsageStats | undefined,
  pricesByModelId: Map<string, CodexModelPrice>,
  modelId = LEGACY_UNKNOWN_MODEL_PRICE_ID,
): CostEstimate {
  if (!usage) {
    return { usd: 0, unknownModelIds: [] };
  }

  return estimateEventsCost(
    [
      {
        timestamp: 0,
        modelId,
        accountId: '',
        email: '',
        apiKeyId: '',
        apiKeyName: '',
        success: true,
        latencyMs: 0,
        inputTokens: usage.inputTokens,
        outputTokens: usage.outputTokens,
        totalTokens: usage.totalTokens,
        cachedTokens: usage.cachedTokens,
        reasoningTokens: usage.reasoningTokens,
      },
    ],
    pricesByModelId,
  );
}

function mergeCostEstimates(estimates: CostEstimate[]): CostEstimate {
  const unknownModels = new Set<string>();
  const usd = estimates.reduce((total, estimate) => {
    estimate.unknownModelIds.forEach((modelId) => unknownModels.add(modelId));
    return total + estimate.usd;
  }, 0);
  return {
    usd,
    unknownModelIds: Array.from(unknownModels).sort((left, right) => left.localeCompare(right)),
  };
}

function estimateModelStatsCost(
  modelStats: CodexLocalAccessModelStats,
  pricesByModelId: Map<string, CodexModelPrice>,
): CostEstimate {
  return estimateUsageStatsCost(
    modelStats.usage,
    pricesByModelId,
    modelStats.modelId?.trim() || LEGACY_UNKNOWN_MODEL_PRICE_ID,
  );
}

function estimateModelStatsListCost(
  models: CodexLocalAccessModelStats[] | undefined,
  pricesByModelId: Map<string, CodexModelPrice>,
  fallbackUsage?: CodexLocalAccessUsageStats,
): CostEstimate {
  if (models && models.length > 0) {
    return mergeCostEstimates(
      models.map((modelStats) => estimateModelStatsCost(modelStats, pricesByModelId)),
    );
  }
  return estimateUsageStatsCost(fallbackUsage, pricesByModelId);
}

function sortModelStats(models: CodexLocalAccessModelStats[]): CodexLocalAccessModelStats[] {
  return [...models].sort((left, right) => {
    const tokenDelta = (right.usage?.totalTokens ?? 0) - (left.usage?.totalTokens ?? 0);
    if (tokenDelta !== 0) return tokenDelta;
    const requestDelta = (right.usage?.requestCount ?? 0) - (left.usage?.requestCount ?? 0);
    if (requestDelta !== 0) return requestDelta;
    const updatedDelta = (right.updatedAt ?? 0) - (left.updatedAt ?? 0);
    if (updatedDelta !== 0) return updatedDelta;
    return (left.modelId || '').localeCompare(right.modelId || '');
  });
}

function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat('en', {
    notation: value >= 1000 ? 'compact' : 'standard',
    maximumFractionDigits: value >= 1000 ? 1 : 0,
  }).format(value || 0);
}

function formatLatencyMs(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '--';
  if (value >= 1000) return `${(value / 1000).toFixed(2)}s`;
  return `${Math.round(value)}ms`;
}

function formatLocalDateTime(value: number | null | undefined): string {
  if (!value) return '--';
  try {
    return new Intl.DateTimeFormat(undefined, {
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    }).format(new Date(value));
  } catch {
    return '--';
  }
}

function formatUsd(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '$0.00';
  if (value < 0.0001) return '$<0.0001';
  return `$${value.toLocaleString('en-US', {
    minimumFractionDigits: value < 1 ? 4 : 2,
    maximumFractionDigits: value < 1 ? 4 : 2,
  })}`;
}

function formatCostEstimate(estimate: CostEstimate): string {
  if (estimate.unknownModelIds.length > 0 && estimate.usd <= 0) return '--';
  return `${formatUsd(estimate.usd)}${estimate.unknownModelIds.length > 0 ? '+' : ''}`;
}

function formatPriceInputValue(value: number | null): string {
  return value == null ? '' : String(value);
}

function formatApiKeyValue(apiKey: string, visible: boolean): string {
  if (visible) return apiKey;
  if (apiKey.length <= 14) return '••••••••••••';
  return `${apiKey.slice(0, 10)}••••••••••••`;
}

function extractUrlHost(value?: string | null): string {
  const trimmed = value?.trim();
  if (!trimmed) return '';
  try {
    return new URL(trimmed).host;
  } catch {
    return trimmed.replace(/^https?:\/\//i, '').split('/')[0] || trimmed;
  }
}

function getApiKeyProviderName(account: CodexAccount): string {
  return (
    account.api_provider_name?.trim() ||
    extractUrlHost(account.api_base_url) ||
    'OpenAI API'
  );
}

function getAccountSourceLabel(account: CodexAccount, t: ReturnType<typeof useTranslation>['t']): string {
  return isCodexApiKeyAccount(account)
    ? t('codex.localAccess.source.relay', '中转站')
    : t('codex.localAccess.source.codex', 'Codex');
}

function draftFromApiKey(apiKey: CodexLocalAccessApiKey): ApiKeyDraft {
  return {
    name: apiKey.name,
    enabled: apiKey.enabled,
    monthlyTokenLimit: apiKey.monthlyTokenLimit ? String(apiKey.monthlyTokenLimit) : '',
    upstreamScope: apiKey.allowedAccountIds == null ? 'all' : 'selected',
    allowedAccountIds: apiKey.allowedAccountIds ?? [],
  };
}

function areStringArraysEqual(left: string[], right: string[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
}

function formatQuotaPoolLabel(
  baseLabel: string,
  pool: CodexQuotaPoolItem,
  hourlyLabel: string,
  weeklyLabel: string,
): string {
  return `${baseLabel} · ${hourlyLabel} ${formatCodexQuotaPoolPercent(pool.hourly)} · ${weeklyLabel} ${formatCodexQuotaPoolPercent(pool.weekly)}`;
}

function areSetsEqual(left: Set<string>, right: Set<string>): boolean {
  if (left.size !== right.size) return false;
  for (const value of left) {
    if (!right.has(value)) return false;
  }
  return true;
}

export function CodexLocalAccessModal({
  isOpen,
  mode,
  state,
  accounts,
  accountGroups,
  initialSelectedIds,
  maskAccountText,
  onClose,
  onSaveAccounts,
  onClearStats,
  onRefreshStats,
  onUpdatePort,
  onUpdateRoutingStrategy,
  onCreateApiKey,
  onUpdateApiKey,
  onSetDefaultApiKey,
  onRotateApiKey,
  onDeleteApiKey,
  onKillPort,
  onToggleEnabled,
  onTest,
  saving,
  testing,
  starting,
  portCleanupBusy,
}: CodexLocalAccessModalProps) {
  const { t } = useTranslation();
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [filterTypes, setFilterTypes] = useState<string[]>([]);
  const [tagFilter, setTagFilter] = useState<string[]>([]);
  const [groupFilter, setGroupFilter] = useState<string[]>([]);
  const [restrictFreeAccounts, setRestrictFreeAccounts] = useState(true);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [portInput, setPortInput] = useState('');
  const [visibleApiKeyIds, setVisibleApiKeyIds] = useState<Set<string>>(new Set());
  const [apiKeyDrafts, setApiKeyDrafts] = useState<Record<string, ApiKeyDraft>>({});
  const [newApiKeyName, setNewApiKeyName] = useState('');
  const [newApiKeyLimit, setNewApiKeyLimit] = useState('');
  const [copiedField, setCopiedField] = useState<CopyableField | null>(null);
  const [selectedModelId, setSelectedModelId] = useState('');
  const [statsRange, setStatsRange] = useState<StatsRangeKey>(() => readStoredStatsRange());
  const [modelPrices, setModelPrices] = useState<CodexModelPrice[]>(() => readStoredModelPrices());
  const [pricingSyncing, setPricingSyncing] = useState(false);
  const [pricingError, setPricingError] = useState('');
  const [totalModelsExpanded, setTotalModelsExpanded] = useState(false);
  const [showAllTotalModels, setShowAllTotalModels] = useState(false);
  const [expandedApiKeyModelIds, setExpandedApiKeyModelIds] = useState<Set<string>>(new Set());
  const [showAllApiKeyModelIds, setShowAllApiKeyModelIds] = useState<Set<string>>(new Set());
  const selectAllCheckboxRef = useRef<HTMLInputElement | null>(null);
  const searchInputRef = useRef<HTMLInputElement | null>(null);

  const collection = state?.collection ?? null;
  const apiKeys = useMemo(() => collection?.apiKeys ?? [], [collection?.apiKeys]);
  const apiPortUrl = state?.apiPortUrl ?? '';
  const baseUrl = state?.baseUrl ?? '';
  const modelIds = state?.modelIds ?? [];
  const stats = state?.stats;
  const statsRangeOptions = useMemo(
    () =>
      [
        { key: 'daily', label: t('codex.localAccess.statsRange.daily', '日') },
        { key: 'weekly', label: t('codex.localAccess.statsRange.weekly', '周') },
        { key: 'monthly', label: t('codex.localAccess.statsRange.monthly', '月') },
      ] satisfies Array<{ key: StatsRangeKey; label: string }>,
    [t],
  );
  const quotaPoolLabels = useMemo(
    () => ({
      hourly: t('codex.localAccess.quotaPool.hourlyShort', '5h'),
      weekly: t('codex.localAccess.quotaPool.weeklyShort', '周'),
      title: t('codex.localAccess.quotaPool.title', '额度池'),
    }),
    [t],
  );
  const selectedStatsWindow = useMemo<CodexLocalAccessStatsWindow | null>(() => {
    if (!stats) return null;
    return stats[statsRange];
  }, [stats, statsRange]);
  const selectedTotals = selectedStatsWindow?.totals;
  const selectedModelStats = selectedStatsWindow?.models ?? [];
  const routingStrategy = collection?.routingStrategy ?? 'auto';
  const modelIdOptions = useMemo(
    () => modelIds.map((modelId) => ({ value: modelId, label: modelId })),
    [modelIds],
  );
  const avgLatencyMs =
    selectedTotals && selectedTotals.requestCount > 0
      ? selectedTotals.totalLatencyMs / selectedTotals.requestCount
      : 0;
  const successRate =
    selectedTotals && selectedTotals.requestCount > 0
      ? Math.round((selectedTotals.successCount / selectedTotals.requestCount) * 100)
      : 0;
  const actionBusy = saving || testing || starting || portCleanupBusy;
  const oauthAccounts = useMemo(
    () => accounts.filter((account) => !isCodexApiKeyAccount(account)),
    [accounts],
  );
  const upstreamAccounts = useMemo(
    () => accounts,
    [accounts],
  );
  const upstreamSourceByAccountId = useMemo(() => {
    const next = new Map<string, NonNullable<CodexLocalAccessState['upstreamSources']>[number]>();
    state?.upstreamSources?.forEach((source) => next.set(source.accountId, source));
    return next;
  }, [state?.upstreamSources]);
  const currentMemberAccounts = useMemo(() => {
    const ids = collection?.accountIds ?? [];
    return ids
      .map((accountId) => upstreamAccounts.find((account) => account.id === accountId))
      .filter((account): account is CodexAccount => Boolean(account));
  }, [collection?.accountIds, upstreamAccounts]);
  const currentMemberAccountIdSet = useMemo(
    () => new Set(currentMemberAccounts.map((account) => account.id)),
    [currentMemberAccounts],
  );
  const quotaPoolSummary = useMemo(
    () => summarizeCodexQuotaPool(oauthAccounts),
    [oauthAccounts],
  );
  const currentQuotaPoolSummary = useMemo(() => {
    const accountIds = new Set(collection?.accountIds ?? []);
    return summarizeCodexQuotaPool(oauthAccounts.filter((account) => accountIds.has(account.id)));
  }, [collection?.accountIds, oauthAccounts]);
  const oauthAccountIdSet = useMemo(
    () => new Set(upstreamAccounts.map((account) => account.id)),
    [upstreamAccounts],
  );
  const normalizedInitialSelectedIds = useMemo(
    () => initialSelectedIds.filter((accountId) => oauthAccountIdSet.has(accountId)),
    [initialSelectedIds, oauthAccountIdSet],
  );

  useEffect(() => {
    if (!isOpen) return;
    setQuery('');
    setSelected(new Set(normalizedInitialSelectedIds));
    setFilterTypes([]);
    setTagFilter([]);
    setGroupFilter([]);
    setRestrictFreeAccounts(collection?.restrictFreeAccounts ?? true);
    setError('');
    setNotice('');
    setVisibleApiKeyIds(new Set());
    setNewApiKeyName('');
    setNewApiKeyLimit('');
    setCopiedField(null);
    setPortInput(collection?.port ? String(collection.port) : '');
    if (mode === 'members') {
      window.setTimeout(() => {
        searchInputRef.current?.focus();
      }, 0);
    }
  }, [collection?.port, collection?.restrictFreeAccounts, isOpen, mode, normalizedInitialSelectedIds]);

  useEffect(() => {
    if (!isOpen) return;
    const nextDrafts: Record<string, ApiKeyDraft> = {};
    apiKeys.forEach((apiKey) => {
      nextDrafts[apiKey.id] = draftFromApiKey(apiKey);
    });
    setApiKeyDrafts(nextDrafts);
  }, [apiKeys, isOpen]);

  useEffect(() => {
    if (modelIds.length === 0) {
      setSelectedModelId('');
      return;
    }
    setSelectedModelId((current) => (modelIds.includes(current) ? current : modelIds[0]));
  }, [modelIds]);

  useEffect(() => {
    persistStatsRange(statsRange);
  }, [statsRange]);

  useEffect(() => {
    persistModelPrices(modelPrices);
  }, [modelPrices]);

  useEffect(() => {
    setShowAllTotalModels(false);
  }, [statsRange]);

  const normalizeTag = (value: string) => value.trim().toLowerCase();

  const availableTags = useMemo(() => {
    const next = new Set<string>();
    upstreamAccounts.forEach((account) => {
      (account.tags || []).forEach((tag) => {
        const trimmed = tag.trim();
        if (trimmed) next.add(trimmed);
      });
    });
    return Array.from(next).sort((left, right) => left.localeCompare(right));
  }, [upstreamAccounts]);

  const groupIdsByAccountId = useMemo(() => {
    const next = new Map<string, Set<string>>();
    accountGroups.forEach((group) => {
      group.accountIds.forEach((accountId) => {
        const current = next.get(accountId) ?? new Set<string>();
        current.add(group.id);
        next.set(accountId, current);
      });
    });
    return next;
  }, [accountGroups]);

  const groupNameByAccountId = useMemo(() => {
    const next = new Map<string, string[]>();
    accountGroups.forEach((group) => {
      group.accountIds.forEach((accountId) => {
        const current = next.get(accountId) ?? [];
        current.push(group.name);
        next.set(accountId, current);
      });
    });
    return next;
  }, [accountGroups]);

  const groupFilterOptions = useMemo<MultiSelectFilterOption[]>(
    () =>
      accountGroups
        .map((group) => ({
          value: group.id,
          label: `${group.name} (${group.accountIds.length})`,
        }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [accountGroups],
  );

  const tierCounts = useMemo(() => {
    const counts = { all: upstreamAccounts.length, VALID: 0, FREE: 0, PLUS: 0, PRO: 0, TEAM: 0, ENTERPRISE: 0, API_KEY: 0, ERROR: 0 };
    upstreamAccounts.forEach((account) => {
      if (isCodexApiKeyAccount(account)) {
        counts.API_KEY += 1;
        counts.VALID += 1;
        return;
      }
      if (!account.quota_error) {
        counts.VALID += 1;
      }
      const tier = getCodexPlanFilterKey(account);
      if (tier in counts) {
        counts[tier as keyof typeof counts] += 1;
      }
      if (account.quota_error) {
        counts.ERROR += 1;
      }
    });
    return counts;
  }, [upstreamAccounts]);

  const allTierFilterLabel = useMemo(
    () =>
      formatQuotaPoolLabel(
        t('common.shared.filter.all', { count: tierCounts.all }),
        quotaPoolSummary.all,
        quotaPoolLabels.hourly,
        quotaPoolLabels.weekly,
      ),
    [quotaPoolLabels.hourly, quotaPoolLabels.weekly, quotaPoolSummary.all, t, tierCounts.all],
  );

  const tierFilterOptions = useMemo<MultiSelectFilterOption[]>(
    () => [
      {
        value: 'API_KEY',
        label: `${t('codex.localAccess.source.relay', '中转站')} (${tierCounts.API_KEY})`,
      },
      {
        value: 'FREE',
        label: formatQuotaPoolLabel(
          `FREE (${tierCounts.FREE})`,
          quotaPoolSummary.byPlan.FREE,
          quotaPoolLabels.hourly,
          quotaPoolLabels.weekly,
        ),
      },
      {
        value: 'PLUS',
        label: formatQuotaPoolLabel(
          `PLUS (${tierCounts.PLUS})`,
          quotaPoolSummary.byPlan.PLUS,
          quotaPoolLabels.hourly,
          quotaPoolLabels.weekly,
        ),
      },
      {
        value: 'PRO',
        label: formatQuotaPoolLabel(
          `PRO (${tierCounts.PRO})`,
          quotaPoolSummary.byPlan.PRO,
          quotaPoolLabels.hourly,
          quotaPoolLabels.weekly,
        ),
      },
      {
        value: 'TEAM',
        label: formatQuotaPoolLabel(
          `TEAM (${tierCounts.TEAM})`,
          quotaPoolSummary.byPlan.TEAM,
          quotaPoolLabels.hourly,
          quotaPoolLabels.weekly,
        ),
      },
      {
        value: 'ENTERPRISE',
        label: formatQuotaPoolLabel(
          `ENTERPRISE (${tierCounts.ENTERPRISE})`,
          quotaPoolSummary.byPlan.ENTERPRISE,
          quotaPoolLabels.hourly,
          quotaPoolLabels.weekly,
        ),
      },
      { value: 'ERROR', label: `ERROR (${tierCounts.ERROR})` },
      buildValidAccountsFilterOption(t, tierCounts.VALID),
    ],
    [quotaPoolLabels.hourly, quotaPoolLabels.weekly, quotaPoolSummary.byPlan, t, tierCounts],
  );

  const visibleAccounts = useMemo(() => {
    const queryText = query.trim().toLowerCase();
    const sorted = [...upstreamAccounts].sort((a, b) => {
      const aName = buildCodexAccountPresentation(a, t).displayName.toLowerCase();
      const bName = buildCodexAccountPresentation(b, t).displayName.toLowerCase();
      return aName.localeCompare(bName);
    });
    const selectedTags = new Set(tagFilter.map(normalizeTag));
    const selectedGroups = new Set(groupFilter);
    const { requireValidAccounts, selectedTypes } = splitValidityFilterValues(filterTypes);

    return sorted.filter((account) => {
      const presentation = buildCodexAccountPresentation(account, t);
      const displayName = presentation.displayName.toLowerCase();
      const groupNames = (groupNameByAccountId.get(account.id) ?? []).join(' ').toLowerCase();
      const matchesQuery =
        !queryText || displayName.includes(queryText) || groupNames.includes(queryText);
      if (!matchesQuery) return false;

      if (selectedTags.size > 0) {
        const accountTags = (account.tags || []).map(normalizeTag);
        if (!accountTags.some((tag) => selectedTags.has(tag))) {
          return false;
        }
      }

      if (selectedGroups.size > 0) {
        const accountGroupIds = groupIdsByAccountId.get(account.id);
        if (!accountGroupIds || !Array.from(accountGroupIds).some((id) => selectedGroups.has(id))) {
          return false;
        }
      }

      if (requireValidAccounts && account.quota_error) {
        return false;
      }

      if (selectedTypes.size > 0) {
        const planKey = isCodexApiKeyAccount(account) ? 'API_KEY' : getCodexPlanFilterKey(account);
        const matchesType = Array.from(selectedTypes).some((type) => {
          if (type === 'ERROR') return Boolean(account.quota_error);
          return type === planKey;
        });
        if (!matchesType) {
          return false;
        }
      }

      return true;
    });
  }, [filterTypes, groupFilter, groupIdsByAccountId, groupNameByAccountId, upstreamAccounts, query, t, tagFilter]);

  const visibleSelectableAccounts = useMemo(
    () =>
      visibleAccounts.filter((account) => {
        const source = upstreamSourceByAccountId.get(account.id);
        if (source && !source.eligible) return selected.has(account.id);
        if (isCodexApiKeyAccount(account)) return true;
        if (!restrictFreeAccounts) return true;
        if (!isCodexExplicitFreePlanType(account.plan_type)) return true;
        return selected.has(account.id);
      }),
    [restrictFreeAccounts, selected, upstreamSourceByAccountId, visibleAccounts],
  );

  const selectedVisibleCount = useMemo(
    () =>
      visibleSelectableAccounts.reduce(
        (count, account) => count + (selected.has(account.id) ? 1 : 0),
        0,
      ),
    [selected, visibleSelectableAccounts],
  );

  const allVisibleSelected =
    visibleSelectableAccounts.length > 0 &&
    selectedVisibleCount === visibleSelectableAccounts.length;

  useEffect(() => {
    if (!selectAllCheckboxRef.current) return;
    selectAllCheckboxRef.current.indeterminate =
      selectedVisibleCount > 0 && !allVisibleSelected;
  }, [allVisibleSelected, selectedVisibleCount]);

  const selectionDirty = useMemo(
    () =>
      !areSetsEqual(selected, new Set(normalizedInitialSelectedIds)) ||
      restrictFreeAccounts !== (collection?.restrictFreeAccounts ?? true),
    [collection?.restrictFreeAccounts, normalizedInitialSelectedIds, restrictFreeAccounts, selected],
  );

  const allStatsByAccountId = useMemo(() => {
    const next = new Map<string, NonNullable<CodexLocalAccessState['stats']>['accounts'][number]>();
    stats?.accounts.forEach((item) => next.set(item.accountId, item));
    return next;
  }, [stats?.accounts]);

  const windowStatsByAccountId = useMemo(() => {
    const next = new Map<string, NonNullable<CodexLocalAccessState['stats']>['accounts'][number]>();
    selectedStatsWindow?.accounts.forEach((item) => next.set(item.accountId, item));
    return next;
  }, [selectedStatsWindow?.accounts]);

  const windowStatsByApiKeyId = useMemo(() => {
    const next = new Map<string, NonNullable<CodexLocalAccessState['stats']>['apiKeys'][number]>();
    selectedStatsWindow?.apiKeys.forEach((item) => next.set(item.apiKeyId, item));
    return next;
  }, [selectedStatsWindow?.apiKeys]);

  const monthlyStatsByApiKeyId = useMemo(() => {
    const next = new Map<string, NonNullable<CodexLocalAccessState['stats']>['apiKeys'][number]>();
    stats?.monthly.apiKeys.forEach((item) => next.set(item.apiKeyId, item));
    return next;
  }, [stats?.monthly.apiKeys]);

  const selectedUsageEvents = useMemo(() => {
    const since = selectedStatsWindow?.since ?? 0;
    return (stats?.events ?? []).filter((event) => event.timestamp >= since);
  }, [selectedStatsWindow?.since, stats?.events]);

  const pricesByModelId = useMemo(() => {
    const next = new Map<string, CodexModelPrice>();
    modelPrices.forEach((price) => {
      const normalized = normalizeModelPriceId(price.modelId);
      if (normalized) next.set(normalized, price);
    });
    return next;
  }, [modelPrices]);

  const modelPriceRows = useMemo(() => {
    const seen = new Set<string>();
    const rows: CodexModelPrice[] = [];
    const append = (modelId: string, fallback?: CodexModelPrice) => {
      const trimmed = modelId.trim();
      const normalized = normalizeModelPriceId(trimmed);
      if (!trimmed || seen.has(normalized)) return;
      seen.add(normalized);
      rows.push(
        pricesByModelId.get(normalized) ??
          fallback ?? {
            modelId: trimmed,
            inputUsdPerMillion: null,
            cachedInputUsdPerMillion: null,
            outputUsdPerMillion: null,
            source: 'manual',
            updatedAt: 0,
          },
      );
    };

    modelIds.forEach((modelId) => append(modelId));
    selectedUsageEvents.forEach((event) => {
      append(event.modelId?.trim() || LEGACY_UNKNOWN_MODEL_PRICE_ID);
    });
    selectedModelStats.forEach((modelStats) => {
      append(modelStats.modelId?.trim() || LEGACY_UNKNOWN_MODEL_PRICE_ID);
    });
    if ((selectedTotals?.totalTokens ?? 0) > 0) {
      append(LEGACY_UNKNOWN_MODEL_PRICE_ID);
    }
    modelPrices
      .filter((price) => price.source === 'manual')
      .forEach((price) => append(price.modelId, price));
    if (rows.length === 0) {
      DEFAULT_MODEL_PRICES.slice(0, 8).forEach((price) => append(price.modelId, price));
    }
    return rows.sort((left, right) =>
      normalizeModelPriceId(left.modelId).localeCompare(normalizeModelPriceId(right.modelId)),
    );
  }, [
    modelIds,
    modelPrices,
    pricesByModelId,
    selectedModelStats,
    selectedTotals?.totalTokens,
    selectedUsageEvents,
  ]);

  const costByApiKeyId = useMemo(() => {
    const next = new Map<string, CostEstimate>();
    selectedStatsWindow?.apiKeys.forEach((apiKeyStats) => {
      next.set(
        apiKeyStats.apiKeyId,
        estimateModelStatsListCost(apiKeyStats.models, pricesByModelId, apiKeyStats.usage),
      );
    });
    const grouped = new Map<string, CodexLocalAccessUsageEvent[]>();
    selectedUsageEvents.forEach((event) => {
      if (!event.apiKeyId) return;
      const current = grouped.get(event.apiKeyId) ?? [];
      current.push(event);
      grouped.set(event.apiKeyId, current);
    });
    grouped.forEach((events, apiKeyId) => {
      if (!next.has(apiKeyId)) {
        next.set(apiKeyId, estimateEventsCost(events, pricesByModelId));
      }
    });
    return next;
  }, [pricesByModelId, selectedStatsWindow?.apiKeys, selectedUsageEvents]);

  const costByAccountId = useMemo(() => {
    const grouped = new Map<string, CodexLocalAccessUsageEvent[]>();
    selectedUsageEvents.forEach((event) => {
      if (!event.accountId) return;
      const current = grouped.get(event.accountId) ?? [];
      current.push(event);
      grouped.set(event.accountId, current);
    });
    const next = new Map<string, CostEstimate>();
    grouped.forEach((events, accountId) => next.set(accountId, estimateEventsCost(events, pricesByModelId)));
    return next;
  }, [pricesByModelId, selectedUsageEvents]);

  const selectedTotalCost = useMemo(() => {
    if (selectedModelStats.length > 0) {
      return estimateModelStatsListCost(selectedModelStats, pricesByModelId, selectedTotals);
    }
    if (selectedUsageEvents.length > 0) {
      return estimateEventsCost(selectedUsageEvents, pricesByModelId);
    }
    return estimateUsageStatsCost(selectedTotals, pricesByModelId);
  }, [pricesByModelId, selectedModelStats, selectedTotals, selectedUsageEvents]);

  const summaryStats = useMemo(
    () => [
      {
        key: 'requests',
        label: t('codex.localAccess.stats.requests', '总请求数'),
        value: formatCompactNumber(selectedTotals?.requestCount ?? 0),
        detail: t('codex.localAccess.stats.requestsDetail', {
          success: formatCompactNumber(selectedTotals?.successCount ?? 0),
          failed: formatCompactNumber(selectedTotals?.failureCount ?? 0),
          defaultValue: '成功 {{success}} / 失败 {{failed}}',
        }),
      },
      {
        key: 'tokens',
        label: t('codex.localAccess.stats.tokens', '总 Token 数'),
        value: formatCompactNumber(selectedTotals?.totalTokens ?? 0),
        detail: t('codex.localAccess.stats.tokensDetail', {
          input: formatCompactNumber(selectedTotals?.inputTokens ?? 0),
          output: formatCompactNumber(selectedTotals?.outputTokens ?? 0),
          defaultValue: '输入 {{input}} / 输出 {{output}}',
        }),
      },
      {
        key: 'specialTokens',
        label: t('codex.localAccess.stats.specialTokens', '缓存 / 思考'),
        value: formatCompactNumber(
          (selectedTotals?.cachedTokens ?? 0) + (selectedTotals?.reasoningTokens ?? 0),
        ),
        detail: t('codex.localAccess.stats.specialTokensDetail', {
          cached: formatCompactNumber(selectedTotals?.cachedTokens ?? 0),
          reasoning: formatCompactNumber(selectedTotals?.reasoningTokens ?? 0),
          defaultValue: '缓存 {{cached}} / 思考 {{reasoning}}',
        }),
      },
      {
        key: 'cost',
        label: t('codex.localAccess.stats.totalCost', '总费用'),
        value: formatCostEstimate(selectedTotalCost),
        detail:
          selectedTotalCost.unknownModelIds.length > 0
            ? t('codex.localAccess.pricing.unknownModels', {
                models: selectedTotalCost.unknownModelIds.join(', '),
                defaultValue: '未配置价格: {{models}}',
              })
            : t('codex.localAccess.pricing.estimatedWithCurrentPrices', '按当前价格估算'),
      },
      {
        key: 'latency',
        label: t('codex.localAccess.stats.avgLatency', '平均延迟'),
        value: formatLatencyMs(avgLatencyMs),
        detail: t('codex.localAccess.stats.successRate', {
          rate: successRate,
          defaultValue: '成功率 {{rate}}%',
        }),
      },
    ],
    [avgLatencyMs, selectedTotals, selectedTotalCost, successRate, t],
  );

  const currentMemberStats = useMemo(() => {
    const currentIds = collection?.accountIds ?? [];
    return currentIds
      .map((accountId) => {
        const account = upstreamAccounts.find((item) => item.id === accountId);
        if (!account) return null;
        const presentation = buildCodexAccountPresentation(account, t);
        const accountStats = windowStatsByAccountId.get(account.id);
        return {
          account,
          presentation,
          stats: accountStats?.usage ?? null,
        };
      })
      .filter((item): item is NonNullable<typeof item> => Boolean(item))
      .sort((left, right) => {
        const rightCount = right.stats?.requestCount ?? 0;
        const leftCount = left.stats?.requestCount ?? 0;
        return rightCount - leftCount;
      });
  }, [collection?.accountIds, upstreamAccounts, t, windowStatsByAccountId]);

  const routingStrategyOptions = useMemo(
    () => [
      {
        value: 'auto',
        label: t('codex.localAccess.routingStrategy.auto', '自动（推荐）'),
      },
      {
        value: 'quota_high_first',
        label: t('codex.localAccess.routingStrategy.quotaHighFirst', '优先高配额'),
      },
      {
        value: 'quota_low_first',
        label: t('codex.localAccess.routingStrategy.quotaLowFirst', '优先低配额'),
      },
      {
        value: 'plan_high_first',
        label: t('codex.localAccess.routingStrategy.planHighFirst', '优先高订阅'),
      },
      {
        value: 'plan_low_first',
        label: t('codex.localAccess.routingStrategy.planLowFirst', '优先低订阅'),
      },
      {
        value: 'expiry_soon_first',
        label: t('codex.localAccess.routingStrategy.expirySoonFirst', '优先近到期'),
      },
    ] satisfies Array<{ value: CodexLocalAccessRoutingStrategy; label: string }>,
    [t],
  );

  const renderQuotaPreview = (
    presentation: ReturnType<typeof buildCodexAccountPresentation>,
    limit = 2,
  ) => {
    const quotaLines = buildQuotaPreviewLines(presentation.quotaItems, limit);
    if (quotaLines.length === 0) {
      return null;
    }

    return (
      <div className="codex-local-access-quota-line">
        {quotaLines.map((line) => (
          <span
            key={line.key}
            className={`codex-local-access-quota-chip ${line.quotaClass}`}
            title={line.title}
          >
            <span className="codex-local-access-quota-dot" />
            <span>{line.text}</span>
          </span>
        ))}
      </div>
    );
  };

  const renderModelStatsTable = (
    models: CodexLocalAccessModelStats[],
    showAll: boolean,
    onToggleShowAll: () => void,
  ) => {
    const sortedModels = sortModelStats(models);
    const visibleModels = showAll
      ? sortedModels
      : sortedModels.slice(0, MODEL_STATS_COLLAPSED_LIMIT);

    if (sortedModels.length === 0) {
      return (
        <div className="codex-local-access-model-stats-empty">
          {t('codex.localAccess.stats.modelStatsEmpty', '当前范围暂无模型统计')}
        </div>
      );
    }

    return (
      <div className="codex-local-access-model-stats">
        <div className="codex-local-access-model-stats-head">
          <span>{t('codex.localAccess.stats.model', '模型')}</span>
          <span>{t('codex.localAccess.stats.requestsShort', '请求')}</span>
          <span>{t('codex.localAccess.stats.totalTokensShort', '总 Token')}</span>
          <span>{t('codex.localAccess.stats.inputOutputShort', '输入 / 输出')}</span>
          <span>{t('codex.localAccess.stats.specialTokensShort', '缓存 / 思考')}</span>
          <span>{t('codex.localAccess.stats.costShort', '费用')}</span>
        </div>
        {visibleModels.map((modelStats) => {
          const usage = modelStats.usage;
          const cost = estimateModelStatsCost(modelStats, pricesByModelId);
          const costTitle =
            cost.unknownModelIds.length > 0
              ? t('codex.localAccess.pricing.unknownModels', {
                  models: cost.unknownModelIds.join(', '),
                  defaultValue: '未配置价格: {{models}}',
                })
              : undefined;
          return (
            <div key={modelStats.modelId || LEGACY_UNKNOWN_MODEL_PRICE_ID} className="codex-local-access-model-stats-row">
              <code title={modelStats.modelId || LEGACY_UNKNOWN_MODEL_PRICE_ID}>
                {modelStats.modelId || LEGACY_UNKNOWN_MODEL_PRICE_ID}
              </code>
              <span>{formatCompactNumber(usage?.requestCount ?? 0)}</span>
              <span>{formatCompactNumber(usage?.totalTokens ?? 0)}</span>
              <span>
                {formatCompactNumber(usage?.inputTokens ?? 0)} /{' '}
                {formatCompactNumber(usage?.outputTokens ?? 0)}
              </span>
              <span>
                {formatCompactNumber(usage?.cachedTokens ?? 0)} /{' '}
                {formatCompactNumber(usage?.reasoningTokens ?? 0)}
              </span>
              <span title={costTitle}>{formatCostEstimate(cost)}</span>
            </div>
          );
        })}
        {sortedModels.length > MODEL_STATS_COLLAPSED_LIMIT && (
          <button
            type="button"
            className="codex-local-access-model-stats-more"
            onClick={onToggleShowAll}
          >
            {showAll
              ? t('codex.localAccess.stats.showLessModels', '收起')
              : t('codex.localAccess.stats.showAllModels', {
                  count: sortedModels.length,
                  defaultValue: '显示全部 {{count}} 个模型',
                })}
          </button>
        )}
      </div>
    );
  };

  const upstreamAccountById = useMemo(
    () => new Map(upstreamAccounts.map((account) => [account.id, account])),
    [upstreamAccounts],
  );

  const handleCopy = async (field: CopyableField, value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setCopiedField(field);
      window.setTimeout(
        () => setCopiedField((current) => (current === field ? null : current)),
        1200,
      );
    } catch (err) {
      setError(t('common.shared.export.copyFailed', '复制失败，请手动复制'));
      console.error('Failed to copy local access value:', err);
    }
  };

  const runAction = async (task: () => Promise<void>, successText: string) => {
    setError('');
    setNotice('');
    try {
      await task();
      setNotice(successText);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleSyncOpenAiPricing = async () => {
    setPricingSyncing(true);
    setPricingError('');
    setError('');
    setNotice('');
    try {
      const markdown = await fetchOpenAiPricingMarkdown();
      const parsedPrices = parseOpenAiPricingMarkdown(markdown);
      if (parsedPrices.length === 0) {
        throw new Error(t('codex.localAccess.pricing.syncNoPrices', '未从官方价格页解析到模型价格'));
      }
      setModelPrices((prev) => mergeModelPrices(prev, parsedPrices));
      setNotice(
        t('codex.localAccess.pricing.syncSuccess', {
          count: parsedPrices.length,
          defaultValue: '已同步 {{count}} 个官方模型价格',
        }),
      );
    } catch (err) {
      setPricingError(err instanceof Error ? err.message : String(err));
    } finally {
      setPricingSyncing(false);
    }
  };

  const handleResetModelPrices = () => {
    setPricingError('');
    setModelPrices([...DEFAULT_MODEL_PRICES]);
    setNotice(t('codex.localAccess.pricing.resetSuccess', '模型价格已重置为内置默认值'));
  };

  const handleOpenPricingSource = async () => {
    try {
      await openUrl(OPENAI_PRICING_SOURCE_URL);
    } catch (err) {
      setPricingError(err instanceof Error ? err.message : String(err));
    }
  };

  const updateModelPrice = (
    modelId: string,
    field: ModelPriceField,
    rawValue: string,
  ) => {
    const nextValue = normalizeNullablePrice(rawValue);
    const normalized = normalizeModelPriceId(modelId);
    if (!normalized) return;
    setModelPrices((prev) => {
      const map = new Map(prev.map((price) => [normalizeModelPriceId(price.modelId), price]));
      const current = map.get(normalized) ?? {
        modelId,
        inputUsdPerMillion: null,
        cachedInputUsdPerMillion: null,
        outputUsdPerMillion: null,
        source: 'manual' as const,
        updatedAt: 0,
      };
      map.set(normalized, {
        ...current,
        [field]: nextValue,
        source: 'manual',
        updatedAt: Date.now(),
      });
      return Array.from(map.values()).sort((left, right) =>
        normalizeModelPriceId(left.modelId).localeCompare(normalizeModelPriceId(right.modelId)),
      );
    });
  };

  const toggleSelectAllVisible = () => {
    if (actionBusy || visibleSelectableAccounts.length === 0) return;
    setSelected((prev) => {
      const next = new Set(prev);
      if (allVisibleSelected) {
        for (const account of visibleSelectableAccounts) {
          next.delete(account.id);
        }
      } else {
        for (const account of visibleSelectableAccounts) {
          next.add(account.id);
        }
      }
      return next;
    });
  };

  const handleToggleRestrictFreeAccounts = async () => {
    if (actionBusy) return;
    setRestrictFreeAccounts((prev) => !prev);
  };

  const toggleSelect = (accountId: string) => {
    if (actionBusy) return;
    const account = upstreamAccountById.get(accountId);
    if (!account) return;
    setSelected((prev) => {
      const source = upstreamSourceByAccountId.get(accountId);
      if (source && !source.eligible && !prev.has(accountId)) {
        return prev;
      }
      const isFreeAccount =
        !isCodexApiKeyAccount(account) && isCodexExplicitFreePlanType(account.plan_type);
      if (isFreeAccount && restrictFreeAccounts && !prev.has(accountId)) {
        return prev;
      }
      const next = new Set(prev);
      if (next.has(accountId)) {
        next.delete(accountId);
      } else {
        next.add(accountId);
      }
      return next;
    });
  };

  const handleSaveMembers = async () => {
    setError('');
    setNotice('');
    try {
      const filtered = Array.from(selected).filter((accountId) => {
        const account = upstreamAccountById.get(accountId);
        if (!account) return false;
        const source = upstreamSourceByAccountId.get(accountId);
        if (source && !source.eligible) return false;
        if (
          !isCodexApiKeyAccount(account) &&
          restrictFreeAccounts &&
          isCodexExplicitFreePlanType(account.plan_type)
        ) {
          return false;
        }
        return true;
      });
      await onSaveAccounts({
        accountIds: filtered,
        restrictFreeAccounts,
      });
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleSavePort = async () => {
    const nextPort = Number(portInput.trim());
    if (!Number.isInteger(nextPort) || nextPort <= 0 || nextPort > 65535) {
      setError(t('codex.localAccess.portInvalid', '请输入 1 到 65535 之间的端口'));
      return;
    }

    await runAction(
      async () => {
        await onUpdatePort(nextPort);
      },
      t('codex.localAccess.portSaveSuccess', 'API 服务端口已更新'),
    );
  };

  const handleChangeRoutingStrategy = async (nextStrategy: string) => {
    if (!collection) return;
    if (nextStrategy === routingStrategy) return;

    await runAction(
      async () => {
        await onUpdateRoutingStrategy(nextStrategy as CodexLocalAccessRoutingStrategy);
      },
      t('codex.localAccess.routingSaveSuccess', 'API 服务调度策略已更新'),
    );
  };

  const parseMonthlyTokenLimit = (value: string): number | null => {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
      throw new Error(
        t('codex.localAccess.apiKeyLimitInvalid', '请输入大于 0 的整数 Token 上限'),
      );
    }
    return parsed;
  };

  const normalizeAllowedAccountIds = (accountIds: string[]): string[] => {
    const selectedIds = new Set(accountIds);
    return currentMemberAccounts
      .map((account) => account.id)
      .filter((accountId) => selectedIds.has(accountId));
  };

  const updateApiKeyDraft = (apiKeyId: string, patch: Partial<ApiKeyDraft>) => {
    setApiKeyDrafts((prev) => {
      const apiKey = apiKeys.find((item) => item.id === apiKeyId);
      const current = prev[apiKeyId] ?? (apiKey ? draftFromApiKey(apiKey) : null);
      if (!current) return prev;
      return {
        ...prev,
        [apiKeyId]: {
          ...current,
          ...patch,
        },
      };
    });
  };

  const toggleApiKeyAllowedAccount = (apiKeyId: string, accountId: string) => {
    if (!currentMemberAccountIdSet.has(accountId)) return;
    setApiKeyDrafts((prev) => {
      const apiKey = apiKeys.find((item) => item.id === apiKeyId);
      const current = prev[apiKeyId] ?? (apiKey ? draftFromApiKey(apiKey) : null);
      if (!current) return prev;
      const selectedIds = new Set(normalizeAllowedAccountIds(current.allowedAccountIds));
      if (selectedIds.has(accountId)) {
        selectedIds.delete(accountId);
      } else {
        selectedIds.add(accountId);
      }
      return {
        ...prev,
        [apiKeyId]: {
          ...current,
          upstreamScope: 'selected',
          allowedAccountIds: normalizeAllowedAccountIds(Array.from(selectedIds)),
        },
      };
    });
  };

  const isApiKeyDraftDirty = (apiKey: CodexLocalAccessApiKey): boolean => {
    const draft = apiKeyDrafts[apiKey.id];
    if (!draft) return false;
    const currentScope = apiKey.allowedAccountIds == null ? 'all' : 'selected';
    const currentAllowedAccountIds =
      apiKey.allowedAccountIds == null
        ? []
        : normalizeAllowedAccountIds(apiKey.allowedAccountIds);
    const draftAllowedAccountIds =
      draft.upstreamScope === 'all' ? [] : normalizeAllowedAccountIds(draft.allowedAccountIds);
    return (
      draft.name !== apiKey.name ||
      draft.enabled !== apiKey.enabled ||
      draft.monthlyTokenLimit !== (apiKey.monthlyTokenLimit ? String(apiKey.monthlyTokenLimit) : '') ||
      draft.upstreamScope !== currentScope ||
      !areStringArraysEqual(draftAllowedAccountIds, currentAllowedAccountIds)
    );
  };

  const handleCreateApiKey = async () => {
    await runAction(
      async () => {
        await onCreateApiKey({
          name: newApiKeyName.trim() || t('codex.localAccess.apiKeyDefaultName', 'API Key'),
          monthlyTokenLimit: parseMonthlyTokenLimit(newApiKeyLimit),
          upstreamScope: 'all',
          allowedAccountIds: [],
        });
        setNewApiKeyName('');
        setNewApiKeyLimit('');
      },
      t('codex.localAccess.apiKeyCreateSuccess', 'API 服务密钥已创建'),
    );
  };

  const handleSaveApiKey = async (apiKey: CodexLocalAccessApiKey) => {
    const draft = apiKeyDrafts[apiKey.id] ?? draftFromApiKey(apiKey);
    await runAction(
      async () => {
        await onUpdateApiKey(apiKey.id, {
          name: draft.name.trim() || apiKey.name,
          enabled: draft.enabled,
          monthlyTokenLimit: parseMonthlyTokenLimit(draft.monthlyTokenLimit),
          upstreamScope: draft.upstreamScope,
          allowedAccountIds:
            draft.upstreamScope === 'all'
              ? []
              : normalizeAllowedAccountIds(draft.allowedAccountIds),
        });
      },
      t('codex.localAccess.apiKeyUpdateSuccess', 'API 服务密钥已更新'),
    );
  };

  const handleSetDefaultApiKey = async (apiKey: CodexLocalAccessApiKey) => {
    await runAction(
      async () => {
        await onSetDefaultApiKey(apiKey.id);
      },
      t('codex.localAccess.defaultApiKeyUpdateSuccess', 'API 服务默认密钥已更新'),
    );
  };

  const toggleApiKeyVisible = (apiKeyId: string) => {
    setVisibleApiKeyIds((prev) => {
      const next = new Set(prev);
      if (next.has(apiKeyId)) {
        next.delete(apiKeyId);
      } else {
        next.add(apiKeyId);
      }
      return next;
    });
  };

  const handleResetKey = async (apiKey: CodexLocalAccessApiKey) => {
    const confirmed = await confirmDialog(
      t(
        'codex.localAccess.rotateConfirmMessage',
        '重置后当前 API 服务密钥会立即失效，正在进行中的请求可能不可用。确认继续吗？',
      ),
      {
        title: t('codex.localAccess.rotateKey', '重置密钥'),
        kind: 'warning',
        okLabel: t('common.confirm'),
        cancelLabel: t('common.cancel'),
      },
    );

    if (!confirmed) {
      return;
    }

    await runAction(
      async () => {
        await onRotateApiKey(apiKey.id);
        setVisibleApiKeyIds((prev) => new Set(prev).add(apiKey.id));
      },
      t('codex.localAccess.rotateSuccess', 'API 服务密钥已重置'),
    );
  };

  const handleDeleteApiKey = async (apiKey: CodexLocalAccessApiKey) => {
    const confirmed = await confirmDialog(
      t('codex.localAccess.apiKeyDeleteConfirm', {
        name: apiKey.name,
        defaultValue: '确定要删除 API 服务密钥 {{name}} 吗？删除后该密钥会立即失效。',
      }),
      {
        title: t('codex.localAccess.apiKeyDelete', '删除密钥'),
        kind: 'warning',
        okLabel: t('common.delete'),
        cancelLabel: t('common.cancel'),
      },
    );

    if (!confirmed) {
      return;
    }

    await runAction(
      async () => {
        await onDeleteApiKey(apiKey.id);
      },
      t('codex.localAccess.apiKeyDeleteSuccess', 'API 服务密钥已删除'),
    );
  };

  const handleClearStats = async () => {
    const confirmed = await confirmDialog(
      t('codex.localAccess.clearStatsConfirm', '确定要清空 API 服务统计吗？'),
      {
        title: t('codex.localAccess.clearStats', '清除统计'),
        kind: 'warning',
        okLabel: t('common.confirm'),
        cancelLabel: t('common.cancel'),
      },
    );

    if (!confirmed) {
      return;
    }

    await runAction(async () => {
      await onClearStats();
    }, t('codex.localAccess.clearStatsSuccess', 'API 服务统计已清空'));
  };

  const handleKillPort = async () => {
    await runAction(
      async () => {
        await onKillPort();
      },
      t('codex.localAccess.killPortSuccessUnknown', 'API 服务端口已清理'),
    );
  };

  const handleRefreshStats = async () => {
    setError('');
    setNotice('');
    try {
      await onRefreshStats();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleToggleEnabled = async () => {
    await runAction(
      async () => {
        await onToggleEnabled();
      },
      collection?.enabled
        ? t('codex.localAccess.disabledSuccess', 'API 服务已停用')
        : t('codex.localAccess.enabledSuccess', 'API 服务已启用'),
    );
  };

  const handleTest = async () => {
    setError('');
    setNotice('');
    try {
      const modelCount = await onTest();
      setNotice(
        t('codex.localAccess.testSuccess', {
          count: modelCount,
          defaultValue:
            modelCount > 0 ? 'API 服务测试成功（{{count}} 个模型）' : 'API 服务测试成功',
        }),
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  if (!isOpen) return null;
  const isMembersMode = mode === 'members';

  return (
    <div
      className={`modal-overlay codex-local-access-modal-overlay${
        isMembersMode ? '' : ' codex-local-access-modal-overlay-panel'
      }`}
      onClick={onClose}
    >
      <div
        className={`modal codex-local-access-modal${
          isMembersMode
            ? ' codex-local-access-modal-members group-account-picker-modal'
            : ' codex-local-access-modal-panel'
        }`}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="modal-header codex-local-access-modal-header">
          <div className="codex-local-access-header-main">
            <h2 className="group-account-picker-title">
              <Server size={18} />
              <span>
                {isMembersMode
                  ? t('codex.localAccess.entryAction', '添加至 API 服务')
                  : t('codex.localAccess.title', 'API 服务')}
              </span>
            </h2>
            {!isMembersMode && (
              <div className="codex-local-access-header-meta">
                <div className="codex-local-access-header-badges">
                  <span
                    className={`codex-local-access-status ${
                      state?.running ? 'running' : 'stopped'
                    }`}
                  >
                    {collection?.enabled
                      ? state?.running
                        ? t('codex.localAccess.statusRunning', '运行中')
                        : t('codex.localAccess.statusStopped', '未运行')
                      : t('codex.localAccess.statusDisabled', '已停用')}
                  </span>
                  <span className="codex-local-access-subtle-badge">
                    {t('codex.localAccess.memberOnlyLocal', '本机/局域网')}
                  </span>
                </div>
                <div className="codex-local-access-header-tools">
                  <button
                    type="button"
                    className="folder-icon-btn codex-local-access-toolbar-btn"
                    onClick={() => void handleRefreshStats()}
                    disabled={!collection || actionBusy}
                    title={t('codex.localAccess.refreshStats', '刷新统计')}
                    aria-label={t('codex.localAccess.refreshStats', '刷新统计')}
                  >
                    <RefreshCw size={14} className={saving ? 'loading-spinner' : ''} />
                  </button>
                  {collection && (
                    <div className="codex-local-access-header-routing">
                      <SingleSelectDropdown
                        value={routingStrategy}
                        options={routingStrategyOptions}
                        onChange={(value) => void handleChangeRoutingStrategy(value)}
                        disabled={saving || testing || starting}
                        ariaLabel={t('codex.localAccess.routingLabel', '调度策略')}
                      />
                    </div>
                  )}
                  <button
                    type="button"
                    className="folder-icon-btn codex-local-access-toolbar-btn"
                    onClick={() => void handleTest()}
                    disabled={!collection || testing || saving}
                    title={t('codex.localAccess.testAction', '测试 API 服务')}
                    aria-label={t('codex.localAccess.testAction', '测试 API 服务')}
                  >
                    <ShieldCheck size={14} className={testing ? 'loading-spinner' : ''} />
                  </button>
                  <button
                    type="button"
                    className={`folder-icon-btn codex-local-access-toolbar-btn ${
                      collection?.enabled ? 'is-danger' : 'is-primary'
                    }`}
                    onClick={() => void handleToggleEnabled()}
                    disabled={!collection || saving || testing || starting}
                    title={
                      collection?.enabled
                        ? t('codex.localAccess.disableService', '停用服务')
                        : t('codex.localAccess.enableService', '启用服务')
                    }
                    aria-label={
                      collection?.enabled
                        ? t('codex.localAccess.disableService', '停用服务')
                        : t('codex.localAccess.enableService', '启用服务')
                    }
                  >
                    <Power size={14} />
                  </button>
                </div>
              </div>
            )}
          </div>
          <button
            className="modal-close codex-local-access-close"
            onClick={onClose}
            aria-label={t('common.close')}
          >
            <X size={18} />
          </button>
        </div>

        <div className="modal-body codex-local-access-modal-body">
          {state?.lastError && (
            <div className="codex-local-access-inline-error codex-local-access-inline-error-with-action">
              <CircleAlert size={14} />
              <span>{state.lastError}</span>
              {collection && (
                <button
                  type="button"
                  className="btn btn-secondary btn-sm codex-local-access-inline-action"
                  onClick={() => void handleKillPort()}
                  disabled={actionBusy}
                >
                  {portCleanupBusy ? (
                    <RefreshCw size={14} className="loading-spinner" />
                  ) : (
                    <Wrench size={14} />
                  )}
                  {t('codex.localAccess.killPortAction', '清理端口')}
                </button>
              )}
            </div>
          )}

          {error && (
            <div className="codex-local-access-inline-error">
              <CircleAlert size={14} />
              <span>{error}</span>
            </div>
          )}

          {notice && (
            <div className="codex-local-access-inline-success">
              <Check size={14} />
              <span>{notice}</span>
            </div>
          )}

          {!isMembersMode && (
            <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-summary-block">
              <div className="codex-local-access-summary-head">
                <div className="codex-local-access-section-title">
                  <Activity size={16} />
                  <span>{t('codex.localAccess.statsTitle', '总量统计')}</span>
                </div>
                <div className="codex-local-access-summary-actions">
                  <div
                    className="codex-local-access-stats-range-tabs"
                    role="tablist"
                    aria-label={t('codex.localAccess.statsRange.label', '统计范围')}
                  >
                    {statsRangeOptions.map((option) => (
                      <button
                        key={option.key}
                        type="button"
                        role="tab"
                        className={`codex-local-access-stats-range-tab${
                          statsRange === option.key ? ' is-active' : ''
                        }`}
                        aria-selected={statsRange === option.key}
                        onClick={() => setStatsRange(option.key)}
                        disabled={actionBusy}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                  <button
                    type="button"
                    className="btn btn-danger btn-sm"
                    onClick={() => void handleClearStats()}
                    disabled={!collection || actionBusy}
                    title={t('codex.localAccess.clearStats', '清除统计')}
                    aria-label={t('codex.localAccess.clearStats', '清除统计')}
                  >
                    <Trash2 size={14} />
                    {t('codex.localAccess.clearStats', '清除统计')}
                  </button>
                </div>
              </div>
              <div className="codex-local-access-stats-grid">
                {summaryStats.map((item) => (
                  <div
                    key={item.key}
                    className={`codex-local-access-stat-card codex-local-access-stat-card-${item.key}`}
                  >
                    <span className="codex-local-access-stat-label">{item.label}</span>
                    <strong>{item.value}</strong>
                    <span className="codex-local-access-stat-sub">{item.detail}</span>
                  </div>
                ))}
              </div>
              <div className="codex-local-access-model-stats-panel">
                <button
                  type="button"
                  className="codex-local-access-model-stats-toggle"
                  onClick={() => setTotalModelsExpanded((current) => !current)}
                  aria-expanded={totalModelsExpanded}
                >
                  {totalModelsExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                  <span>
                    {t('codex.localAccess.stats.modelStatsTitle', {
                      count: selectedModelStats.length,
                      defaultValue: '按模型统计 {{count}}',
                    })}
                  </span>
                </button>
                {totalModelsExpanded &&
                  renderModelStatsTable(selectedModelStats, showAllTotalModels, () =>
                    setShowAllTotalModels((current) => !current),
                  )}
              </div>
              {currentQuotaPoolSummary.visiblePlans.length > 0 && (
                <div
                  className="codex-local-access-quota-pool-grid"
                  aria-label={quotaPoolLabels.title}
                >
                  {currentQuotaPoolSummary.visiblePlans.map((item) => (
                    <div key={item.key} className="codex-local-access-quota-pool-card">
                      <span className="codex-local-access-quota-pool-plan">
                        {item.key} ({item.count})
                      </span>
                      <span className="codex-local-access-quota-pool-value">
                        {quotaPoolLabels.hourly} {formatCodexQuotaPoolPercent(item.hourly)}
                      </span>
                      <span className="codex-local-access-quota-pool-value">
                        {quotaPoolLabels.weekly} {formatCodexQuotaPoolPercent(item.weekly)}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </section>
          )}

          {!isMembersMode && (
            <div className="codex-local-access-panel-grid">
              <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-config-section">
                <div className="codex-local-access-section-title">
                  <KeyRound size={16} />
                  <span>{t('codex.localAccess.configTitle', '服务配置')}</span>
                </div>
                {collection ? (
                  <div className="codex-local-access-config-grid">
                    <div className="codex-local-access-config-card codex-local-access-config-card-base">
                      <div className="codex-local-access-config-head">
                        <span className="codex-local-access-config-label">
                          {t('codex.localAccess.baseUrl', '地址')}
                        </span>
                        <div className="codex-local-access-config-actions">
                          <button
                            type="button"
                            className="folder-icon-btn"
                            onClick={() => void handleCopy('baseUrl', baseUrl)}
                            title={t('common.copy', '复制')}
                          >
                            {copiedField === 'baseUrl' ? <Check size={14} /> : <Copy size={14} />}
                          </button>
                        </div>
                      </div>
                      <code className="codex-local-access-code" title={baseUrl}>
                        {baseUrl}
                      </code>
                    </div>

                    <div className="codex-local-access-config-card codex-local-access-api-key-create-card">
                      <div className="codex-local-access-config-head">
                        <span className="codex-local-access-config-label">
                          {t('codex.localAccess.apiKeysTitle', 'API Keys')}
                        </span>
                        <span className="codex-local-access-view-only-badge">
                          {t('codex.localAccess.apiKeysCount', {
                            count: apiKeys.length,
                            defaultValue: '{{count}} 个',
                          })}
                        </span>
                      </div>
                      <div className="codex-local-access-api-key-create-row">
                        <input
                          type="text"
                          value={newApiKeyName}
                          onChange={(event) => setNewApiKeyName(event.target.value)}
                          placeholder={t('codex.localAccess.apiKeyNamePlaceholder', '用户或用途')}
                          disabled={actionBusy}
                        />
                        <input
                          type="number"
                          min={1}
                          value={newApiKeyLimit}
                          onChange={(event) => setNewApiKeyLimit(event.target.value)}
                          placeholder={t('codex.localAccess.apiKeyLimitPlaceholder', '30天 Token 上限')}
                          disabled={actionBusy}
                        />
                        <button
                          type="button"
                          className="btn btn-secondary btn-sm"
                          onClick={() => void handleCreateApiKey()}
                          disabled={actionBusy}
                        >
                          <KeyRound size={14} />
                          {t('codex.localAccess.apiKeyCreate', '新增密钥')}
                        </button>
                      </div>
                    </div>

                    <div className="codex-local-access-api-key-list">
                      {apiKeys.map((apiKey) => {
                        const draft = apiKeyDrafts[apiKey.id] ?? draftFromApiKey(apiKey);
                        const copyField = `apiKey:${apiKey.id}` as const;
                        const visible = visibleApiKeyIds.has(apiKey.id);
                        const monthlyStats = monthlyStatsByApiKeyId.get(apiKey.id)?.usage;
                        const usedTokens = monthlyStats?.totalTokens ?? 0;
                        const limit = apiKey.monthlyTokenLimit;
                        const remaining =
                          limit && limit > 0 ? Math.max(0, limit - usedTokens) : null;
                        const overLimit = Boolean(limit && usedTokens >= limit);
                        const draftAllowedAccountIds = normalizeAllowedAccountIds(draft.allowedAccountIds);
                        const isScopedToSelected = draft.upstreamScope === 'selected';
                        const allowedMemberCount = isScopedToSelected
                          ? draftAllowedAccountIds.length
                          : currentMemberAccounts.length;
                        const selectedAllowedIdSet = new Set(draftAllowedAccountIds);
                        const isDefaultApiKey = apiKey.id === collection.defaultApiKeyId;
                        const canSetDefaultApiKey = apiKey.enabled && apiKey.key.trim().length > 0;

                        return (
                          <div
                            key={apiKey.id}
                            className={`codex-local-access-api-key-row${
                              apiKey.enabled ? '' : ' is-disabled'
                            }${overLimit ? ' is-over-limit' : ''}`}
                          >
                            <div className="codex-local-access-api-key-main">
                              <div className="codex-local-access-api-key-fields">
                                <label>
                                  <span>{t('codex.localAccess.apiKeyName', '名称')}</span>
                                  <input
                                    type="text"
                                    value={draft.name}
                                    onChange={(event) =>
                                      updateApiKeyDraft(apiKey.id, { name: event.target.value })
                                    }
                                    disabled={actionBusy}
                                  />
                                </label>
                                <label>
                                  <span>{t('codex.localAccess.apiKeyMonthlyLimit', '30天 Token 上限')}</span>
                                  <input
                                    type="number"
                                    min={1}
                                    value={draft.monthlyTokenLimit}
                                    onChange={(event) =>
                                      updateApiKeyDraft(apiKey.id, {
                                        monthlyTokenLimit: event.target.value,
                                      })
                                    }
                                    placeholder={t('codex.localAccess.apiKeyUnlimited', '不限')}
                                    disabled={actionBusy}
                                  />
                                </label>
                              </div>
                              <code
                                className="codex-local-access-code codex-local-access-api-key-code"
                                title={visible ? apiKey.key : t('codex.localAccess.apiKeyHiddenHint', 'API Key 已隐藏，点击显示')}
                              >
                                {formatApiKeyValue(apiKey.key, visible)}
                              </code>
                              <div className="codex-local-access-api-key-meta">
                                <span className={apiKey.enabled ? 'enabled' : 'disabled'}>
                                  {apiKey.enabled
                                    ? t('codex.localAccess.apiKeyEnabled', '启用')
                                    : t('codex.localAccess.apiKeyDisabled', '停用')}
                                </span>
                                {isDefaultApiKey && (
                                  <span className="default">
                                    {t('codex.localAccess.defaultApiKeyBadge', '默认')}
                                  </span>
                                )}
                                <span>
                                  {t('codex.localAccess.apiKeyUsedMonthly', {
                                    used: formatCompactNumber(usedTokens),
                                    defaultValue: '近30天 {{used}} Tokens',
                                  })}
                                </span>
                                <span>
                                  {remaining == null
                                    ? t('codex.localAccess.apiKeyUnlimited', '不限')
                                    : t('codex.localAccess.apiKeyRemaining', {
                                        remaining: formatCompactNumber(remaining),
                                        defaultValue: '剩余 {{remaining}}',
                                      })}
                                </span>
                                <span>
                                  {t('codex.localAccess.apiKeyLastUsed', {
                                    time: formatLocalDateTime(apiKey.lastUsedAt),
                                    defaultValue: '最近使用 {{time}}',
                                  })}
                                </span>
                                <span className={isScopedToSelected && allowedMemberCount === 0 ? 'warning' : ''}>
                                  {isScopedToSelected
                                    ? t('codex.localAccess.apiKeyScopedUpstreams', {
                                        count: allowedMemberCount,
                                        total: currentMemberAccounts.length,
                                        defaultValue: '已授权 {{count}}/{{total}}',
                                      })
                                    : t('codex.localAccess.apiKeyAllUpstreams', {
                                        count: currentMemberAccounts.length,
                                        defaultValue: '全部 {{count}} 个上游',
                                      })}
                                </span>
                              </div>
                              <div className="codex-local-access-api-key-upstreams">
                                <div className="codex-local-access-api-key-upstreams-head">
                                  <span>{t('codex.localAccess.apiKeyUpstreamScope', '上游范围')}</span>
                                  <div className="codex-local-access-api-key-scope-toggle">
                                    <button
                                      type="button"
                                      className={draft.upstreamScope === 'all' ? 'is-active' : ''}
                                      onClick={() =>
                                        updateApiKeyDraft(apiKey.id, {
                                          upstreamScope: 'all',
                                          allowedAccountIds: [],
                                        })
                                      }
                                      disabled={actionBusy}
                                    >
                                      {t('codex.localAccess.apiKeyUpstreamAll', '全部集合')}
                                    </button>
                                    <button
                                      type="button"
                                      className={draft.upstreamScope === 'selected' ? 'is-active' : ''}
                                      onClick={() =>
                                        updateApiKeyDraft(apiKey.id, {
                                          upstreamScope: 'selected',
                                          allowedAccountIds: draftAllowedAccountIds,
                                        })
                                      }
                                      disabled={actionBusy}
                                    >
                                      {t('codex.localAccess.apiKeyUpstreamSelected', '指定上游')}
                                    </button>
                                  </div>
                                </div>
                                {isScopedToSelected && (
                                  <div className="codex-local-access-api-key-upstream-list">
                                    {currentMemberAccounts.length === 0 ? (
                                      <div className="group-account-empty">
                                        {t('codex.localAccess.apiKeyUpstreamEmpty', '集合内暂无上游账号')}
                                      </div>
                                    ) : (
                                      currentMemberAccounts.map((account) => {
                                        const presentation = buildCodexAccountPresentation(account, t);
                                        const isRelayAccount = isCodexApiKeyAccount(account);
                                        const source = upstreamSourceByAccountId.get(account.id);
                                        const providerName =
                                          source?.providerName || getApiKeyProviderName(account);
                                        const baseUrlHost =
                                          source?.baseUrlHost || extractUrlHost(account.api_base_url);
                                        return (
                                          <label
                                            key={account.id}
                                            className="codex-local-access-api-key-upstream-item"
                                          >
                                            <input
                                              type="checkbox"
                                              checked={selectedAllowedIdSet.has(account.id)}
                                              onChange={() =>
                                                toggleApiKeyAllowedAccount(apiKey.id, account.id)
                                              }
                                              disabled={actionBusy}
                                            />
                                            <span
                                              className="codex-local-access-api-key-upstream-name"
                                              title={maskAccountText(presentation.displayName)}
                                            >
                                              {maskAccountText(presentation.displayName)}
                                            </span>
                                            <span className={`tier-badge ${presentation.planClass}`}>
                                              {isRelayAccount
                                                ? getAccountSourceLabel(account, t)
                                                : presentation.planLabel}
                                            </span>
                                            {isRelayAccount && (
                                              <span
                                                className="codex-local-access-member-metric"
                                                title={account.api_base_url || providerName}
                                              >
                                                {baseUrlHost || providerName}
                                              </span>
                                            )}
                                          </label>
                                        );
                                      })
                                    )}
                                  </div>
                                )}
                              </div>
                            </div>
                            <div className="codex-local-access-api-key-actions">
                              <label className="codex-local-access-api-key-toggle">
                                <input
                                  type="checkbox"
                                  checked={draft.enabled}
                                  onChange={(event) =>
                                    updateApiKeyDraft(apiKey.id, { enabled: event.target.checked })
                                  }
                                  disabled={actionBusy}
                                />
                                <span>{t('common.enabled', '启用')}</span>
                              </label>
                              {isDefaultApiKey ? (
                                <span
                                  className="codex-local-access-default-key-badge"
                                  title={t(
                                    'codex.localAccess.defaultApiKeyCurrent',
                                    '当前默认密钥',
                                  )}
                                >
                                  <ShieldCheck size={14} />
                                  {t('codex.localAccess.defaultApiKeyBadge', '默认')}
                                </span>
                              ) : (
                                <button
                                  type="button"
                                  className="btn btn-secondary btn-sm codex-local-access-default-key-button"
                                  onClick={() => void handleSetDefaultApiKey(apiKey)}
                                  disabled={actionBusy || !canSetDefaultApiKey}
                                  title={
                                    canSetDefaultApiKey
                                      ? t(
                                          'codex.localAccess.setDefaultApiKey',
                                          '设为默认',
                                        )
                                      : t(
                                          'codex.localAccess.setDefaultApiKeyUnavailable',
                                          '只能将已启用且非空的密钥设为默认',
                                        )
                                  }
                                >
                                  <ShieldCheck size={14} />
                                  {t('codex.localAccess.setDefaultApiKey', '设为默认')}
                                </button>
                              )}
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => toggleApiKeyVisible(apiKey.id)}
                                title={
                                  visible
                                    ? t('codex.localAccess.hideKey', '隐藏密钥')
                                    : t('codex.localAccess.showKey', '显示密钥')
                                }
                              >
                                {visible ? <EyeOff size={14} /> : <Eye size={14} />}
                              </button>
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => void handleCopy(copyField, apiKey.key)}
                                title={t('common.copy', '复制')}
                              >
                                {copiedField === copyField ? <Check size={14} /> : <Copy size={14} />}
                              </button>
                              <button
                                type="button"
                                className="btn btn-secondary btn-sm"
                                onClick={() => void handleSaveApiKey(apiKey)}
                                disabled={actionBusy || !isApiKeyDraftDirty(apiKey)}
                              >
                                <Check size={14} />
                                {t('common.save', '保存')}
                              </button>
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => void handleResetKey(apiKey)}
                                disabled={actionBusy}
                                title={t('codex.localAccess.rotateKey', '重置密钥')}
                              >
                                <RefreshCw size={14} className={saving ? 'loading-spinner' : ''} />
                              </button>
                              <button
                                type="button"
                                className="folder-icon-btn codex-local-access-danger-icon"
                                onClick={() => void handleDeleteApiKey(apiKey)}
                                disabled={actionBusy || apiKeys.length <= 1}
                                title={t('codex.localAccess.apiKeyDelete', '删除密钥')}
                              >
                                <Trash2 size={14} />
                              </button>
                            </div>
                          </div>
                        );
                      })}
                    </div>

                    <div className="codex-local-access-config-card codex-local-access-config-card-port codex-local-access-port-card">
                      <div className="codex-local-access-config-head">
                        <label
                          className="codex-local-access-config-label"
                          htmlFor="codex-local-access-port"
                        >
                          {t('codex.localAccess.portLabel', '服务端口')}
                        </label>
                        <div className="codex-local-access-config-actions">
                          <button
                            type="button"
                            className="btn btn-secondary btn-sm"
                            onClick={() => void handleSavePort()}
                            disabled={saving || testing || starting}
                          >
                            {saving ? (
                              <RefreshCw size={14} className="loading-spinner" />
                            ) : (
                              <Gauge size={14} />
                            )}
                            {t('codex.localAccess.portSave', '保存端口')}
                          </button>
                        </div>
                      </div>
                      <div className="codex-local-access-port-row">
                        <input
                          id="codex-local-access-port"
                          type="number"
                          min={1}
                          max={65535}
                          value={portInput}
                          onChange={(event) => setPortInput(event.target.value)}
                          disabled={saving || testing || starting}
                        />
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="group-account-empty">
                    {t(
                      'codex.localAccess.configEmpty',
                      '先把账号保存到 API 服务集合，随后会自动生成地址、密钥和端口。',
                    )}
                  </div>
                )}
                {collection || modelIdOptions.length > 0 ? (
                  <div className="codex-local-access-config-extra-grid">
                    {collection ? (
                      <div className="codex-local-access-config-card codex-local-access-config-card-root">
                        <div className="codex-local-access-config-head">
                          <span className="codex-local-access-config-label">
                            {t('codex.localAccess.apiPortUrl', 'API端口URL')}
                          </span>
                          <div className="codex-local-access-config-actions">
                            <button
                              type="button"
                              className="folder-icon-btn"
                              onClick={() => void handleCopy('apiPortUrl', apiPortUrl)}
                              title={t('common.copy', '复制')}
                            >
                              {copiedField === 'apiPortUrl' ? <Check size={14} /> : <Copy size={14} />}
                            </button>
                          </div>
                        </div>
                        <code className="codex-local-access-code" title={apiPortUrl}>
                          {apiPortUrl}
                        </code>
                      </div>
                    ) : null}

                    {modelIdOptions.length > 0 ? (
                      <div className="codex-local-access-config-card codex-local-access-config-card-model">
                        <div className="codex-local-access-config-head">
                          <span className="codex-local-access-config-label">
                            {t('codex.localAccess.modelId', '模型 ID')}
                          </span>
                          <span className="codex-local-access-view-only-badge">
                            {t('codex.localAccess.modelIdViewOnly', '仅查看使用，无切换功能')}
                          </span>
                          <div className="codex-local-access-config-actions">
                            <button
                              type="button"
                              className="folder-icon-btn"
                              onClick={() => void handleCopy('modelId', selectedModelId)}
                              title={t('common.copy', '复制')}
                              disabled={!selectedModelId}
                            >
                              {copiedField === 'modelId' ? <Check size={14} /> : <Copy size={14} />}
                            </button>
                          </div>
                        </div>
                        <div className="codex-local-access-model-row">
                          <SingleSelectDropdown
                            value={selectedModelId}
                            options={modelIdOptions}
                            onChange={setSelectedModelId}
                            disabled={modelIdOptions.length === 0}
                            ariaLabel={t('codex.localAccess.modelId', '模型 ID')}
                            placeholder={t('codex.localAccess.modelIdPlaceholder', '选择模型 ID')}
                            menuPlacement="up"
                            menuMaxHeight={240}
                          />
                        </div>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </section>

              <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-pricing-section">
                <div className="codex-local-access-section-head">
                  <div className="codex-local-access-section-title">
                    <DollarSign size={16} />
                    <span>{t('codex.localAccess.pricing.title', '费用估算')}</span>
                  </div>
                  <div className="codex-local-access-pricing-actions">
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => void handleSyncOpenAiPricing()}
                      disabled={pricingSyncing}
                    >
                      <RefreshCw size={14} className={pricingSyncing ? 'loading-spinner' : ''} />
                      {t('codex.localAccess.pricing.sync', '同步官方价格')}
                    </button>
                    <button
                      type="button"
                      className="folder-icon-btn"
                      onClick={() => void handleOpenPricingSource()}
                      title={t('codex.localAccess.pricing.openSource', '打开价格来源')}
                      aria-label={t('codex.localAccess.pricing.openSource', '打开价格来源')}
                    >
                      <ExternalLink size={14} />
                    </button>
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={handleResetModelPrices}
                    >
                      {t('codex.localAccess.pricing.reset', '重置')}
                    </button>
                  </div>
                </div>
                {pricingError && (
                  <div className="codex-local-access-inline-error codex-local-access-pricing-error">
                    <CircleAlert size={14} />
                    <span>{pricingError}</span>
                  </div>
                )}
                <div className="codex-local-access-pricing-table">
                  <div className="codex-local-access-pricing-row codex-local-access-pricing-row-head">
                    <span>{t('codex.localAccess.pricing.model', '模型')}</span>
                    <span>{t('codex.localAccess.pricing.input', '输入 $/1M')}</span>
                    <span>{t('codex.localAccess.pricing.cachedInput', '缓存 $/1M')}</span>
                    <span>{t('codex.localAccess.pricing.output', '输出 $/1M')}</span>
                    <span>{t('codex.localAccess.pricing.source', '来源')}</span>
                  </div>
                  {modelPriceRows.map((price) => {
                    const sourceLabel =
                      price.source === 'openai'
                        ? t('codex.localAccess.pricing.sourceOpenAi', '官方')
                        : price.source === 'builtin'
                          ? t('codex.localAccess.pricing.sourceBuiltin', '内置')
                          : t('codex.localAccess.pricing.sourceManual', '手动');
                    return (
                      <div key={price.modelId} className="codex-local-access-pricing-row">
                        <code title={price.modelId}>{price.modelId}</code>
                        <input
                          type="number"
                          min={0}
                          step="0.0001"
                          value={formatPriceInputValue(price.inputUsdPerMillion)}
                          onChange={(event) =>
                            updateModelPrice(price.modelId, 'inputUsdPerMillion', event.target.value)
                          }
                          aria-label={`${price.modelId} ${t('codex.localAccess.pricing.input', '输入 $/1M')}`}
                        />
                        <input
                          type="number"
                          min={0}
                          step="0.0001"
                          value={formatPriceInputValue(price.cachedInputUsdPerMillion)}
                          onChange={(event) =>
                            updateModelPrice(
                              price.modelId,
                              'cachedInputUsdPerMillion',
                              event.target.value,
                            )
                          }
                          placeholder={t('codex.localAccess.pricing.sameAsInput', '同输入')}
                          aria-label={`${price.modelId} ${t('codex.localAccess.pricing.cachedInput', '缓存 $/1M')}`}
                        />
                        <input
                          type="number"
                          min={0}
                          step="0.0001"
                          value={formatPriceInputValue(price.outputUsdPerMillion)}
                          onChange={(event) =>
                            updateModelPrice(price.modelId, 'outputUsdPerMillion', event.target.value)
                          }
                          aria-label={`${price.modelId} ${t('codex.localAccess.pricing.output', '输出 $/1M')}`}
                        />
                        <span className={`codex-local-access-pricing-source is-${price.source}`}>
                          {sourceLabel}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </section>

              <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-key-stats-section">
                <div className="codex-local-access-section-title">
                  <KeyRound size={16} />
                  <span>{t('codex.localAccess.apiKeyStatsTitle', '按密钥统计')}</span>
                </div>
                <div className="codex-local-access-account-stats">
                  {apiKeys.length === 0 ? (
                    <div className="group-account-empty">
                      {t('codex.localAccess.apiKeyEmpty', '当前还没有 API Key')}
                    </div>
                  ) : (
                    apiKeys.map((apiKey) => {
                      const windowApiKeyStats = windowStatsByApiKeyId.get(apiKey.id);
                      const keyStats = windowApiKeyStats?.usage;
                      const keyModelStats = windowApiKeyStats?.models ?? [];
                      const keyCost =
                        costByApiKeyId.get(apiKey.id) ??
                        estimateModelStatsListCost(keyModelStats, pricesByModelId, keyStats);
                      const monthlyStats = monthlyStatsByApiKeyId.get(apiKey.id)?.usage;
                      const usedTokens = monthlyStats?.totalTokens ?? 0;
                      const limit = apiKey.monthlyTokenLimit;
                      const remaining =
                        limit && limit > 0 ? Math.max(0, limit - usedTokens) : null;
                      const overLimit = Boolean(limit && usedTokens >= limit);
                      const modelsExpanded = expandedApiKeyModelIds.has(apiKey.id);
                      const showAllKeyModels = showAllApiKeyModelIds.has(apiKey.id);

                      return (
                        <div
                          key={apiKey.id}
                          className={`codex-local-access-account-stat-row codex-local-access-key-stat-row${
                            apiKey.enabled ? '' : ' is-disabled'
                          }${overLimit ? ' is-over-limit' : ''}`}
                        >
                          <div className="codex-local-access-account-stat-top">
                            <div className="codex-local-access-account-stat-main">
                              <span className="group-account-email" title={apiKey.name}>
                                {apiKey.name}
                              </span>
                              <span className={`tier-badge ${apiKey.enabled ? 'valid' : 'error'}`}>
                                {apiKey.enabled
                                  ? t('codex.localAccess.apiKeyEnabled', '启用')
                                  : t('codex.localAccess.apiKeyDisabled', '停用')}
                              </span>
                            </div>
                            <div className="codex-local-access-account-stat-block codex-local-access-account-stat-block-metrics">
                              <div className="codex-local-access-account-stat-metrics">
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.accountRequestsCompact', {
                                    value: formatCompactNumber(keyStats?.requestCount ?? 0),
                                    defaultValue: '请求 {{value}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.accountResult', {
                                    success: keyStats?.successCount ?? 0,
                                    failed: keyStats?.failureCount ?? 0,
                                    defaultValue: '成功 {{success}} / 失败 {{failed}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.totalTokensCompact', {
                                    value: formatCompactNumber(keyStats?.totalTokens ?? 0),
                                    defaultValue: '总 {{value}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.tokensDetail', {
                                    input: formatCompactNumber(keyStats?.inputTokens ?? 0),
                                    output: formatCompactNumber(keyStats?.outputTokens ?? 0),
                                    defaultValue: '输入 {{input}} / 输出 {{output}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.specialTokensDetail', {
                                    cached: formatCompactNumber(keyStats?.cachedTokens ?? 0),
                                    reasoning: formatCompactNumber(keyStats?.reasoningTokens ?? 0),
                                    defaultValue: '缓存 {{cached}} / 思考 {{reasoning}}',
                                  })}
                                </span>
                                <span
                                  className="codex-local-access-account-stat-pill codex-local-access-cost-pill"
                                  title={
                                    keyCost.unknownModelIds.length > 0
                                      ? t('codex.localAccess.pricing.unknownModels', {
                                          models: keyCost.unknownModelIds.join(', '),
                                          defaultValue: '未配置价格: {{models}}',
                                        })
                                      : undefined
                                  }
                                >
                                  {t('codex.localAccess.pricing.costPill', {
                                    cost: formatCostEstimate(keyCost),
                                    defaultValue: '费用 {{cost}}',
                                  })}
                                </span>
                                <button
                                  type="button"
                                  className="codex-local-access-account-stat-pill codex-local-access-model-pill"
                                  onClick={() => {
                                    setExpandedApiKeyModelIds((current) => {
                                      const next = new Set(current);
                                      if (next.has(apiKey.id)) {
                                        next.delete(apiKey.id);
                                      } else {
                                        next.add(apiKey.id);
                                      }
                                      return next;
                                    });
                                  }}
                                  aria-expanded={modelsExpanded}
                                >
                                  {modelsExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                                  {t('codex.localAccess.stats.modelStatsCompact', {
                                    count: keyModelStats.length,
                                    defaultValue: '模型 {{count}}',
                                  })}
                                </button>
                                <span className="codex-local-access-account-stat-pill">
                                  {remaining == null
                                    ? t('codex.localAccess.apiKeyUnlimited', '不限')
                                    : t('codex.localAccess.apiKeyRemaining', {
                                        remaining: formatCompactNumber(remaining),
                                        defaultValue: '剩余 {{remaining}}',
                                      })}
                                </span>
                              </div>
                            </div>
                          </div>
                          {modelsExpanded && (
                            <div className="codex-local-access-key-model-stats">
                              {renderModelStatsTable(keyModelStats, showAllKeyModels, () => {
                                setShowAllApiKeyModelIds((current) => {
                                  const next = new Set(current);
                                  if (next.has(apiKey.id)) {
                                    next.delete(apiKey.id);
                                  } else {
                                    next.add(apiKey.id);
                                  }
                                  return next;
                                });
                              })}
                            </div>
                          )}
                        </div>
                      );
                    })
                  )}
                </div>
              </section>

              <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-account-stats-section">
                <div className="codex-local-access-section-title">
                  <Server size={16} />
                  <span>{t('codex.localAccess.accountStatsTitle', '按账号统计')}</span>
                </div>
                <div className="codex-local-access-account-stats">
                  {currentMemberStats.length === 0 ? (
                    <div className="group-account-empty">
                      {t('codex.localAccess.statsEmpty', '当前还没有统计数据')}
                    </div>
                  ) : (
                    currentMemberStats.map(({ account, presentation, stats: accountStats }) => {
                      const isRelayAccount = isCodexApiKeyAccount(account);
                      const source = upstreamSourceByAccountId.get(account.id);
                      const providerName =
                        source?.providerName || getApiKeyProviderName(account);
                      const baseUrlHost =
                        source?.baseUrlHost || extractUrlHost(account.api_base_url);
                      const accountCost =
                        costByAccountId.get(account.id) ??
                        estimateUsageStatsCost(accountStats ?? undefined, pricesByModelId);
                      return (
                        <div key={account.id} className="codex-local-access-account-stat-row">
                          <div className="codex-local-access-account-stat-top">
                            <div className="codex-local-access-account-stat-main">
                              <span
                                className="group-account-email"
                                title={maskAccountText(presentation.displayName)}
                              >
                                {maskAccountText(presentation.displayName)}
                              </span>
                              <span className={`tier-badge ${presentation.planClass}`}>
                                {isRelayAccount
                                  ? getAccountSourceLabel(account, t)
                                  : presentation.planLabel}
                              </span>
                              {isRelayAccount && (
                                <span
                                  className="codex-local-access-member-metric"
                                  title={account.api_base_url || providerName}
                                >
                                  {baseUrlHost || providerName}
                                </span>
                              )}
                            </div>
                            <div className="codex-local-access-account-stat-block codex-local-access-account-stat-block-quota">
                              {isRelayAccount ? null : renderQuotaPreview(presentation, 3)}
                            </div>
                            <div className="codex-local-access-account-stat-block codex-local-access-account-stat-block-metrics">
                              <div className="codex-local-access-account-stat-metrics">
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.accountRequestsCompact', {
                                    value: formatCompactNumber(accountStats?.requestCount ?? 0),
                                    defaultValue: '请求 {{value}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.accountResult', {
                                    success: accountStats?.successCount ?? 0,
                                    failed: accountStats?.failureCount ?? 0,
                                    defaultValue: '成功 {{success}} / 失败 {{failed}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.totalTokensCompact', {
                                    value: formatCompactNumber(accountStats?.totalTokens ?? 0),
                                    defaultValue: '总 {{value}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.tokensDetail', {
                                    input: formatCompactNumber(accountStats?.inputTokens ?? 0),
                                    output: formatCompactNumber(accountStats?.outputTokens ?? 0),
                                    defaultValue: '输入 {{input}} / 输出 {{output}}',
                                  })}
                                </span>
                                <span className="codex-local-access-account-stat-pill">
                                  {t('codex.localAccess.stats.specialTokensDetail', {
                                    cached: formatCompactNumber(accountStats?.cachedTokens ?? 0),
                                    reasoning: formatCompactNumber(accountStats?.reasoningTokens ?? 0),
                                    defaultValue: '缓存 {{cached}} / 思考 {{reasoning}}',
                                  })}
                                </span>
                                <span
                                  className="codex-local-access-account-stat-pill codex-local-access-cost-pill"
                                  title={
                                    accountCost.unknownModelIds.length > 0
                                      ? t('codex.localAccess.pricing.unknownModels', {
                                          models: accountCost.unknownModelIds.join(', '),
                                          defaultValue: '未配置价格: {{models}}',
                                        })
                                      : undefined
                                  }
                                >
                                  {t('codex.localAccess.pricing.costPill', {
                                    cost: formatCostEstimate(accountCost),
                                    defaultValue: '费用 {{cost}}',
                                  })}
                                </span>
                              </div>
                            </div>
                          </div>
                        </div>
                      );
                    })
                  )}
                </div>
              </section>
            </div>
          )}

          {isMembersMode && (
            <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-member-section">
              <div className="codex-local-access-section-head">
                <div className="codex-local-access-section-title">
                  <FolderPlus size={16} />
                  <span>{t('codex.localAccess.memberTitle', '集合成员')}</span>
                </div>
                <label className="codex-local-access-free-toggle">
                  <input
                    type="checkbox"
                    checked={restrictFreeAccounts}
                    onChange={() => void handleToggleRestrictFreeAccounts()}
                    disabled={actionBusy}
                  />
                  <span>
                    {t(
                      'codex.localAccess.modal.restrictFreeToggle',
                      '限制 Free 账号使用',
                    )}
                  </span>
                </label>
              </div>

              <div className="group-account-toolbar">
                <div className="group-account-search">
                  <Search size={16} className="group-account-search-icon" />
                  <input
                    ref={searchInputRef}
                    type="text"
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                    placeholder={t('accounts.search')}
                  />
                </div>
                <div className="group-account-picker-filters">
                  <MultiSelectFilterDropdown
                    options={tierFilterOptions}
                    selectedValues={filterTypes}
                    allLabel={allTierFilterLabel}
                    filterLabel={t('common.shared.filterLabel', '筛选')}
                    clearLabel={t('accounts.clearFilter', '清空筛选')}
                    emptyLabel={t('common.none', '暂无')}
                    ariaLabel={t('common.shared.filterLabel', '筛选')}
                    onToggleValue={(value) =>
                      setFilterTypes((prev) =>
                        prev.includes(value)
                          ? prev.filter((item) => item !== value)
                          : [...prev, value],
                      )
                    }
                    onClear={() => setFilterTypes([])}
                  />
                  <AccountTagFilterDropdown
                    availableTags={availableTags}
                    selectedTags={tagFilter}
                    onToggleTag={(value) =>
                      setTagFilter((prev) =>
                        prev.includes(value)
                          ? prev.filter((item) => item !== value)
                          : [...prev, value],
                      )
                    }
                    onClear={() => setTagFilter([])}
                  />
                  <MultiSelectFilterDropdown
                    options={groupFilterOptions}
                    selectedValues={groupFilter}
                    allLabel={t('accounts.groups.allGroups', '全部分组')}
                    filterLabel={t('accounts.groups.manageTitle', '分组管理')}
                    clearLabel={t('accounts.clearFilter', '清空筛选')}
                    emptyLabel={t('common.none', '暂无')}
                    ariaLabel={t('accounts.groups.manageTitle', '分组管理')}
                    onToggleValue={(value) =>
                      setGroupFilter((prev) =>
                        prev.includes(value)
                          ? prev.filter((item) => item !== value)
                          : [...prev, value],
                      )
                    }
                    onClear={() => setGroupFilter([])}
                  />
                </div>
              </div>

              <div className="group-account-item group-account-item-header">
                <input
                  ref={selectAllCheckboxRef}
                  type="checkbox"
                  checked={allVisibleSelected}
                  onChange={toggleSelectAllVisible}
                  disabled={actionBusy || visibleSelectableAccounts.length === 0}
                />
                <div className="group-account-main" />
              </div>

              <div className="group-account-list codex-local-access-member-list">
                {upstreamAccounts.length === 0 ? (
                  <div className="group-account-empty">
                    {t('codex.localAccess.modal.empty', '暂无可加入的账号或中转站')}
                  </div>
                ) : visibleAccounts.length === 0 ? (
                  <div className="group-account-empty">
                    {t('common.shared.noMatch.title', '没有匹配的账号')}
                  </div>
                ) : (
                  visibleAccounts.map((account) => {
                    const presentation = buildCodexAccountPresentation(account, t);
                    const isRelayAccount = isCodexApiKeyAccount(account);
                    const source = upstreamSourceByAccountId.get(account.id);
                    const providerName = source?.providerName || getApiKeyProviderName(account);
                    const baseUrlHost = source?.baseUrlHost || extractUrlHost(account.api_base_url);
                    const isChecked = selected.has(account.id);
                    const isFreeAccount =
                      !isRelayAccount && isCodexExplicitFreePlanType(account.plan_type);
                    const isFreeSelectionBlocked =
                      isFreeAccount && restrictFreeAccounts && !isChecked;
                    const isSourceBlocked = Boolean(source && !source.eligible && !isChecked);
                    const accountStats = allStatsByAccountId.get(account.id)?.usage;

                    return (
                      <label
                        key={account.id}
                        className={`group-account-item${isChecked ? ' is-current' : ''}${
                          isFreeSelectionBlocked || isSourceBlocked ? ' is-disabled' : ''
                        }`}
                        title={isSourceBlocked ? source?.disabledReason || undefined : undefined}
                      >
                        <input
                          type="checkbox"
                          checked={isChecked}
                          disabled={actionBusy || isFreeSelectionBlocked || isSourceBlocked}
                          onChange={() => toggleSelect(account.id)}
                        />
                        <div className="group-account-main">
                        <div className="codex-local-access-member-mainline">
                          <span
                            className="group-account-email"
                            title={maskAccountText(presentation.displayName)}
                          >
                              {maskAccountText(presentation.displayName)}
                            </span>
                          <span className={`tier-badge ${presentation.planClass}`}>
                              {isRelayAccount
                                ? getAccountSourceLabel(account, t)
                                : presentation.planLabel}
                            </span>
                          {isRelayAccount && (
                            <span
                              className="codex-local-access-member-metric"
                              title={account.api_base_url || providerName}
                            >
                              {baseUrlHost || providerName}
                            </span>
                          )}
                          <span className="codex-local-access-member-metric">
                            {t('codex.localAccess.stats.accountRequests', {
                              count: accountStats?.requestCount ?? 0,
                              defaultValue: '{{count}} 次请求',
                            })}
                          </span>
                          {isRelayAccount ? null : renderQuotaPreview(presentation, 2)}
                        </div>
                        </div>
                      </label>
                    );
                  })
                )}
              </div>
            </section>
          )}
        </div>

        <div className="modal-footer group-account-picker-footer codex-local-access-modal-footer">
          {isMembersMode ? (
            <>
              <button className="btn btn-secondary" onClick={onClose} disabled={actionBusy}>
                {t('common.cancel')}
              </button>
              <button
                className="btn btn-primary"
                onClick={() => void handleSaveMembers()}
                disabled={actionBusy || !selectionDirty}
              >
                {saving ? t('common.saving') : t('codex.localAccess.modal.save', '保存集合')}
              </button>
            </>
          ) : (
            <button className="btn btn-secondary" onClick={onClose} disabled={actionBusy}>
              {t('common.close')}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

export default CodexLocalAccessModal;
