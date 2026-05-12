import { invoke } from '@tauri-apps/api/core';
import type {
  CodexLocalAccessApiKeyInput,
  CodexLocalAccessPortCleanupResult,
  CodexLocalAccessRoutingStrategy,
  CodexLocalAccessState,
} from '../types/codexLocalAccess';

export async function getCodexLocalAccessState(): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_get_state');
}

export async function testCodexLocalAccessUpstream(
  serviceId: string | null,
  accountId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_test_upstream', { serviceId, accountId });
}

export async function createCodexLocalAccessService(
  name?: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_create_service', { name: name ?? null });
}

export async function renameCodexLocalAccessService(
  serviceId: string,
  name: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_rename_service', { serviceId, name });
}

export async function deleteCodexLocalAccessService(
  serviceId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_delete_service', { serviceId });
}

export async function selectCodexLocalAccessService(
  serviceId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_select_service', { serviceId });
}

export async function saveCodexLocalAccessAccounts(
  serviceId: string | null,
  accountIds: string[],
  restrictFreeAccounts: boolean,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_save_accounts', {
    serviceId,
    accountIds,
    restrictFreeAccounts,
  });
}

export async function removeCodexLocalAccessAccount(
  serviceId: string | null,
  accountId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_remove_account', { serviceId, accountId });
}

export async function createCodexLocalAccessApiKey(
  serviceId: string | null,
  input: CodexLocalAccessApiKeyInput,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_create_api_key', {
    serviceId,
    name: input.name,
    monthlyTokenLimit: input.monthlyTokenLimit,
    upstreamScope: input.upstreamScope,
    allowedAccountIds: input.allowedAccountIds,
  });
}

export async function updateCodexLocalAccessApiKey(
  serviceId: string | null,
  apiKeyId: string,
  input: CodexLocalAccessApiKeyInput & { enabled: boolean },
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_update_api_key', {
    serviceId,
    apiKeyId,
    name: input.name,
    enabled: input.enabled,
    monthlyTokenLimit: input.monthlyTokenLimit,
    upstreamScope: input.upstreamScope,
    allowedAccountIds: input.allowedAccountIds,
  });
}

export async function setCodexLocalAccessDefaultApiKey(
  serviceId: string | null,
  apiKeyId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_set_default_api_key', { serviceId, apiKeyId });
}

export async function rotateCodexLocalAccessApiKey(
  serviceId: string | null,
  apiKeyId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_rotate_api_key', { serviceId, apiKeyId });
}

export async function deleteCodexLocalAccessApiKey(
  serviceId: string | null,
  apiKeyId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_delete_api_key', { serviceId, apiKeyId });
}

export async function clearCodexLocalAccessStats(
  serviceId: string | null,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_clear_stats', { serviceId });
}

export async function fetchOpenAiPricingMarkdown(): Promise<string> {
  return await invoke('codex_local_access_fetch_openai_pricing_markdown');
}

export async function prepareCodexLocalAccessForRestart(): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_prepare_restart');
}

export async function killCodexLocalAccessPort(
  serviceId: string | null,
): Promise<CodexLocalAccessPortCleanupResult> {
  return await invoke('codex_local_access_kill_port', { serviceId });
}

export async function updateCodexLocalAccessPort(
  serviceId: string | null,
  port: number,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_update_port', { serviceId, port });
}

export async function updateCodexLocalAccessRoutingStrategy(
  serviceId: string | null,
  strategy: CodexLocalAccessRoutingStrategy,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_update_routing_strategy', { serviceId, strategy });
}

export async function setCodexLocalAccessEnabled(
  serviceId: string | null,
  enabled: boolean,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_set_enabled', { serviceId, enabled });
}

export async function activateCodexLocalAccess(
  serviceId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_activate', { serviceId });
}
