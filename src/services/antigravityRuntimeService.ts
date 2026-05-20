import { invoke } from '@tauri-apps/api/core';

export interface AntigravityInstalledVersionInfo {
  product_name: string;
  version: string;
  app_path: string;
  source: string;
}

export async function getAntigravityInstalledVersionInfo(): Promise<AntigravityInstalledVersionInfo | null> {
  return invoke<AntigravityInstalledVersionInfo | null>('get_antigravity_installed_version_info');
}
