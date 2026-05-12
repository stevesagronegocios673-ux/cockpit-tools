import { useEffect, useMemo, useState, type MouseEvent as ReactMouseEvent } from 'react';
import {
  Check,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  Copy,
  Database,
  FolderPlus,
  Pencil,
  Play,
  Power,
  RefreshCw,
  Server,
  Trash2,
  X,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type {
  CodexLocalAccessServiceSummary,
  CodexLocalAccessState,
} from '../types/codexLocalAccess';
import './CodexLocalAccessModal.css';

interface CodexLocalAccessServicesPanelProps {
  state: CodexLocalAccessState | null;
  launchCurrent?: boolean;
  actionBusy?: boolean;
  refreshing?: boolean;
  starting?: boolean;
  variant?: 'overview' | 'modal';
  onCreateService?: (name?: string) => Promise<unknown> | unknown;
  onRefresh?: () => Promise<unknown> | unknown;
  onSelectService?: (serviceId: string) => Promise<unknown> | unknown;
  onRenameService?: (serviceId: string, name: string) => Promise<unknown> | unknown;
  onDeleteService?: (serviceId: string) => Promise<unknown> | unknown;
  onToggleServiceEnabled?: (service: CodexLocalAccessServiceSummary) => Promise<unknown> | unknown;
  onActivateService?: (serviceId: string) => Promise<unknown> | unknown;
  onManageService?: (serviceId: string) => Promise<unknown> | unknown;
  onConfigureUpstreams?: (serviceId: string) => Promise<unknown> | unknown;
  onHideEntry?: () => void;
}

function isHealthyStatus(status: string): boolean {
  const normalized = status.trim().toLowerCase();
  return normalized === 'healthy' || normalized === 'normal' || normalized === 'ok';
}

function getServiceStatusClass(service: CodexLocalAccessServiceSummary): string {
  if (service.running) return 'running';
  if (service.enabled) return 'stopped';
  return 'disabled';
}

function getHealthStatusClass(service: CodexLocalAccessServiceSummary): string {
  if (isHealthyStatus(service.healthStatus) && service.alertCount === 0 && !service.lastError) {
    return 'is-healthy';
  }
  if (service.healthStatus.trim().toLowerCase() === 'unavailable' || service.lastError) {
    return 'is-unavailable';
  }
  return 'is-degraded';
}

function formatServiceStatus(service: CodexLocalAccessServiceSummary, t: ReturnType<typeof useTranslation>['t']) {
  if (service.running) return t('codex.localAccess.statusRunning', '运行中');
  if (service.enabled) return t('codex.localAccess.statusStopped', '未运行');
  return t('codex.localAccess.statusDisabled', '已停用');
}

function formatHealthStatus(service: CodexLocalAccessServiceSummary, t: ReturnType<typeof useTranslation>['t']) {
  if (isHealthyStatus(service.healthStatus) && service.alertCount === 0 && !service.lastError) {
    return t('codex.localAccess.health.statusHealthy', '正常');
  }
  if (service.healthStatus.trim().toLowerCase() === 'unavailable' || service.lastError) {
    return t('codex.localAccess.health.statusUnavailable', '不可用');
  }
  return t('codex.localAccess.health.statusDegraded', '部分异常');
}

function formatCompactNumber(value: number): string {
  if (!Number.isFinite(value)) return '0';
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
}

function formatBaseUrl(service: CodexLocalAccessServiceSummary): string {
  return service.baseUrl || service.apiPortUrl || `http://127.0.0.1:${service.port}/v1`;
}

export function CodexLocalAccessServicesPanel({
  state,
  launchCurrent = false,
  actionBusy = false,
  refreshing = false,
  starting = false,
  variant = 'overview',
  onCreateService,
  onRefresh,
  onSelectService,
  onRenameService,
  onDeleteService,
  onToggleServiceEnabled,
  onActivateService,
  onManageService,
  onConfigureUpstreams,
  onHideEntry,
}: CodexLocalAccessServicesPanelProps) {
  const { t } = useTranslation();
  const services = state?.services ?? [];
  const selectedServiceId = state?.selectedServiceId ?? state?.collection?.id ?? services[0]?.id ?? null;
  const selectedService = services.find((service) => service.id === selectedServiceId) ?? services[0] ?? null;
  const [expandedServiceIds, setExpandedServiceIds] = useState<Set<string>>(new Set());
  const [copiedServiceId, setCopiedServiceId] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');

  useEffect(() => {
    setExpandedServiceIds((current) => {
      const serviceIds = new Set(services.map((service) => service.id));
      const next = new Set(Array.from(current).filter((serviceId) => serviceIds.has(serviceId)));
      return next.size === current.size ? current : next;
    });
  }, [services]);

  const summary = useMemo(() => {
    const runningCount = services.filter((service) => service.running).length;
    const abnormalCount = services.filter(
      (service) => !isHealthyStatus(service.healthStatus) || service.alertCount > 0 || Boolean(service.lastError),
    ).length;
    const todayRequests = state?.stats?.daily?.totals?.requestCount ?? 0;
    const todayTokens = state?.stats?.daily?.totals?.totalTokens ?? 0;
    return {
      runningCount,
      abnormalCount,
      todayRequests,
      todayTokens,
    };
  }, [services, state?.stats?.daily?.totals?.requestCount, state?.stats?.daily?.totals?.totalTokens]);

  const runPanelAction = async (
    action: () => Promise<unknown> | unknown,
    successMessage?: string,
  ) => {
    if (actionBusy) return;
    setError('');
    setNotice('');
    try {
      const result = await action();
      if (result !== null && successMessage) {
        setNotice(successMessage);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleCreateService = async () => {
    if (!onCreateService) return;
    const name = window.prompt(
      t('codex.localAccess.serviceNamePrompt', '输入新 API 服务名称'),
      t('codex.localAccess.defaultNewServiceName', 'API 服务'),
    );
    if (name == null) return;
    await runPanelAction(
      () => onCreateService(name.trim() || undefined),
      t('codex.localAccess.serviceCreateSuccess', 'API 服务已创建'),
    );
  };

  const handleRenameService = async (service: CodexLocalAccessServiceSummary) => {
    if (!onRenameService) return;
    const name = window.prompt(
      t('codex.localAccess.serviceRenamePrompt', '输入新的 API 服务名称'),
      service.name,
    );
    if (name == null || name.trim() === service.name.trim()) return;
    await runPanelAction(
      () => onRenameService(service.id, name.trim()),
      t('codex.localAccess.serviceRenameSuccess', 'API 服务已重命名'),
    );
  };

  const handleCopyBaseUrl = async (service: CodexLocalAccessServiceSummary) => {
    const baseUrl = formatBaseUrl(service);
    try {
      await navigator.clipboard.writeText(baseUrl);
      setCopiedServiceId(service.id);
      window.setTimeout(() => {
        setCopiedServiceId((current) => (current === service.id ? null : current));
      }, 1200);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const toggleExpanded = (serviceId: string) => {
    setExpandedServiceIds((current) => {
      const next = new Set(current);
      if (next.has(serviceId)) {
        next.delete(serviceId);
      } else {
        next.add(serviceId);
      }
      return next;
    });
  };

  const handleButtonClick = (
    event: ReactMouseEvent<HTMLButtonElement>,
    callback: () => void,
  ) => {
    event.stopPropagation();
    callback();
  };

  return (
    <section className={`codex-local-access-services-panel codex-local-access-services-panel--${variant}`}>
      <div className="codex-local-access-services-panel-head">
        <div className="codex-local-access-services-panel-title">
          <Server size={18} />
          <div>
            <strong>{t('codex.localAccess.overviewTitle', 'Codex API 服务')}</strong>
            <span>
              {t('codex.localAccess.servicesPanelSummary', {
                count: services.length,
                runningCount: summary.runningCount,
                defaultValue: '{{count}} 个服务 · {{runningCount}} 个运行中',
              })}
            </span>
          </div>
        </div>
        <div className="codex-local-access-services-panel-actions">
          {onCreateService && (
            <button
              type="button"
              className="btn btn-secondary btn-sm"
              onClick={() => void handleCreateService()}
              disabled={actionBusy}
            >
              <FolderPlus size={14} />
              {t('codex.localAccess.serviceCreateAction', '新建服务')}
            </button>
          )}
          {onRefresh && (
            <button
              type="button"
              className="btn btn-secondary btn-sm"
              onClick={() => void runPanelAction(() => onRefresh())}
              disabled={actionBusy}
            >
              <RefreshCw size={14} className={refreshing ? 'loading-spinner' : ''} />
              {t('codex.localAccess.refreshStats', '刷新统计')}
            </button>
          )}
          {selectedService && onManageService && (
            <button
              type="button"
              className="btn btn-secondary btn-sm"
              onClick={() => void runPanelAction(() => onManageService(selectedService.id))}
              disabled={actionBusy}
            >
              <Database size={14} />
              {t('codex.localAccess.dashboardAction', '服务面板')}
            </button>
          )}
          {selectedService && onActivateService && (
            <button
              type="button"
              className="btn btn-primary btn-sm"
              onClick={() => void runPanelAction(() => onActivateService(selectedService.id))}
              disabled={actionBusy}
            >
              <Play size={14} className={starting ? 'loading-spinner' : ''} />
              {t('codex.localAccess.activateAction', '启动 API 服务')}
            </button>
          )}
          {onHideEntry && (
            <button
              type="button"
              className="folder-icon-btn codex-local-access-services-hide-btn"
              onClick={onHideEntry}
              disabled={actionBusy}
              title={t('codex.localAccess.hideEntryAction', '关闭 API 服务入口')}
              aria-label={t('codex.localAccess.hideEntryAction', '关闭 API 服务入口')}
            >
              <X size={14} />
            </button>
          )}
        </div>
      </div>

      <div className="codex-local-access-services-metrics">
        <div>
          <span>{t('codex.localAccess.servicesTotal', '服务数')}</span>
          <strong>{services.length}</strong>
        </div>
        <div>
          <span>{t('codex.localAccess.servicesRunning', '运行中')}</span>
          <strong>{summary.runningCount}</strong>
        </div>
        <div>
          <span>{t('codex.localAccess.servicesAbnormal', '异常')}</span>
          <strong>{summary.abnormalCount}</strong>
        </div>
        <div>
          <span>{t('codex.localAccess.currentService', '当前服务')}</span>
          <strong title={selectedService?.name}>{selectedService?.name ?? '-'}</strong>
        </div>
        <div>
          <span>{t('codex.localAccess.health.todayRequests', '今日请求')}</span>
          <strong>{formatCompactNumber(summary.todayRequests)}</strong>
        </div>
        <div>
          <span>{t('codex.localAccess.health.todayTokens', '今日 Token')}</span>
          <strong>{formatCompactNumber(summary.todayTokens)}</strong>
        </div>
      </div>

      {(error || notice) && (
        <div className={error ? 'codex-local-access-inline-error' : 'codex-local-access-inline-success'}>
          {error ? <CircleAlert size={14} /> : <Check size={14} />}
          <span>{error || notice}</span>
        </div>
      )}

      {services.length === 0 ? (
        <div className="codex-local-access-services-empty">
          {t('codex.localAccess.serviceInstancesEmpty', '暂无 API 服务实例')}
        </div>
      ) : (
        <div className="codex-local-access-service-card-grid">
          {services.map((service) => {
            const isSelected = service.id === selectedServiceId;
            const isExpanded = expandedServiceIds.has(service.id);
            const baseUrl = formatBaseUrl(service);
            return (
              <article
                key={service.id}
                className={`codex-local-access-service-card ${
                  isSelected ? 'is-selected' : ''
                } ${isExpanded ? 'is-expanded' : ''}`}
                onClick={() => toggleExpanded(service.id)}
              >
                <div className="codex-local-access-service-card-head">
                  <div className="codex-local-access-service-card-title">
                    <span className="codex-local-access-service-card-icon">
                      <Server size={16} />
                    </span>
                    <div>
                      <strong title={service.name}>{service.name}</strong>
                      <span title={baseUrl}>{baseUrl}</span>
                    </div>
                  </div>
                  <div className="codex-local-access-service-card-badges">
                    {isSelected && (
                      <span className="codex-local-access-subtle-badge">
                        {t('codex.localAccess.currentService', '当前服务')}
                      </span>
                    )}
                    {launchCurrent && isSelected && (
                      <span className="current-tag">{t('codex.current', '当前')}</span>
                    )}
                  </div>
                </div>

                <div className="codex-local-access-service-card-status-row">
                  <span className={`codex-local-access-status ${getServiceStatusClass(service)}`}>
                    <Power size={12} />
                    {formatServiceStatus(service, t)}
                  </span>
                  <span className={`codex-local-access-health-status ${getHealthStatusClass(service)}`}>
                    {formatHealthStatus(service, t)}
                  </span>
                  {service.alertCount > 0 && (
                    <span className="codex-local-access-alert-count">
                      {t('codex.localAccess.serviceAlertCount', {
                        count: service.alertCount,
                        defaultValue: '{{count}} 个告警',
                      })}
                    </span>
                  )}
                </div>

                <div className="codex-local-access-service-card-metrics">
                  <div>
                    <span>{t('codex.localAccess.health.port', '端口')}</span>
                    <strong>{service.port}</strong>
                  </div>
                  <div>
                    <span>{t('codex.localAccess.health.upstreams', '可用上游')}</span>
                    <strong>{service.memberCount}</strong>
                  </div>
                  <div>
                    <span>{t('codex.localAccess.apiKeysTitle', '用户密钥')}</span>
                    <strong>{service.apiKeyCount}</strong>
                  </div>
                </div>

                {service.lastError && (
                  <div className="codex-local-access-service-card-error" title={service.lastError}>
                    <CircleAlert size={13} />
                    <span>{service.lastError}</span>
                  </div>
                )}

                {isExpanded && (
                  <div className="codex-local-access-service-card-details">
                    <div className="codex-local-access-service-card-detail-row">
                      <span>{t('codex.localAccess.health.defaultKey', '默认密钥')}</span>
                      <strong>{service.defaultApiKeyName ?? t('codex.localAccess.health.noDefaultKey', '无')}</strong>
                    </div>
                    <div className="codex-local-access-service-card-detail-row">
                      <span>{t('codex.localAccess.baseUrl', '地址')}</span>
                      <code title={baseUrl}>{baseUrl}</code>
                    </div>
                  </div>
                )}

                <div className="codex-local-access-service-card-actions">
                  {onSelectService && (
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={(event) => handleButtonClick(event, () => void runPanelAction(
                        () => onSelectService(service.id),
                        t('codex.localAccess.serviceSelectSuccess', '已切换服务实例'),
                      ))}
                      disabled={actionBusy || isSelected}
                    >
                      {isSelected
                        ? t('codex.localAccess.currentService', '当前服务')
                        : t('codex.localAccess.serviceSelectAction', '选择')}
                    </button>
                  )}
                  {onConfigureUpstreams && (
                    <button
                      type="button"
                      className="folder-icon-btn codex-local-access-toolbar-btn"
                      onClick={(event) => handleButtonClick(event, () => void runPanelAction(
                        () => onConfigureUpstreams(service.id),
                      ))}
                      disabled={actionBusy}
                      title={t('codex.localAccess.manageUpstreamsAction', '选择上游')}
                      aria-label={t('codex.localAccess.manageUpstreamsAction', '选择上游')}
                    >
                      <FolderPlus size={14} />
                    </button>
                  )}
                  {onManageService && (
                    <button
                      type="button"
                      className="folder-icon-btn codex-local-access-toolbar-btn"
                      onClick={(event) => handleButtonClick(event, () => void runPanelAction(
                        () => onManageService(service.id),
                      ))}
                      disabled={actionBusy}
                      title={t('codex.localAccess.dashboardAction', '服务面板')}
                      aria-label={t('codex.localAccess.dashboardAction', '服务面板')}
                    >
                      <Database size={14} />
                    </button>
                  )}
                  {onToggleServiceEnabled && (
                    <button
                      type="button"
                      className={`folder-icon-btn codex-local-access-toolbar-btn ${
                        service.enabled ? 'is-danger' : 'is-primary'
                      }`}
                      onClick={(event) => handleButtonClick(event, () => void runPanelAction(
                        () => onToggleServiceEnabled(service),
                      ))}
                      disabled={actionBusy}
                      title={
                        service.enabled
                          ? t('codex.localAccess.disableService', '停用服务')
                          : t('codex.localAccess.enableService', '启用服务')
                      }
                      aria-label={
                        service.enabled
                          ? t('codex.localAccess.disableService', '停用服务')
                          : t('codex.localAccess.enableService', '启用服务')
                      }
                    >
                      <Power size={14} />
                    </button>
                  )}
                  {onActivateService && (
                    <button
                      type="button"
                      className="folder-icon-btn codex-local-access-toolbar-btn is-primary"
                      onClick={(event) => handleButtonClick(event, () => void runPanelAction(
                        () => onActivateService(service.id),
                      ))}
                      disabled={actionBusy}
                      title={t('codex.localAccess.activateAction', '启动 API 服务')}
                      aria-label={t('codex.localAccess.activateAction', '启动 API 服务')}
                    >
                      <Play size={14} className={starting ? 'loading-spinner' : ''} />
                    </button>
                  )}
                  <button
                    type="button"
                    className="folder-icon-btn codex-local-access-toolbar-btn"
                    onClick={(event) => handleButtonClick(event, () => void handleCopyBaseUrl(service))}
                    title={t('common.copy', '复制')}
                    aria-label={t('common.copy', '复制')}
                  >
                    {copiedServiceId === service.id ? <Check size={14} /> : <Copy size={14} />}
                  </button>
                  {onRenameService && (
                    <button
                      type="button"
                      className="folder-icon-btn codex-local-access-toolbar-btn"
                      onClick={(event) => handleButtonClick(event, () => void handleRenameService(service))}
                      disabled={actionBusy}
                      title={t('codex.localAccess.serviceRenameAction', '重命名服务')}
                      aria-label={t('codex.localAccess.serviceRenameAction', '重命名服务')}
                    >
                      <Pencil size={14} />
                    </button>
                  )}
                  {onDeleteService && (
                    <button
                      type="button"
                      className="folder-icon-btn codex-local-access-toolbar-btn is-danger"
                      onClick={(event) => handleButtonClick(event, () => void runPanelAction(
                        () => onDeleteService(service.id),
                        t('codex.localAccess.serviceDeleteSuccess', 'API 服务已删除'),
                      ))}
                      disabled={actionBusy || services.length <= 1}
                      title={t('codex.localAccess.serviceDeleteAction', '删除服务')}
                      aria-label={t('codex.localAccess.serviceDeleteAction', '删除服务')}
                    >
                      <Trash2 size={14} />
                    </button>
                  )}
                  <button
                    type="button"
                    className="folder-icon-btn codex-local-access-toolbar-btn"
                    onClick={(event) => handleButtonClick(event, () => toggleExpanded(service.id))}
                    title={
                      isExpanded
                        ? t('codex.localAccess.collapseDetails', '收起详情')
                        : t('codex.localAccess.expandDetails', '展开详情')
                    }
                    aria-label={
                      isExpanded
                        ? t('codex.localAccess.collapseDetails', '收起详情')
                        : t('codex.localAccess.expandDetails', '展开详情')
                    }
                  >
                    {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                  </button>
                </div>
              </article>
            );
          })}
        </div>
      )}
    </section>
  );
}

export default CodexLocalAccessServicesPanel;
