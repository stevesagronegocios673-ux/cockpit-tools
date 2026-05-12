import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type {
  CodexLocalAccessServiceSummary,
  CodexLocalAccessState,
} from '../types/codexLocalAccess';
import { CodexLocalAccessServicesPanel } from './CodexLocalAccessServicesPanel';
import './CodexLocalAccessModal.css';

interface CodexLocalAccessServiceInstancesModalProps {
  isOpen: boolean;
  state: CodexLocalAccessState | null;
  onClose: () => void;
  onCreateService: (name?: string) => Promise<unknown> | unknown;
  onRenameService: (serviceId: string, name: string) => Promise<unknown> | unknown;
  onDeleteService: (serviceId: string) => Promise<unknown> | unknown;
  onSelectService: (serviceId: string) => Promise<unknown> | unknown;
  onToggleServiceEnabled?: (service: CodexLocalAccessServiceSummary) => Promise<unknown> | unknown;
  onActivateService?: (serviceId: string) => Promise<unknown> | unknown;
  onManageService?: (serviceId: string) => Promise<unknown> | unknown;
  onConfigureUpstreams?: (serviceId: string) => Promise<unknown> | unknown;
  onRefresh?: () => Promise<unknown> | unknown;
  saving: boolean;
  refreshing?: boolean;
  starting?: boolean;
}

export function CodexLocalAccessServiceInstancesModal({
  isOpen,
  state,
  onClose,
  onCreateService,
  onRenameService,
  onDeleteService,
  onSelectService,
  onToggleServiceEnabled,
  onActivateService,
  onManageService,
  onConfigureUpstreams,
  onRefresh,
  saving,
  refreshing = false,
  starting = false,
}: CodexLocalAccessServiceInstancesModalProps) {
  const { t } = useTranslation();

  if (!isOpen) return null;

  return (
    <div className="modal-overlay codex-local-access-modal-overlay" onClick={onClose}>
      <div
        className="modal codex-local-access-modal codex-local-access-services-modal"
        onClick={(event) => event.stopPropagation()}
      >
        <button
          className="modal-close codex-local-access-close codex-local-access-services-modal-close"
          onClick={onClose}
          aria-label={t('common.close')}
        >
          <X size={18} />
        </button>
        <div className="modal-body codex-local-access-modal-body codex-local-access-services-modal-body">
          <CodexLocalAccessServicesPanel
            state={state}
            actionBusy={saving}
            refreshing={refreshing}
            starting={starting}
            variant="modal"
            onCreateService={onCreateService}
            onRefresh={onRefresh}
            onSelectService={onSelectService}
            onRenameService={onRenameService}
            onDeleteService={onDeleteService}
            onToggleServiceEnabled={onToggleServiceEnabled}
            onActivateService={onActivateService}
            onManageService={onManageService}
            onConfigureUpstreams={onConfigureUpstreams}
          />
        </div>
      </div>
    </div>
  );
}

export default CodexLocalAccessServiceInstancesModal;
