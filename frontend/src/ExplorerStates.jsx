import { IconInfoCircle, IconRefresh } from "@tabler/icons-react";


export function LoadingState() {
  return (
    <div className="loading-state" role="status">
      <span />
      Načítám parkovací data…
    </div>
  );
}

export function ErrorState({ hasData, onRetry }) {
  return (
    <div className="error-state" role="alert">
      <div>
        <strong>Data se nepodařilo aktualizovat.</strong>
        <span>{hasData ? " Zobrazené výsledky mohou být zastaralé." : " Zkontrolujte připojení a zkuste to znovu."}</span>
      </div>
      <button type="button" className="text-action" onClick={onRetry}>Zkusit znovu</button>
    </div>
  );
}

export function NoResultsState({ onReset }) {
  return (
    <section className="no-results-state" role="status">
      <span className="eyebrow">Bez výsledků</span>
      <h2>Tomuto výběru neodpovídají žádná data</h2>
      <p>Zkuste rozšířit období nebo zrušit některé filtry. Jedním krokem můžete obnovit celý výběr.</p>
      <button type="button" className="primary-action" onClick={onReset}>
        <IconRefresh size={16} stroke={1.7} aria-hidden="true" />
        Obnovit všechny filtry
      </button>
    </section>
  );
}

export function CoverageNotice() {
  return (
    <div className="coverage-notice" role="note">
      <IconInfoCircle size={16} stroke={1.6} aria-hidden="true" />
      <span><strong>Pro toto období nejsou dostupná data o parkovacích oprávněních.</strong> Zobrazujeme dostupný vývoj kapacity parkovacích míst.</span>
    </div>
  );
}
