// Main entry point — orchestrates initialization of all modules
import { initNavigation } from './ui/navigation.js';
import { initQueryEditor } from './ui/queryEditor.js';
import { initFileManagerUI } from './ui/fileManager.js';

async function boot() {
    console.log('[App] Starting initialization...');

    // 1. Register duckdb-ready listener FIRST — before any awaits — so we never
    //    miss the event if DuckDB initialises eagerly from duckdbInterop.js module load.
    let duckdbReadyFired = false;
    let duckdbTimeoutId = null;

    function applyDuckDbBadge(opfs) {
        const badge = document.getElementById('persistence-badge');
        const badgeText = document.getElementById('persistence-badge-text');
        if (badge && badgeText) {
            badge.classList.remove('d-none', 'bg-success', 'bg-secondary', 'bg-warning');
            if (opfs) {
                badge.classList.add('bg-success');
                badgeText.textContent = 'OPFS Persistent';
            } else {
                badge.classList.add('bg-secondary');
                badgeText.textContent = 'In-Memory';
            }
        }
    }

    window.addEventListener('duckdb-ready', (e) => {
        duckdbReadyFired = true;
        if (duckdbTimeoutId) clearTimeout(duckdbTimeoutId);
        applyDuckDbBadge(e.detail?.opfs);
    });

    // If the event already fired before this listener was registered
    if (typeof window.duckdbUsesOpfs !== 'undefined') {
        duckdbReadyFired = true;
        applyDuckDbBadge(window.duckdbUsesOpfs);
    }

    // Timeout fallback: if duckdb-ready hasn't fired within 30 seconds, show a warning.
    if (!duckdbReadyFired) {
        duckdbTimeoutId = setTimeout(() => {
            if (!duckdbReadyFired) {
                console.warn('[App] duckdb-ready event not received within 30 s');
                const badge = document.getElementById('persistence-badge');
                const badgeText = document.getElementById('persistence-badge-text');
                if (badge && badgeText) {
                    badge.classList.remove('d-none', 'bg-success', 'bg-secondary');
                    badge.classList.add('bg-warning');
                    badgeText.textContent = 'Init timeout';
                }
            }
        }, 30000);
    }

    // 2. Initialize hash-based navigation
    initNavigation();

    // 3. Initialize the KQL-to-SQL WASM bridge (may take a few seconds)
    try {
        await globalThis.KqlBridge.initialize();
        console.log('[App] KQL Bridge ready');
    } catch (err) {
        console.error('[App] KQL Bridge init failed:', err);
        // Non-fatal: users can still run SQL directly
    }

    // 4. Initialize UI modules
    initQueryEditor();
    initFileManagerUI();

    // 5. Show the app, hide loading overlay
    document.getElementById('app').classList.remove('d-none');
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        overlay.classList.add('fade-out');
        setTimeout(() => overlay.remove(), 300);
    }

    console.log('[App] Initialization complete');
}

boot().catch(err => {
    console.error('[App] Boot failed:', err);
    // Still show app even on error
    document.getElementById('app')?.classList.remove('d-none');
    document.getElementById('loading-overlay')?.remove();
});
