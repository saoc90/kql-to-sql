// Main entry point — orchestrates initialization of all modules
import { initNavigation } from './ui/navigation.js';
import { initQueryEditor } from './ui/queryEditor.js';
import { initFileManagerUI } from './ui/fileManager.js';

// Reveal the app shell and dismiss the loading overlay. Idempotent — safe to call more than once.
function revealApp() {
    document.getElementById('app')?.classList.remove('d-none');
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        overlay.classList.add('fade-out');
        setTimeout(() => overlay.remove(), 300);
    }
}

function startEngine() {
    // Load the KQL-to-SQL WASM bridge. Multi-MB download; on memory-constrained mobile browsers it
    // can even stall while instantiating. It is started AFTER the UI/editor is up (see boot) so its
    // download doesn't starve Monaco — and never awaited on the UI's critical path. SQL works
    // immediately; KQL execution guards on KqlBridge.isReady(); the #engine-loading badge shows state.
    globalThis.KqlBridge.initialize()
        .then(() => {
            console.log('[App] KQL Bridge ready');
            document.getElementById('engine-loading')?.classList.add('d-none');
        })
        .catch(err => {
            console.error('[App] KQL Bridge init failed:', err);
            const badge = document.getElementById('engine-loading');
            if (badge) { badge.classList.remove('bg-warning'); badge.classList.add('bg-danger'); badge.textContent = 'Engine unavailable'; }
        });
}

async function boot() {
    console.log('[App] Starting initialization...');

    // 1. Initialize hash-based navigation
    initNavigation();

    // 2. Safety net: never let the overlay block the UI for more than a few seconds, even if Monaco
    //    is slow on a constrained/mobile connection. The UI fills in as pieces arrive.
    const safety = setTimeout(() => { console.warn('[App] Reveal safety timeout fired'); revealApp(); startEngine(); }, 6000);

    // 3. Bring up the UI/editor FIRST (Monaco). The heavy engine download is deferred until after this
    //    so it can't starve Monaco — otherwise the editor never appears while the engine downloads.
    try {
        await initQueryEditor();
        await initFileManagerUI();
    } catch (err) {
        console.error('[App] UI init error:', err);
    }

    // 4. Show the app, then start the engine in the background.
    clearTimeout(safety);
    revealApp();
    startEngine();
    console.log('[App] UI ready (query engine loading in background)');
}

boot().catch(err => {
    console.error('[App] Boot failed:', err);
    revealApp(); // still show the app even on error
});
