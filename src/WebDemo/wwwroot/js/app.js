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

async function boot() {
    console.log('[App] Starting initialization...');

    // 1. Initialize hash-based navigation
    initNavigation();

    // 2. Kick off the KQL-to-SQL WASM bridge in the BACKGROUND. It's a multi-MB download (and on
    //    memory-constrained mobile browsers it can stall indefinitely while instantiating), so
    //    awaiting it here would freeze the whole UI behind the overlay — the reported "hang". Let it
    //    load while the UI comes up: SQL mode works immediately, KQL execution already guards on
    //    KqlBridge.isReady(), and the #engine-loading badge shows progress / failure.
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

    // 3. Safety net: never let the overlay block the UI for more than a few seconds, even if Monaco
    //    or the engine are slow on a constrained/mobile connection. The UI fills in as pieces arrive.
    const safety = setTimeout(() => { console.warn('[App] Reveal safety timeout fired'); revealApp(); }, 6000);

    // 4. Initialize UI modules (these don't need the WASM bridge)
    try {
        await initQueryEditor();
        await initFileManagerUI();
    } catch (err) {
        console.error('[App] UI init error:', err);
    }

    // 5. Show the app — don't wait for the WASM engine
    clearTimeout(safety);
    revealApp();
    console.log('[App] UI ready (query engine loading in background)');
}

boot().catch(err => {
    console.error('[App] Boot failed:', err);
    revealApp(); // still show the app even on error
});
