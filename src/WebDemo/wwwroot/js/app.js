// Main entry point — orchestrates initialization of all modules
import { initNavigation } from './ui/navigation.js';
import { initQueryEditor } from './ui/queryEditor.js';
import { initFileManagerUI } from './ui/fileManager.js';

async function boot() {
    console.log('[App] Starting initialization...');

    // 1. Initialize hash-based navigation
    initNavigation();

    // 2. Kick off the KQL-to-SQL WASM bridge in the BACKGROUND. It's a multi-MB download, so awaiting
    //    it here would freeze the whole UI behind the loading overlay (it looks like a hang on a cold
    //    load). Instead we let it load while the UI comes up: SQL mode works immediately, and KQL
    //    execution already guards on KqlBridge.isReady() until the engine finishes. A small badge
    //    (#engine-loading) signals progress and clears when ready.
    globalThis.KqlBridge.initialize()
        .then(() => {
            console.log('[App] KQL Bridge ready');
            document.getElementById('engine-loading')?.classList.add('d-none');
        })
        .catch(err => {
            console.error('[App] KQL Bridge init failed:', err);
            const badge = document.getElementById('engine-loading');
            if (badge) { badge.classList.remove('bg-warning'); badge.classList.add('bg-danger'); badge.textContent = 'Engine failed to load'; }
        });

    // 3. Initialize UI modules (these don't need the WASM bridge)
    await initQueryEditor();
    await initFileManagerUI();

    // 4. Show the app immediately — don't wait for the WASM engine
    document.getElementById('app').classList.remove('d-none');
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        overlay.classList.add('fade-out');
        setTimeout(() => overlay.remove(), 300);
    }

    console.log('[App] UI ready (query engine loading in background)');
}

boot().catch(err => {
    console.error('[App] Boot failed:', err);
    // Still show app even on error
    document.getElementById('app')?.classList.remove('d-none');
    document.getElementById('loading-overlay')?.remove();
});
