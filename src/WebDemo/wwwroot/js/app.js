// Main entry point — orchestrates initialization of all modules
import { initNavigation } from './ui/navigation.js';
import { initQueryEditor } from './ui/queryEditor.js';
import { initFileManagerUI } from './ui/fileManager.js';

async function boot() {
    console.log('[App] Starting initialization...');

    // 1. Initialize hash-based navigation
    initNavigation();

    // 2. Initialize the KQL-to-SQL WASM bridge (may take a few seconds)
    try {
        await globalThis.KqlBridge.initialize();
        console.log('[App] KQL Bridge ready');
    } catch (err) {
        console.error('[App] KQL Bridge init failed:', err);
        // Non-fatal: users can still run SQL directly
    }

    // 3. Initialize UI modules
    await initQueryEditor();
    await initFileManagerUI();

    // 4. Show the app, hide loading overlay
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
