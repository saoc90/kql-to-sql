// Simple hash-based SPA routing

const pages = {
    '':        'page-editor',
    '/':       'page-editor',
    '/files':  'page-files'
};

export function initNavigation() {
    window.addEventListener('hashchange', applyRoute);
    applyRoute();
}

function applyRoute() {
    const hash = window.location.hash.replace('#', '') || '/';
    const targetId = pages[hash] || 'page-editor';

    // Toggle page visibility
    Object.values(pages).forEach(id => {
        const el = document.getElementById(id);
        if (el) el.classList.toggle('d-none', el.id !== targetId);
    });

    // Update nav active state
    document.querySelectorAll('[data-nav]').forEach(link => {
        const navTarget = link.dataset.nav;
        const isActive = (navTarget === 'editor' && targetId === 'page-editor') ||
                         (navTarget === 'files' && targetId === 'page-files');
        link.classList.toggle('active', isActive);
    });

    // Refresh editor schema when navigating to the Query Editor page
    if (targetId === 'page-editor' && window.refreshEditorSchema) {
        window.refreshEditorSchema();
    }
}
