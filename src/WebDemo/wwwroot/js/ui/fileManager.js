// File Manager UI logic — replaces FileManager.razor
import { showAlert, hideAlert } from './notifications.js';
import { renderResults, clearResults } from './resultTable.js';

function quoteIdentifier(name) { return '"' + name.replace(/"/g, '""') + '"'; }

let selectedBackend = 'duckdb';

export function initFileManagerUI() {
    // Backend selector
    const backendSelect = document.getElementById('file-backend-select');
    if (backendSelect) {
        backendSelect.addEventListener('change', (e) => { selectedBackend = e.target.value; });
    }

    // Browse button triggers hidden file input
    document.getElementById('btn-browse-files')?.addEventListener('click', () => {
        document.getElementById('file-input')?.click();
    });

    // Clear all
    document.getElementById('btn-clear-all')?.addEventListener('click', clearAllFiles);

    // Listen for events from the core FileManager class
    const fm = window.fileManager;
    if (fm) {
        // Replace Blazor notification with event-driven pattern
        fm.addEventListener('OnUploadStarted', (e) => onUploadStarted(e.detail));
        fm.addEventListener('OnUploadCompleted', (e) => onUploadCompleted(e.detail));
        fm.addEventListener('OnFileAdded', (e) => onFileAdded(e.detail));
        fm.addEventListener('OnFileValidationFailed', (e) => onValidationFailed(e.detail));
        fm.addEventListener('OnFileProcessingFailed', (e) => onProcessingFailed(e.detail));
        fm.addEventListener('OnFilesRefreshed', () => refreshFileList());
    }

    // Initialize the core file manager (no dotnetRef needed)
    window.fileManager.initialize(null);
    window.FileManagerInterop.setupFileHandling('file-input', 'file-drop-zone');

    // Initial render
    refreshFileList();
}

function onUploadStarted(data) {
    const el = document.getElementById('upload-progress');
    if (el) el.classList.remove('d-none');
    const txt = document.getElementById('upload-status-text');
    if (txt) txt.textContent = `Processing ${data?.count || 0} file(s)...`;
}

function onUploadCompleted(data) {
    const el = document.getElementById('upload-progress');
    if (el) el.classList.add('d-none');

    if (data?.success) {
        showFileStatus(`Successfully processed ${data.count} file(s). Click 'Load' to add them to the database.`, 'success');
    } else {
        showFileStatus(`Failed to process files: ${data?.error || 'Unknown error'}`, 'danger');
    }
    refreshFileList();
}

function onFileAdded(_metadata) {
    refreshFileList();
}

function onValidationFailed(data) {
    showFileStatus(`${data?.fileName}: ${data?.error || 'Validation failed'}`, 'warning');
}

function onProcessingFailed(data) {
    showFileStatus(`Failed to process ${data?.fileName}: ${data?.error || 'Processing failed'}`, 'danger');
}

function showFileStatus(message, type) {
    const el = document.getElementById('file-status-alert');
    if (!el) return;
    const txt = document.getElementById('file-status-text');
    if (txt) txt.textContent = message;
    el.className = el.className.replace(/alert-(success|danger|warning|info)/g, '');
    el.classList.add('alert-' + (type || 'info'));
    el.classList.remove('d-none');
}

function refreshFileList() {
    const metadata = window.fileManager.getFileMetadata();
    const tbody = document.getElementById('files-tbody');
    const filesPanel = document.getElementById('files-list-panel');
    const emptyPanel = document.getElementById('files-empty');
    const clearRow = document.getElementById('clear-all-row');
    const countEl = document.getElementById('files-count');

    if (!metadata || metadata.length === 0) {
        if (filesPanel) filesPanel.classList.add('d-none');
        if (emptyPanel) emptyPanel.classList.remove('d-none');
        if (clearRow) clearRow.classList.add('d-none');
        return;
    }

    if (filesPanel) filesPanel.classList.remove('d-none');
    if (emptyPanel) emptyPanel.classList.add('d-none');
    if (clearRow) clearRow.classList.remove('d-none');
    if (countEl) countEl.textContent = metadata.length;

    if (!tbody) return;
    tbody.innerHTML = '';

    metadata.forEach(file => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td style="white-space:nowrap;">
                <span class="material-icons text-primary" style="font-size:16px;vertical-align:middle;">${getFileIcon(file.type)}</span>
                ${escapeHtml(file.name)}
            </td>
            <td style="white-space:nowrap;">${formatFileSize(file.size)}</td>
            <td style="white-space:nowrap;">${escapeHtml(file.type)}</td>
            <td style="white-space:nowrap;">${formatDate(file.uploadDate)}</td>
            <td>${getStatusBadge(file)}</td>
            <td style="white-space:nowrap;">${getActionButtons(file)}</td>
        `;
        tbody.appendChild(tr);
    });

    // Attach action button handlers
    tbody.querySelectorAll('[data-action]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const action = e.currentTarget.dataset.action;
            const fileId = e.currentTarget.dataset.fileId;
            if (action === 'load') loadFile(fileId);
            else if (action === 'preview') previewFile(fileId);
            else if (action === 'remove') removeFile(fileId);
        });
    });
}

async function loadFile(fileId) {
    try {
        let result;
        if (selectedBackend === 'pglite') {
            result = await window.fileManager.loadFileIntoDatabasePglite(fileId);
        } else {
            result = await window.fileManager.loadFileIntoDatabase(fileId);
        }

        if (result?.success) {
            showFileStatus(result.message || 'File loaded successfully', 'success');
            // Refresh Monaco editor schema
            if (window.refreshEditorSchema) await window.refreshEditorSchema();
        } else {
            showFileStatus(`Failed to load file: ${result?.error || 'Unknown error'}`, 'danger');
        }
    } catch (err) {
        showFileStatus(`Failed to load file: ${err.message}`, 'danger');
    }
    refreshFileList();
}

async function previewFile(fileId) {
    const metadata = window.fileManager.getFileMetadata();
    const file = metadata.find(f => f.id === fileId);
    if (!file || !file.tableName) return;

    try {
        let resultJson;
        if (selectedBackend === 'pglite') {
            resultJson = await window.PGliteInterop.queryJson(`SELECT * FROM ${quoteIdentifier(file.tableName)} LIMIT 100`);
        } else {
            resultJson = await window.DuckDbInterop.queryJson(`SELECT * FROM ${quoteIdentifier(file.tableName)} LIMIT 100`);
        }

        const data = JSON.parse(resultJson);
        document.getElementById('preview-file-name').textContent = file.name;
        renderResults(data, 'preview-thead', 'preview-tbody');

        const info = document.getElementById('preview-row-info');
        if (data.length >= 100 && info) {
            info.textContent = `Showing first 100 rows of ${file.rowCount} total rows`;
            info.classList.remove('d-none');
        } else if (info) {
            info.classList.add('d-none');
        }

        // Show the Bootstrap modal
        const modal = new bootstrap.Modal(document.getElementById('previewModal'));
        modal.show();
    } catch (err) {
        showFileStatus(`Failed to preview ${file.name}: ${err.message}`, 'danger');
    }
}

async function removeFile(fileId) {
    const metadata = window.fileManager.getFileMetadata();
    const file = metadata.find(f => f.id === fileId);

    if (file?.isLoaded && file?.tableName) {
        try {
            await window.PGliteInterop.queryJson(`DROP TABLE IF EXISTS ${quoteIdentifier(file.tableName)}`);
        } catch { /* ignore */ }
        try {
            await window.DuckDbInterop.queryJson(`DROP TABLE IF EXISTS ${quoteIdentifier(file.tableName)}`);
        } catch { /* ignore */ }
    }

    await window.fileManager.removeFile(fileId);
    showFileStatus(`Removed ${file?.name || 'file'}`, 'info');
    refreshFileList();
}

async function clearAllFiles() {
    const metadata = window.fileManager.getFileMetadata();
    for (const file of metadata) {
        if (file.isLoaded && file.tableName) {
            try {
                await window.PGliteInterop.queryJson(`DROP TABLE IF EXISTS ${quoteIdentifier(file.tableName)}`);
            } catch { /* ignore */ }
            try {
                await window.DuckDbInterop.queryJson(`DROP TABLE IF EXISTS ${quoteIdentifier(file.tableName)}`);
            } catch { /* ignore */ }
        }
    }
    await window.fileManager.clearAllFiles();
    showFileStatus('All files cleared', 'info');
    refreshFileList();
}

// Helpers
function getFileIcon(contentType) {
    if (contentType?.includes('csv')) return 'table_chart';
    if (contentType?.includes('json')) return 'data_object';
    if (contentType?.includes('text')) return 'text_snippet';
    return 'insert_drive_file';
}

function getStatusBadge(file) {
    if (file.isLoaded) {
        const persistIcon = file.isPersistent ? ' <span class="material-icons" style="font-size:12px;vertical-align:middle;" title="Persisted in OPFS">cloud_done</span>' : '';
        return `<span class="badge badge-loaded">Loaded (${file.rowCount} rows)${persistIcon}</span>`;
    }
    if (file.hasError) return '<span class="badge badge-error">Error</span>';
    const persistNote = file.isPersistent ? ' (persisted)' : '';
    return `<span class="badge badge-pending">Uploaded${persistNote}</span>`;
}

function getActionButtons(file) {
    let html = '';
    if (!file.isLoaded && !file.hasError) {
        html += `<button class="btn btn-sm btn-outline-primary me-1" data-action="load" data-file-id="${file.id}">Load</button>`;
    }
    if (file.isLoaded) {
        html += `<button class="btn btn-sm btn-outline-primary me-1" data-action="load" data-file-id="${file.id}">Re-Load</button>`;
        html += `<button class="btn btn-sm btn-outline-info me-1" data-action="preview" data-file-id="${file.id}">Preview</button>`;
    }
    html += `<button class="btn btn-sm btn-outline-danger" data-action="remove" data-file-id="${file.id}">Remove</button>`;
    return html;
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    return (bytes / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
}

function formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    return d.toLocaleString();
}

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
