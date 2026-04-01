// Query Editor UI logic — replaces Home.razor
import { showAlert, hideAlert, hideAllAlerts } from './notifications.js';
import { renderResults, clearResults } from './resultTable.js';

const EDITOR_ID = 'monaco-editor-container';
let selectedMode = 'kql';
let selectedBackend = 'duckdb';
let isExecuting = false;
let convertedSql = '';

export async function initQueryEditor() {
    // Mode toggle
    document.querySelectorAll('input[name="queryMode"]').forEach(radio => {
        radio.addEventListener('change', (e) => onModeChanged(e.target.value));
    });

    // Backend selector
    const backendSelect = document.getElementById('backend-select');
    if (backendSelect) {
        backendSelect.addEventListener('change', (e) => { selectedBackend = e.target.value; });
    }

    // Buttons
    document.getElementById('btn-execute')?.addEventListener('click', run);
    document.getElementById('btn-convert')?.addEventListener('click', convertToSql);
    document.getElementById('btn-show-tables')?.addEventListener('click', showTables);
    document.getElementById('btn-copy-sql')?.addEventListener('click', copySql);
    document.getElementById('btn-use-sql')?.addEventListener('click', useConvertedSql);

    // Initialize Monaco editor — must complete before file manager restores OPFS files
    await initEditor();
}

async function initEditor() {
    try {
        await window.waitForContainer(EDITOR_ID);
        await window.ensureMonacoEditor(EDITOR_ID);

        // Setup Shift+Enter keyboard shortcut (without dotnetRef — use direct callback)
        const editor = window.nativeMonacoEditor.getEditor(EDITOR_ID);
        if (editor && window.nativeMonacoEditor.monaco) {
            const monaco = window.nativeMonacoEditor.monaco;
            editor.addCommand(
                monaco.KeyMod.Shift | monaco.KeyCode.Enter,
                () => { run(true); }
            );
            console.log('[QueryEditor] Shift+Enter shortcut registered');
        }
    } catch (err) {
        console.error('[QueryEditor] Editor init failed:', err);
    }
}

async function onModeChanged(newMode) {
    if (selectedMode === newMode) return;
    selectedMode = newMode;

    // Update UI
    document.getElementById('btn-execute-mode').textContent = newMode.toUpperCase();
    document.getElementById('btn-convert').classList.toggle('d-none', newMode !== 'kql');
    hidePanel('converted-sql-panel');
    hideAlert('conversion-alert');

    // Switch editor language
    const currentValue = window.nativeMonacoEditor.getValue(EDITOR_ID);
    const newLang = newMode === 'kql' ? 'kusto' : 'sql';
    window.nativeMonacoEditor.setLanguage(EDITOR_ID, newLang);

    if (!currentValue || currentValue.trim() === '' || currentValue.includes('Load sample data first')) {
        window.nativeMonacoEditor.setValue(EDITOR_ID, getDefaultQuery());
    }

    // Refresh Kusto schema when switching to KQL
    if (newMode === 'kql') {
        try {
            if (window.refreshEditorSchema) await window.refreshEditorSchema();
        } catch (err) {
            console.warn('[QueryEditor] Schema refresh failed:', err);
        }
    }
}

function getDefaultQuery() {
    return selectedMode === 'kql'
        ? 'StormEvents | take 10'
        : 'SELECT * FROM StormEvents LIMIT 10;';
}

async function convertToSql() {
    if (selectedMode !== 'kql' || !globalThis.KqlBridge?.isReady()) return;

    const kql = window.nativeMonacoEditor.getValue(EDITOR_ID);
    if (!kql || !kql.trim()) {
        showAlert('error-alert', 'Please enter some KQL to convert.', 'danger');
        return;
    }

    hideAllAlerts();
    const result = globalThis.KqlBridge.translateKqlToSql(kql, selectedBackend);

    if (result.success) {
        convertedSql = result.sql;
        document.getElementById('converted-sql-code').textContent = convertedSql;
        showPanel('converted-sql-panel');
        const dialectName = selectedBackend === 'pglite' ? 'PGlite' : 'DuckDB';
        showAlert('conversion-alert', `KQL successfully converted to SQL (${dialectName})!`, 'success');
    } else {
        showAlert('error-alert', `Conversion failed: ${result.error}`, 'danger');
        hidePanel('converted-sql-panel');
    }
}

async function copySql() {
    if (convertedSql) {
        try {
            await navigator.clipboard.writeText(convertedSql);
            showAlert('conversion-alert', 'SQL copied to clipboard!', 'success');
        } catch {
            showAlert('error-alert', 'Failed to copy to clipboard.', 'warning');
        }
    }
}

function useConvertedSql() {
    if (!convertedSql) return;
    selectedMode = 'sql';
    document.getElementById('mode-sql').checked = true;
    document.getElementById('btn-execute-mode').textContent = 'SQL';
    document.getElementById('btn-convert').classList.add('d-none');

    window.nativeMonacoEditor.setLanguage(EDITOR_ID, 'sql');
    window.nativeMonacoEditor.setValue(EDITOR_ID, convertedSql);
    convertedSql = '';
    hidePanel('converted-sql-panel');
    showAlert('conversion-alert', 'Converted SQL loaded into editor. You can now execute it.', 'success');
}

// Get the statement at the cursor position (blocks separated by blank lines, like ADX).
// If text is selected, return the selection instead.
function getActiveStatement() {
    const editor = window.nativeMonacoEditor.getEditor(EDITOR_ID);
    if (!editor) return null;

    const selection = editor.getSelection();
    if (selection && !selection.isEmpty()) {
        return editor.getModel().getValueInRange(selection);
    }

    const model = editor.getModel();
    const position = editor.getPosition();
    const lineCount = model.getLineCount();
    const cursorLine = position.lineNumber;

    // Walk up to find the start of the statement block (first blank line above)
    let startLine = cursorLine;
    while (startLine > 1 && model.getLineContent(startLine - 1).trim() !== '') {
        startLine--;
    }

    // Walk down to find the end of the statement block (first blank line below)
    let endLine = cursorLine;
    while (endLine < lineCount && model.getLineContent(endLine + 1).trim() !== '') {
        endLine++;
    }

    const range = new window.nativeMonacoEditor.monaco.Range(startLine, 1, endLine, model.getLineMaxColumn(endLine));

    // Briefly highlight the executed block
    const decorations = editor.createDecorationsCollection([{
        range,
        options: { className: 'executed-statement-highlight', isWholeLine: true }
    }]);
    setTimeout(() => decorations.clear(), 600);

    return model.getValueInRange(range);
}

async function run(cursorOnly) {
    if (isExecuting) return;
    isExecuting = true;

    const btn = document.getElementById('btn-execute');
    btn.classList.add('btn-executing');
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Running...';

    hideAllAlerts();
    hidePanel('results-panel');
    hidePanel('raw-result-panel');
    hidePanel('converted-sql-panel');

    try {
        const query = cursorOnly
            ? getActiveStatement()
            : window.nativeMonacoEditor.getValue(EDITOR_ID);
        if (!query || !query.trim()) {
            showAlert('error-alert', `Please enter some ${selectedMode.toUpperCase()} to execute.`, 'danger');
            return;
        }

        let sqlToExecute = query;

        // Convert KQL to SQL if in KQL mode
        if (selectedMode === 'kql') {
            if (!globalThis.KqlBridge?.isReady()) {
                showAlert('error-alert', 'KQL bridge is not ready yet. Please wait.', 'warning');
                return;
            }
            const result = globalThis.KqlBridge.translateKqlToSql(query, selectedBackend);
            if (!result.success) {
                showAlert('error-alert', `KQL to SQL conversion failed: ${result.error}`, 'danger');
                return;
            }
            sqlToExecute = result.sql;
        }

        // Execute against selected backend
        let queryResult;
        if (selectedBackend === 'duckdb') {
            queryResult = await window.DuckDbInterop.queryJson(sqlToExecute);
        } else {
            queryResult = await window.PGliteInterop.queryJson(sqlToExecute);
        }

        // Parse and render
        try {
            const data = JSON.parse(queryResult);
            if (Array.isArray(data) && data.length > 0) {
                renderResults(data, 'results-thead', 'results-tbody');
                document.getElementById('results-count').textContent = data.length;
                showPanel('results-panel');
            } else {
                document.getElementById('raw-result-code').textContent = queryResult;
                showPanel('raw-result-panel');
            }
        } catch {
            document.getElementById('raw-result-code').textContent = queryResult;
            showPanel('raw-result-panel');
        }
    } catch (err) {
        showAlert('error-alert', err.message, 'danger');
    } finally {
        isExecuting = false;
        btn.classList.remove('btn-executing');
        btn.innerHTML = `<span class="material-icons" style="font-size:18px;vertical-align:middle;">play_arrow</span> Execute <span id="btn-execute-mode">${selectedMode.toUpperCase()}</span>`;
    }
}

async function showTables() {
    hideAllAlerts();
    hidePanel('results-panel');
    hidePanel('raw-result-panel');

    try {
        let tablesResult;
        if (selectedBackend === 'duckdb') {
            tablesResult = await window.DuckDbInterop.queryJson('SHOW TABLES;');
        } else {
            tablesResult = await window.PGliteInterop.queryJson(
                "SELECT tablename AS table_name FROM pg_tables WHERE schemaname='public' ORDER BY tablename;"
            );
        }

        const data = JSON.parse(tablesResult);
        if (Array.isArray(data) && data.length > 0) {
            renderResults(data, 'results-thead', 'results-tbody');
            document.getElementById('results-count').textContent = data.length;
            showPanel('results-panel');
            showAlert('conversion-alert', `Found ${data.length} table(s) in the database.`, 'success');
        } else {
            showAlert('conversion-alert', 'No tables found in the database.', 'info');
        }
    } catch (err) {
        showAlert('error-alert', `Error checking tables: ${err.message}`, 'danger');
    }
}

function showPanel(id) {
    document.getElementById(id)?.classList.remove('d-none');
}

function hidePanel(id) {
    document.getElementById(id)?.classList.add('d-none');
}
