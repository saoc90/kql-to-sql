// Query Editor UI logic — replaces Home.razor
import { showAlert, hideAlert, hideAllAlerts } from './notifications.js';
import { renderResults, clearResults, rowsOf, columnsOf } from './resultTable.js';
import { renderChart } from './chartRenderer.js';

const EDITOR_ID = 'monaco-editor-container';
const STORAGE_KEY = 'queryEditorState';
let selectedMode = 'kql';
let selectedBackend = 'duckdb';
let isExecuting = false;
let convertedSql = '';
let saveTimer = null;

function loadState() {
    try {
        return JSON.parse(localStorage.getItem(STORAGE_KEY)) || {};
    } catch { return {}; }
}

function saveState() {
    const editor = window.nativeMonacoEditor?.getEditor(EDITOR_ID);
    const query = editor ? editor.getValue() : '';
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ query, mode: selectedMode, backend: selectedBackend }));
}

function debouncedSave() {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(saveState, 500);
}

export async function initQueryEditor() {
    // Restore persisted state
    const saved = loadState();
    if (saved.mode) selectedMode = saved.mode;
    if (saved.backend) selectedBackend = saved.backend;

    // Mode toggle — restore and listen
    document.querySelectorAll('input[name="queryMode"]').forEach(radio => {
        if (radio.value === selectedMode) radio.checked = true;
        radio.addEventListener('change', (e) => { onModeChanged(e.target.value); debouncedSave(); });
    });
    document.getElementById('btn-execute-mode').textContent = selectedMode.toUpperCase();
    document.getElementById('btn-convert')?.classList.toggle('d-none', selectedMode !== 'kql');

    // Backend selector — restore and listen
    const backendSelect = document.getElementById('backend-select');
    if (backendSelect) {
        backendSelect.value = selectedBackend;
        backendSelect.addEventListener('change', (e) => { selectedBackend = e.target.value; debouncedSave(); });
    }

    // Buttons
    document.getElementById('btn-execute')?.addEventListener('click', run);
    document.getElementById('btn-convert')?.addEventListener('click', convertToSql);
    document.getElementById('btn-show-tables')?.addEventListener('click', showTables);
    document.getElementById('btn-copy-sql')?.addEventListener('click', copySql);
    document.getElementById('btn-use-sql')?.addEventListener('click', useConvertedSql);

    // Result view toggle (Chart vs Table) + keep the chart sized to its container
    document.querySelectorAll('input[name="resultView"]').forEach(radio => {
        radio.addEventListener('change', (e) => setResultView(e.target.value));
    });
    window.addEventListener('resize', () => { if (window.__resultChart) { try { window.__resultChart.resize(); } catch { /* noop */ } } });

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

            // Restore persisted query text, or seed the default example query on first load
            const saved = loadState();
            editor.setValue(saved.query || getDefaultQuery());
            const lang = selectedMode === 'kql' ? 'kusto' : 'sql';
            window.nativeMonacoEditor.setLanguage(EDITOR_ID, lang);

            // Save on every edit
            editor.onDidChangeModelContent(() => debouncedSave());

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
        ? 'StormEvents\n| summarize EventCount = count() by State\n| top 10 by EventCount\n| render columnchart'
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
        let renderInfo = null;

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
            renderInfo = result.render || null;   // chart metadata from a `| render` operator
        }

        // Execute against selected backend
        let queryResult;
        if (selectedBackend === 'duckdb') {
            queryResult = await window.DuckDbInterop.queryJson(sqlToExecute);
        } else {
            queryResult = await window.PGliteInterop.queryJson(sqlToExecute);
        }

        // Parse and render — always populate the table, then draw the chart if the query has `| render`.
        try {
            const data = JSON.parse(queryResult);
            const rows = rowsOf(data);
            if (rows.length > 0) {
                renderResults(data, 'results-thead', 'results-tbody');
                document.getElementById('results-count').textContent = rows.length;
                showPanel('results-panel');
                await showResultViews(renderInfo, data);
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
        const rows = rowsOf(data);
        if (rows.length > 0) {
            renderResults(data, 'results-thead', 'results-tbody');
            document.getElementById('results-count').textContent = rows.length;
            showPanel('results-panel');
            await showResultViews(null, data);   // plain table, no chart
            showAlert('conversion-alert', `Found ${rows.length} table(s) in the database.`, 'success');
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

// ----- Result view (Chart vs Table), ADX-style -------------------------------------------

function setResultView(view) {
    const chartWrap = document.getElementById('results-chart-wrap');
    const tableWrap = document.getElementById('results-table-wrap');
    if (view === 'chart') {
        chartWrap?.classList.remove('d-none');
        tableWrap?.classList.add('d-none');
        const r = document.getElementById('result-view-chart'); if (r) r.checked = true;
        // ECharts mis-sizes when initialized in a hidden container — resize once visible.
        if (window.__resultChart) { try { window.__resultChart.resize(); } catch { /* noop */ } }
    } else {
        tableWrap?.classList.remove('d-none');
        chartWrap?.classList.add('d-none');
        const r = document.getElementById('result-view-table'); if (r) r.checked = true;
    }
}

// Draw the chart for `renderInfo` (if present) and pick the default view: Chart when the query has a
// real `| render` chart, otherwise Table. table/pivotchart/timepivot fall back to the table with a note.
async function showResultViews(renderInfo, data) {
    const toggle = document.getElementById('result-view-toggle');
    const note = document.getElementById('chart-note');
    if (note) { note.classList.add('d-none'); note.textContent = ''; }

    if (!renderInfo) {
        toggle?.classList.add('d-none');
        setResultView('table');
        return;
    }

    const chartEl = document.getElementById('results-chart');
    let res;
    try {
        res = await renderChart(renderInfo, columnsOf(data), rowsOf(data), chartEl);
    } catch (err) {
        console.error('[QueryEditor] chart render failed:', err);
        if (note) { note.textContent = `Chart rendering failed: ${err?.message || err}`; note.classList.remove('d-none'); }
        toggle?.classList.add('d-none');
        setResultView('table');
        return;
    }

    if (res && res.table) {
        if (res.note && note) { note.textContent = res.note; note.classList.remove('d-none'); }
        toggle?.classList.add('d-none');
        setResultView('table');
        return;
    }

    // Real chart (or card) — show the toggle and default to the Chart view.
    toggle?.classList.remove('d-none');
    setResultView('chart');
}
