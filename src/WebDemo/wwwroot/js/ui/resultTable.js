// Render query results into an HTML table.
//
// The interop layer returns an envelope { columns:[{name,type}], rows:[...] }. These helpers also
// accept a bare rows array so older call sites (and any legacy shape) keep working unchanged.

/** Returns the row objects regardless of whether `data` is an envelope or a bare array. */
export function rowsOf(data) {
    if (Array.isArray(data)) return data;
    if (data && Array.isArray(data.rows)) return data.rows;
    return [];
}

/** Returns [{name,type}] — explicit when the envelope carries it, else derived from the first row's keys. */
export function columnsOf(data) {
    if (data && Array.isArray(data.columns) && data.columns.length) return data.columns;
    const rows = rowsOf(data);
    if (rows.length) return Object.keys(rows[0]).map(name => ({ name, type: 'string' }));
    return [];
}

/** Formats a Date as "2007-01-01 00:00:00" (UTC wall-clock — KQL datetimes are tz-agnostic). */
function formatDate(d) {
    if (isNaN(d.getTime())) return '';
    const p = n => String(n).padStart(2, '0');
    return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} `
        + `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
}

/** Tidies an ISO timestamp string (2007-01-01T00:00:00.000Z) to "2007-01-01 00:00:00". */
function formatDateTimeString(s) {
    const m = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})/.exec(s);
    return m ? `${m[1]} ${m[2]}` : s;
}

/** Renders a single cell value to text. Datetimes are shown as a readable date regardless of whether
 *  the engine handed back a Date, an ISO string, or epoch ms/µs. Objects fall back to JSON so a stray
 *  value never shows as the useless "[object Object]". */
function formatCell(val, type) {
    if (val == null) return '';
    if (val instanceof Date) return formatDate(val);
    if (type === 'datetime') {
        if (typeof val === 'number') {
            // Arrow may report timestamps as epoch ms or µs depending on the column unit.
            return formatDate(new Date(Math.abs(val) > 1e14 ? val / 1000 : val));
        }
        if (typeof val === 'string') return formatDateTimeString(val);
    }
    if (typeof val === 'object') return JSON.stringify(val);
    return String(val);
}

export function renderResults(data, theadId, tbodyId) {
    const thead = document.getElementById(theadId);
    const tbody = document.getElementById(tbodyId);
    if (!thead || !tbody) return;

    thead.innerHTML = '';
    tbody.innerHTML = '';

    const rows = rowsOf(data);
    if (rows.length === 0) return;

    // Header — preserve the column order reported by the engine when available.
    const cols = columnsOf(data);
    const columns = cols.map(c => c.name);
    const typeByName = Object.fromEntries(cols.map(c => [c.name, c.type]));
    const headerRow = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;
        th.style.whiteSpace = 'nowrap';
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    // Body
    rows.forEach(row => {
        const tr = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            td.textContent = formatCell(row[col], typeByName[col]);
            td.title = td.textContent;
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
}

export function clearResults(theadId, tbodyId) {
    const thead = document.getElementById(theadId);
    const tbody = document.getElementById(tbodyId);
    if (thead) thead.innerHTML = '';
    if (tbody) tbody.innerHTML = '';
}
