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

export function renderResults(data, theadId, tbodyId) {
    const thead = document.getElementById(theadId);
    const tbody = document.getElementById(tbodyId);
    if (!thead || !tbody) return;

    thead.innerHTML = '';
    tbody.innerHTML = '';

    const rows = rowsOf(data);
    if (rows.length === 0) return;

    // Header — preserve the column order reported by the engine when available.
    const columns = columnsOf(data).map(c => c.name);
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
            const val = row[col];
            td.textContent = val != null ? String(val) : '';
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
