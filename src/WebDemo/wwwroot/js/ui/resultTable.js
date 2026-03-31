// Render query results into an HTML table

export function renderResults(data, theadId, tbodyId) {
    const thead = document.getElementById(theadId);
    const tbody = document.getElementById(tbodyId);
    if (!thead || !tbody) return;

    thead.innerHTML = '';
    tbody.innerHTML = '';

    if (!data || data.length === 0) return;

    // Header
    const columns = Object.keys(data[0]);
    const headerRow = document.createElement('tr');
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;
        th.style.whiteSpace = 'nowrap';
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    // Body
    data.forEach(row => {
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
