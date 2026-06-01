// Kusto-compatible `| render` chart rendering on top of Apache ECharts.
//
// `buildChartOption` is pure (returns a plain ECharts option, or a sentinel for the kinds that fall
// back to the data table / a card) so it can be unit-tested without a DOM. `renderChart` lazily loads
// ECharts (so non-charting sessions never pay the bundle cost), then draws into a container element.
//
// renderInfo uses PascalCase keys mirroring Kusto's @ExtendedProperties "Visualization" annotation:
//   Visualization, Title, XColumn, Series[], YColumns[], AnomalyColumns[], XTitle, YTitle,
//   XAxis(linear|log), YAxis(linear|log), Legend(visible|hidden), YSplit, Accumulate, Kind, Ymin, Ymax …

const ECHARTS_URL = 'https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.esm.min.js';
let echartsLib = null;

async function loadECharts() {
    if (!echartsLib) echartsLib = await import(ECHARTS_URL);
    return echartsLib;
}

/** A render kind has a chart equivalent? (table/pivotchart/timepivot do not.) */
export function isChartable(viz) {
    const v = (viz || '').toLowerCase();
    return !!v && v !== 'table' && v !== 'pivotchart' && v !== 'timepivot';
}

// ---- small value/type helpers -------------------------------------------------------------

function typeOf(columns, name) {
    const c = columns.find(c => c.name === name);
    return c ? c.type : 'string';
}
const isNum = t => t === 'number';
const isDate = t => t === 'datetime';

function toMillis(v) {
    if (v == null) return null;
    if (v instanceof Date) return v.getTime();
    if (typeof v === 'number') return v;
    const t = Date.parse(v);
    return isNaN(t) ? null : t;
}
function toNumber(v) {
    if (v == null || v === '') return null;
    if (typeof v === 'number') return v;
    const n = Number(v);
    return isNaN(n) ? null : n;
}

// Default column→axis mapping (applied only where renderInfo doesn't specify it explicitly).
function resolveAxes(ri, columns) {
    const names = columns.map(c => c.name);
    const viz = (ri.Visualization || '').toLowerCase();
    const numeric = names.filter(n => isNum(typeOf(columns, n)));
    const datetimes = names.filter(n => isDate(typeOf(columns, n)));
    const strings = names.filter(n => typeOf(columns, n) === 'string');

    let xName = ri.XColumn || null;
    if (!xName) {
        if (viz === 'timechart' || viz === 'anomalychart') xName = datetimes[0] || names[0];
        else xName = names[0];
    }

    let seriesNames = (ri.Series && ri.Series.length) ? ri.Series.slice() : null;
    if (!seriesNames) {
        if (viz === 'timechart' || viz === 'anomalychart') {
            const s = strings.filter(n => n !== xName);
            seriesNames = s.length ? [s[0]] : [];
        } else {
            seriesNames = [];
        }
    }

    let yNames = (ri.YColumns && ri.YColumns.length) ? ri.YColumns.slice() : null;
    if (!yNames) {
        const used = new Set([xName, ...seriesNames]);
        yNames = numeric.filter(n => !used.has(n));
        if (!yNames.length) yNames = names.filter(n => !used.has(n)); // last resort
    }
    return { xName, yNames, seriesNames };
}

function logIf(axisSetting) {
    return (axisSetting || '').toLowerCase() === 'log' ? 'log' : 'value';
}
function applyBounds(axis, ri) {
    const lo = ri.Ymin, hi = ri.Ymax;
    const n = v => (v == null || v === '' || String(v).toLowerCase() === 'nan') ? null : Number(v);
    if (n(lo) != null) axis.min = n(lo);
    if (n(hi) != null) axis.max = n(hi);
}

// ---- per-kind builders --------------------------------------------------------------------

export function buildChartOption(ri, columns, rows) {
    const viz = (ri.Visualization || 'table').toLowerCase();

    if (viz === 'table') return { __table: true };
    if (viz === 'pivotchart' || viz === 'timepivot') {
        return { __table: true, note: `render ${viz} is an interactive Kusto.Explorer-only visual — showing the data table instead.` };
    }
    if (viz === 'card') {
        return { __card: true, cards: columns.map(c => ({ label: c.name, value: rows.length ? rows[0][c.name] : null })) };
    }

    const title = ri.Title ? { text: ri.Title, left: 'center' } : undefined;
    const legend = { show: (ri.Legend || '').toLowerCase() !== 'hidden', type: 'scroll', bottom: 0 };

    if (viz === 'piechart') return buildPie(ri, columns, rows, title, legend);
    if (viz === 'treemap') return buildTreemap(ri, columns, rows, title);
    if (viz === 'ladderchart') return buildLadder(ri, columns, rows, title);

    return buildAxisChart(viz, ri, columns, rows, title, legend);
}

function buildPie(ri, columns, rows, title, legend) {
    const { xName, yNames } = resolveAxes(ri, columns);
    const labelCol = ri.XColumn || xName || columns[0]?.name;
    // first numeric value column, else the second column
    const valueCol = (ri.YColumns && ri.YColumns[0]) || yNames[0] || columns[1]?.name || columns[0]?.name;
    const data = rows.map(r => ({ name: String(r[labelCol]), value: toNumber(r[valueCol]) }));
    return {
        title, legend, tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
        series: [{ type: 'pie', radius: ['35%', '70%'], data, label: { show: true } }]
    };
}

function buildTreemap(ri, columns, rows, title) {
    const names = columns.map(c => c.name);
    const valueCol = (ri.YColumns && ri.YColumns[0]) || names.filter(n => isNum(typeOf(columns, n))).pop() || names[names.length - 1];
    const pathCols = names.filter(n => n !== valueCol);
    // Build a nested hierarchy from the path columns.
    const root = { children: [] };
    for (const r of rows) {
        let node = root;
        for (const pc of pathCols) {
            const key = String(r[pc]);
            let child = node.children.find(c => c.name === key);
            if (!child) { child = { name: key, children: [] }; node.children.push(child); }
            node = child;
        }
        node.value = (node.value || 0) + (toNumber(r[valueCol]) || 0);
    }
    const strip = n => { if (n.children && n.children.length) n.children.forEach(strip); else delete n.children; };
    root.children.forEach(strip);
    return {
        title, tooltip: { trigger: 'item' },
        series: [{ type: 'treemap', data: root.children, label: { show: true }, breadcrumb: { show: true } }]
    };
}

// Ladder: last two columns are an x-range [start,end]; remaining columns identify the row (y category).
function buildLadder(ri, columns, rows, title) {
    const names = columns.map(c => c.name);
    if (names.length < 2) return { __table: true, note: 'ladderchart needs at least two columns.' };
    const startCol = names[names.length - 2], endCol = names[names.length - 1];
    const catCols = names.slice(0, names.length - 2);
    const cats = rows.map(r => catCols.length ? catCols.map(c => r[c]).join(' / ') : '');
    const timeAxis = isDate(typeOf(columns, startCol)) || isDate(typeOf(columns, endCol));
    const conv = timeAxis ? toMillis : toNumber;
    const data = rows.map((r, i) => ({ value: [conv(r[startCol]), conv(r[endCol]), i], name: cats[i] }));
    return {
        title,
        tooltip: { trigger: 'item' },
        grid: { containLabel: true, left: 8, right: 24, top: title ? 40 : 16, bottom: 24 },
        xAxis: { type: timeAxis ? 'time' : 'value', name: ri.XTitle },
        yAxis: { type: 'category', data: cats, name: ri.YTitle },
        series: [{
            type: 'custom',
            renderItem(params, api) {
                const start = api.coord([api.value(0), params.dataIndex]);
                const end = api.coord([api.value(1), params.dataIndex]);
                const h = Math.max(6, api.size([0, 1])[1] * 0.6);
                return {
                    type: 'rect',
                    shape: { x: start[0], y: start[1] - h / 2, width: Math.max(1, end[0] - start[0]), height: h },
                    style: api.style()
                };
            },
            encode: { x: [0, 1], y: 2 },
            data
        }]
    };
}

function buildAxisChart(viz, ri, columns, rows, title, legend) {
    const { xName, yNames, seriesNames } = resolveAxes(ri, columns);

    const horizontal = viz === 'barchart';
    const isArea = viz === 'areachart' || viz === 'stackedareachart';
    const kind = (ri.Kind || '').toLowerCase();
    const stacked = kind === 'stacked' || kind === 'stacked100' || viz === 'stackedareachart';
    const stacked100 = kind === 'stacked100';
    const accumulate = ri.Accumulate === true;

    const seriesType =
        viz === 'scatterchart' ? 'scatter' :
        (viz === 'barchart' || viz === 'columnchart') ? 'bar' : 'line';

    // x-axis mode
    let xMode;
    if (viz === 'timechart' || viz === 'anomalychart') xMode = 'time';
    else if (viz === 'barchart' || viz === 'columnchart') xMode = 'category';
    else if (isDate(typeOf(columns, xName))) xMode = 'time';
    else if (viz === 'scatterchart') xMode = isNum(typeOf(columns, xName)) ? 'value' : 'category';
    else xMode = isNum(typeOf(columns, xName)) ? 'value' : 'category';

    const categories = xMode === 'category' ? rows.map(r => r[xName]) : undefined;
    const series = [];

    if (xMode === 'category') {
        // One series per y-column, values aligned to the category axis. (Series grouping isn't
        // meaningful on a shared category axis — Kusto bar/column charts use multiple y-columns.)
        for (const y of yNames) series.push(mkSeries(y, seriesType, isArea, rows.map(r => toNumber(r[y]))));
    } else {
        const xconv = xMode === 'time' ? toMillis : toNumber;
        const groups = new Map();
        if (seriesNames.length) {
            for (const r of rows) {
                const key = seriesNames.map(s => r[s]).join(' / ');
                (groups.get(key) || groups.set(key, []).get(key)).push(r);
            }
        } else {
            groups.set(null, rows);
        }
        for (const y of yNames) {
            for (const [key, grp] of groups) {
                const label = key == null ? y : (yNames.length > 1 ? `${key} · ${y}` : key);
                const data = grp.map(r => [xconv(r[xName]), toNumber(r[y])]).filter(p => p[0] != null);
                series.push(mkSeries(label, seriesType, isArea, data));
            }
        }
    }

    if (stacked) series.forEach(s => { s.stack = 'total'; if (isArea) s.areaStyle = s.areaStyle || {}; });
    if (accumulate) series.forEach(accumulateSeries);
    if (stacked100 && xMode === 'category') normalize100(series);

    // anomaly overlay: each anomaly column drawn as scatter points
    if (viz === 'anomalychart' && ri.AnomalyColumns && ri.AnomalyColumns.length && xMode !== 'category') {
        const xconv = toMillis;
        for (const a of ri.AnomalyColumns) {
            const data = rows.map(r => [xconv(r[xName]), toNumber(r[a])]).filter(p => p[0] != null && p[1] != null);
            series.push({ name: a, type: 'scatter', symbolSize: 9, itemStyle: { color: '#d62728' }, data });
        }
    }

    const valueAxis = { type: logIf(ri.YAxis), name: ri.YTitle, scale: true };
    applyBounds(valueAxis, ri);
    const catOrTime = {
        type: xMode === 'time' ? 'time' : (xMode === 'value' ? logIf(ri.XAxis) : 'category'),
        name: ri.XTitle
    };
    if (categories) catOrTime.data = categories;

    const opt = {
        title, legend,
        tooltip: { trigger: seriesType === 'scatter' ? 'item' : 'axis' },
        grid: { containLabel: true, left: 12, right: 24, top: title ? 56 : 28, bottom: legend.show ? 40 : 24 },
        series
    };
    if (horizontal) { opt.xAxis = valueAxis; opt.yAxis = catOrTime; }
    else { opt.xAxis = catOrTime; opt.yAxis = valueAxis; }
    return opt;
}

function mkSeries(name, type, isArea, data) {
    const s = { name: String(name), type, data, showSymbol: data.length <= 200 };
    if (isArea) s.areaStyle = {};
    if (type === 'line') s.smooth = false;
    return s;
}

function accumulateSeries(s) {
    let acc = 0;
    s.data = s.data.map(p => {
        if (Array.isArray(p)) { acc += (p[1] || 0); return [p[0], acc]; }
        acc += (p || 0); return acc;
    });
}

// Normalize aligned numeric series so each category sums to 100%.
function normalize100(series) {
    if (!series.length) return;
    const n = series[0].data.length;
    for (let i = 0; i < n; i++) {
        let total = 0;
        for (const s of series) total += (s.data[i] || 0);
        if (total) for (const s of series) s.data[i] = (s.data[i] || 0) / total * 100;
    }
}

function chartTheme() {
    const attr = document.documentElement.getAttribute('data-bs-theme');
    if (attr === 'dark') return 'dark';
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) return 'dark';
    return undefined;
}

function renderCard(cards, containerEl) {
    containerEl.innerHTML = '';
    const wrap = document.createElement('div');
    wrap.style.cssText = 'display:flex;flex-wrap:wrap;gap:1.5rem;align-items:center;justify-content:center;height:100%;';
    for (const c of cards) {
        const cell = document.createElement('div');
        cell.style.cssText = 'text-align:center;min-width:120px;';
        const val = document.createElement('div');
        val.style.cssText = 'font-size:2.2rem;font-weight:600;line-height:1.1;';
        val.textContent = c.value != null ? String(c.value) : '—';
        const lbl = document.createElement('div');
        lbl.className = 'text-muted';
        lbl.style.cssText = 'font-size:0.85rem;margin-top:0.25rem;';
        lbl.textContent = c.label;
        cell.appendChild(val); cell.appendChild(lbl);
        wrap.appendChild(cell);
    }
    containerEl.appendChild(wrap);
}

/**
 * Render a chart for the given render metadata + typed columns + rows into `containerEl`.
 * Returns { table:true, note? } when the kind falls back to the data table, { card:true } for a card,
 * or { chart:true } when an ECharts chart was drawn.
 */
export async function renderChart(renderInfo, columns, rows, containerEl) {
    const opt = buildChartOption(renderInfo, columns, rows);

    // Dispose any previous chart instance bound to this container.
    if (containerEl.__chart) { try { containerEl.__chart.dispose(); } catch { /* noop */ } containerEl.__chart = null; }

    if (opt.__table) { window.__resultChart = null; return { table: true, note: opt.note }; }
    if (opt.__card) { window.__resultChart = null; renderCard(opt.cards, containerEl); return { card: true }; }

    const echarts = await loadECharts();
    containerEl.innerHTML = ''; // clear any leftover card markup before ECharts takes over the node
    const inst = echarts.init(containerEl, chartTheme(), { renderer: 'canvas' });
    inst.setOption(opt, true);
    containerEl.__chart = inst;
    window.__resultChart = inst;
    return { chart: true };
}
