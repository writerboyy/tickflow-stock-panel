(function () {
  'use strict';

  if (!window.gp) throw new Error('gp.js 未加载');

  const CODE_INDEX_DATE = '20260625';
  const SUGGEST_ROW_HEIGHT = 32;
  const SUGGEST_BUFFER_ROWS = 10;

  const FIELD_MAP = {
    '日期/时间': 'date',
    '代码': 'code',
    '名称': 'name',
    '开盘价': 'open',
    '最高价': 'high',
    '最低价': 'low',
    '收盘价': 'close',
    '前收盘价': 'pre_close',
    '成交量': 'volume',
    '成交额': 'amount',
    '换手率': 'turnover',
    '涨幅%': 'pct_chg',
    '振幅%': 'amplitude',
    '是否ST': 'is_st',
    '量比': 'vol_ratio',
    '总股本': 'total_share',
    '流通股本': 'float_share',
    '总市值': 'total_mv',
    '流通市值': 'float_mv',
    '市盈率': 'pe_ttm',
    '市净率': 'pb',
  };
  const FIELD_ORDER = Object.values(FIELD_MAP);
  const FIELD_LABELS = Object.fromEntries(
    Object.entries(FIELD_MAP).map(([label, key]) => [key, label]),
  );

  const state = {
    codes: [],
    dates: [],
    datesCode: '',
    currentData: [],
    loadSerial: 0,
  };

  const el = {
    code: document.getElementById('codeInput'),
    start: document.getElementById('dateStart'),
    end: document.getElementById('dateEnd'),
    frequency: document.getElementById('frequencySelect'),
    fq: document.getElementById('fqSelect'),
    read: document.getElementById('readBtn'),
    export: document.getElementById('exportBtn'),
    hint: document.getElementById('dateHint'),
    msg: document.getElementById('message'),
    table: document.getElementById('tableWrap'),
    title: document.getElementById('sheetTitle'),
    rows: document.getElementById('rowCount'),
    status: document.getElementById('status'),
    codeSuggest: document.getElementById('codeSuggest'),
    startSuggest: document.getElementById('startSuggest'),
    endSuggest: document.getElementById('endSuggest'),
  };

  el.status.textContent = `接口：${window.baseurl || '127.0.0.1:7899'} · gp.js`;

  function normalizeCode(raw) {
    return String(raw || '')
      .trim()
      .toLowerCase()
      .replace(/^sz|^sh/, '')
      .replace(/\D/g, '');
  }

  function normalizeCodeSuggestQuery(raw) {
    const digits = normalizeCode(raw);
    return digits.length >= 6 ? digits.slice(0, -1) : digits;
  }

  function isValidCode(code) {
    return /^\d{6}$/.test(code);
  }

  function normalizeDate(raw) {
    const digits = String(raw || '').replace(/\D/g, '');
    return digits.length < 8 ? '' : digits.slice(0, 8);
  }

  function normalizeDateQuery(raw) {
    return String(raw || '').replace(/\D/g, '');
  }

  function normalizeDateSuggestQuery(raw) {
    const digits = normalizeDateQuery(raw);
    return digits.length >= 8 ? digits.slice(0, -1) : digits;
  }

  function displayDate(value) {
    const text = String(value || '');
    if (text.length === 8) return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
    if (text.length === 14) {
      return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)} `
        + `${text.slice(8, 10)}:${text.slice(10, 12)}:${text.slice(12, 14)}`;
    }
    return text;
  }

  function normalizeDateRangeInputs() {
    let start = normalizeDate(el.start.value);
    let end = normalizeDate(el.end.value);
    if (start && end && end < start) [start, end] = [end, start];
    if (start) el.start.value = displayDate(start);
    if (end) el.end.value = displayDate(end);
    return { start, end };
  }

  function setMessage(text, type = '') {
    el.msg.textContent = text || '';
    el.msg.className = `message ${type}`;
  }

  function debounce(fn, delay = 260) {
    let timer = 0;
    return (...args) => {
      clearTimeout(timer);
      timer = setTimeout(() => fn(...args), delay);
    };
  }

  function filterOptions(options, rawInput, normalizer = (value) => String(value || '')) {
    const query = normalizer(rawInput);
    if (!query) return options.slice();
    const starts = [];
    const contains = [];
    for (const item of options) {
      const value = normalizer(item);
      if (value.startsWith(query)) starts.push(item);
      else if (value.includes(query)) contains.push(item);
    }
    return starts.concat(contains);
  }

  function bindSuggest({ input, box, getOptions, normalizeInput, formatOption, onPick, onInput }) {
    const virtual = { matches: [], raf: 0 };

    function renderVirtual() {
      const visibleRows = Math.ceil(box.clientHeight / SUGGEST_ROW_HEIGHT) + SUGGEST_BUFFER_ROWS * 2;
      const first = Math.max(0, Math.floor(box.scrollTop / SUGGEST_ROW_HEIGHT) - SUGGEST_BUFFER_ROWS);
      const last = Math.min(virtual.matches.length, first + visibleRows);
      const visible = virtual.matches.slice(first, last);
      box.innerHTML = '';
      if (!visible.length) {
        box.classList.remove('open');
        return;
      }
      const canvas = document.createElement('div');
      canvas.className = 'suggest-canvas';
      canvas.style.height = `${virtual.matches.length * SUGGEST_ROW_HEIGHT}px`;
      box.appendChild(canvas);
      visible.forEach((item, index) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = formatOption(item);
        button.style.top = `${(first + index) * SUGGEST_ROW_HEIGHT}px`;
        button.addEventListener('mousedown', (event) => {
          event.preventDefault();
          onPick(item);
          box.classList.remove('open');
        });
        canvas.appendChild(button);
      });
      box.classList.add('open');
    }

    function show() {
      virtual.matches = filterOptions(getOptions(), input.value, normalizeInput);
      box.scrollTop = 0;
      box.classList.toggle('open', virtual.matches.length > 0);
      renderVirtual();
    }

    input.addEventListener('input', () => {
      if (onInput) onInput(input.value);
      show();
    });
    input.addEventListener('focus', show);
    input.addEventListener('click', show);
    input.addEventListener('blur', () => setTimeout(() => box.classList.remove('open'), 120));
    box.addEventListener('scroll', () => {
      if (!box.classList.contains('open')) return;
      if (virtual.raf) cancelAnimationFrame(virtual.raf);
      virtual.raf = requestAnimationFrame(() => {
        virtual.raf = 0;
        renderVirtual();
      });
    });
  }

  function normalizeScalarList(values, width) {
    const output = [];
    const seen = new Set();
    for (const value of Array.isArray(values) ? values : []) {
      const text = String(value === undefined || value === null ? '' : value).replace(/\D/g, '');
      const item = width ? text.slice(0, width) : text;
      if (item.length !== width || seen.has(item)) continue;
      seen.add(item);
      output.push(item);
    }
    return output;
  }

  async function initCodes() {
    setMessage('正在通过 gp.js 初始化代码列表...');
    try {
      const values = await gp.get({
        code: '*',
        start: CODE_INDEX_DATE,
        end: CODE_INDEX_DATE,
        frequency: '1d',
        fields: 'code',
        fq: null,
      });
      state.codes = normalizeScalarList(values, 6).sort();
      el.hint.textContent = `已加载 ${state.codes.length} 个代码，输入可筛选。`;
      setMessage('代码列表初始化完成。', 'ok');
    } catch (error) {
      el.hint.textContent = '未检测到代码列表，请确保数据库与 gp.js 可用。';
      setMessage(`代码初始化失败：${error.message}`, 'error');
    }
  }

  async function loadDatesForCode(code) {
    if (!isValidCode(code) || state.datesCode === code) return;
    state.dates = [];
    state.datesCode = code;
    el.hint.textContent = `正在通过 gp.js 加载 ${code} 的历史日期...`;
    try {
      const values = await gp.get({
        code,
        frequency: '1d',
        fields: 'date',
        desc: true,
        fq: null,
      });
      if (state.datesCode !== code) return;
      state.dates = normalizeScalarList(values, 8).sort((a, b) => b.localeCompare(a));
      if (!state.dates.length) {
        el.hint.textContent = `${code} 没有日期数据。`;
        return;
      }
      const newest = state.dates[0];
      const oldest = state.dates[state.dates.length - 1];
      el.hint.textContent = `个股: ${code}\n交易日: ${state.dates.length} 天\n区间: `
        + `${displayDate(oldest)} 至 ${displayDate(newest)}`;
      setMessage(`${code} 日期加载完成。`, 'ok');
    } catch (error) {
      if (state.datesCode !== code) return;
      state.dates = [];
      state.datesCode = '';
      el.hint.textContent = `${code} 日期加载失败。`;
      setMessage(`加载日期失败：${error.message}`, 'error');
    }
  }

  function stringifyCell(value) {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  function renderTable(rows) {
    el.rows.textContent = `${rows.length} 行`;
    if (!rows.length) {
      el.table.innerHTML = '<div class="empty">没有数据。</div>';
      return;
    }
    const columns = [];
    const seen = new Set();
    for (const field of FIELD_ORDER) {
      if (rows.some((row) => Object.prototype.hasOwnProperty.call(row, field))) {
        seen.add(field);
        columns.push(field);
      }
    }
    for (const row of rows) {
      for (const field of Object.keys(row)) {
        if (seen.has(field)) continue;
        seen.add(field);
        columns.push(field);
      }
    }
    const table = document.createElement('table');
    const thead = document.createElement('thead');
    const header = document.createElement('tr');
    for (const field of columns) {
      const th = document.createElement('th');
      th.textContent = FIELD_LABELS[field] || field;
      header.appendChild(th);
    }
    thead.appendChild(header);
    table.appendChild(thead);
    const tbody = document.createElement('tbody');
    for (const row of rows) {
      const tr = document.createElement('tr');
      for (const field of columns) {
        const td = document.createElement('td');
        td.textContent = stringifyCell(row[field]);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    el.table.replaceChildren(table);
  }

  function progressText(info) {
    if (!info || info.kind !== 'market') return null;
    if (Number.isFinite(info.percent)) return `正在读取行情：${info.percent.toFixed(1)}%`;
    if (Number.isFinite(info.loaded)) return `正在读取行情：${info.loaded.toLocaleString()} 字节`;
    return '正在读取行情...';
  }

  async function loadData() {
    const code = normalizeCode(el.code.value);
    const { start, end } = normalizeDateRangeInputs();
    const frequency = el.frequency.value;
    if (!isValidCode(code)) {
      setMessage('代码需要是 6 位数字。', 'error');
      return;
    }

    const serial = ++state.loadSerial;
    const useLatestRows = !start && !end;
    el.read.disabled = true;
    el.export.disabled = true;
    setMessage('正在通过 gp.js 获取并处理数据...');
    try {
      const rows = await gp.get({
        code,
        start: start || null,
        end: end || null,
        frequency,
        fields: null,
        limit: useLatestRows ? 100 : null,
        desc: true,
        fq: el.fq.value,
        onProgress(info) {
          if (serial !== state.loadSerial) return;
          const text = progressText(info);
          if (text) setMessage(text);
        },
      });
      if (serial !== state.loadSerial) return;
      const processed = (Array.isArray(rows) ? rows : [])
        .filter((row) => row && typeof row === 'object' && !Array.isArray(row));
      state.currentData = processed;
      renderTable(processed);
      el.title.textContent = useLatestRows
        ? `${code} [${frequency}] 最近 100 条`
        : `${code} [${frequency}] ${displayDate(start)} 至 ${displayDate(end || start)}`;
      if (!processed.length) {
        setMessage('数据库中未查询到有效记录。', 'error');
        return;
      }
      setMessage(`${code} 数据载入成功。`, 'ok');
      el.export.disabled = false;
    } catch (error) {
      if (serial !== state.loadSerial) return;
      state.currentData = [];
      renderTable([]);
      setMessage(`数据加载失败：${error.message}`, 'error');
    } finally {
      if (serial === state.loadSerial) el.read.disabled = false;
    }
  }

  function csvCell(value) {
    if (value === null || value === undefined) return '';
    const text = String(value).replace(/"/g, '""');
    return /[,\n"]/.test(text) ? `"${text}"` : text;
  }

  function exportToCsv() {
    if (!state.currentData.length) {
      setMessage('无数据可供导出。', 'error');
      return;
    }
    const columns = FIELD_ORDER.filter((field) => (
      state.currentData.some((row) => Object.prototype.hasOwnProperty.call(row, field))
    ));
    const headers = columns.map((field) => FIELD_LABELS[field] || field);
    const lines = [headers.join(',')];
    for (const row of state.currentData) lines.push(columns.map((field) => csvCell(row[field])).join(','));
    const blob = new Blob([`\ufeff${lines.join('\r\n')}\r\n`], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    const code = normalizeCode(el.code.value);
    const { start, end } = normalizeDateRangeInputs();
    link.href = url;
    link.download = `${code}_${el.frequency.value}_${start || 'all'}_to_${end || start || 'all'}.csv`;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
    setMessage('已成功导出 CSV。', 'ok');
  }

  const triggerDateInputLoad = debounce(() => {
    const code = normalizeCode(el.code.value);
    const start = normalizeDate(el.start.value);
    const endDigits = normalizeDateQuery(el.end.value);
    if (!isValidCode(code) || !start || (endDigits && endDigits.length < 8)) return;
    loadData();
  });

  const triggerCodeInputLoad = debounce(() => {
    if (isValidCode(normalizeCode(el.code.value))) loadData();
  });

  bindSuggest({
    input: el.code,
    box: el.codeSuggest,
    getOptions: () => state.codes,
    normalizeInput: normalizeCodeSuggestQuery,
    formatOption: (code) => code,
    onInput(raw) {
      const code = normalizeCode(raw);
      if (!isValidCode(code)) return;
      loadDatesForCode(code);
      triggerCodeInputLoad();
    },
    onPick(code) {
      el.code.value = code;
      loadDatesForCode(code);
      loadData();
    },
  });

  for (const [input, box] of [[el.start, el.startSuggest], [el.end, el.endSuggest]]) {
    bindSuggest({
      input,
      box,
      getOptions: () => state.dates,
      normalizeInput: normalizeDateSuggestQuery,
      formatOption: displayDate,
      onInput: triggerDateInputLoad,
      onPick(date) {
        input.value = displayDate(date);
        loadData();
      },
    });
  }

  el.start.addEventListener('blur', normalizeDateRangeInputs);
  el.end.addEventListener('blur', normalizeDateRangeInputs);
  el.code.addEventListener('blur', () => {
    const code = normalizeCode(el.code.value);
    el.code.value = code;
    if (!isValidCode(code)) return;
    loadDatesForCode(code);
    loadData();
  });
  el.read.addEventListener('click', loadData);
  el.export.addEventListener('click', exportToCsv);
  el.fq.addEventListener('change', loadData);
  el.frequency.addEventListener('change', loadData);
  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    loadData();
  });

  initCodes();
}());
