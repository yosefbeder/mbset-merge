"""
templates.py — Jinja2 HTML templates for the MBSet Merge web app.

Exports:
    UPLOAD_TEMPLATE  — upload form page
    REVIEW_TEMPLATE  — interactive conflict-review page
    RESULTS_TEMPLATE — results page with interactive merge report
"""

# ─── Shared Design System ─────────────────────────────────────────────────────

_BASE_STYLE = """
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg: #070711;
      --surface: rgba(255,255,255,0.04);
      --surface-2: rgba(255,255,255,0.07);
      --border: rgba(255,255,255,0.08);
      --border-strong: rgba(255,255,255,0.15);
      --primary: #7c6aff;
      --primary-dim: rgba(124,106,255,0.15);
      --primary-glow: rgba(124,106,255,0.3);
      --accent: #06b6d4;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --text: #f1f5f9;
      --muted: #94a3b8;
      --subtle: #475569;
    }

    html { height: 100%; }

    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      line-height: 1.6;
    }

    body::before {
      content: '';
      position: fixed;
      inset: 0;
      background:
        radial-gradient(ellipse 70% 50% at 15% -10%, rgba(124,106,255,0.14) 0%, transparent 60%),
        radial-gradient(ellipse 50% 40% at 85% 110%, rgba(6,182,212,0.07) 0%, transparent 55%);
      pointer-events: none;
      z-index: 0;
    }

    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 20px;
      backdrop-filter: blur(24px);
      -webkit-backdrop-filter: blur(24px);
    }

    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 12px 22px;
      border: none;
      border-radius: 12px;
      font-family: inherit;
      font-size: 0.925rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.18s ease;
    }

    .btn-primary {
      background: linear-gradient(135deg, var(--primary), #5e4fd9);
      color: #fff;
      box-shadow: 0 4px 20px var(--primary-glow);
    }
    .btn-primary:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 8px 32px var(--primary-glow); }
    .btn-primary:active:not(:disabled) { transform: translateY(0); }
    .btn-primary:disabled { opacity: 0.4; cursor: not-allowed; }

    .btn-secondary {
      background: var(--surface-2);
      color: var(--muted);
      border: 1px solid var(--border);
    }
    .btn-secondary:hover:not(:disabled) { background: rgba(255,255,255,0.1); color: var(--text); }
    .btn-secondary:disabled { opacity: 0.35; cursor: not-allowed; }

    .btn-success {
      background: linear-gradient(135deg, var(--success), #059669);
      color: #fff;
      box-shadow: 0 4px 20px rgba(16,185,129,0.25);
    }
    .btn-success:hover { transform: translateY(-1px); box-shadow: 0 8px 32px rgba(16,185,129,0.3); }

    input[type="text"], input[type="file"] {
      width: 100%;
      background: rgba(255,255,255,0.04);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 11px 14px;
      color: var(--text);
      font-family: inherit;
      font-size: 0.9rem;
      outline: none;
      transition: border-color 0.18s, box-shadow 0.18s;
    }
    input[type="text"]:focus {
      border-color: var(--primary);
      box-shadow: 0 0 0 3px rgba(124,106,255,0.15);
    }
    input[type="text"]::placeholder { color: var(--subtle); }

    pre {
      background: rgba(0,0,0,0.35);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
      font-size: 0.78rem;
      line-height: 1.7;
      overflow-x: auto;
      white-space: pre-wrap;
      word-break: break-word;
      color: #cbd5e1;
      max-height: 420px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 3px 10px;
      border-radius: 20px;
      font-size: 0.75rem;
      font-weight: 600;
    }
    .badge-warning { background: rgba(245,158,11,0.15); color: var(--warning); border: 1px solid rgba(245,158,11,0.3); }
    .badge-danger  { background: rgba(239,68,68,0.15);  color: #f87171;         border: 1px solid rgba(239,68,68,0.3); }
    .badge-success { background: rgba(16,185,129,0.15); color: #34d399;         border: 1px solid rgba(16,185,129,0.3); }
    .badge-info    { background: rgba(6,182,212,0.12);  color: var(--accent);   border: 1px solid rgba(6,182,212,0.25); }

    @keyframes spin    { to { transform: rotate(360deg); } }
    @keyframes fadeIn  { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: none; } }
    @keyframes pulse   { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }
    @keyframes slideDown { from { opacity: 0; transform: translateY(-6px); } to { opacity: 1; transform: none; } }
"""


# ─── Upload Template ──────────────────────────────────────────────────────────

UPLOAD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="MBSet Merge - Deduplicate and merge question bank datasets with fuzzy matching and priority-based tagging.">
  <title>MBSet Merge</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    """ + _BASE_STYLE + """

    body { display: flex; align-items: center; justify-content: center; padding: 32px 16px; }

    .wrap {
      position: relative;
      z-index: 1;
      width: 100%;
      max-width: 520px;
      animation: fadeIn 0.5s ease;
    }

    .brand { text-align: center; margin-bottom: 36px; }

    .brand-logo {
      width: 60px;
      height: 60px;
      border-radius: 18px;
      background: linear-gradient(135deg, var(--primary), var(--accent));
      display: inline-flex;
      align-items: center;
      justify-content: center;
      margin-bottom: 16px;
      box-shadow: 0 0 50px var(--primary-glow), 0 0 0 1px rgba(124,106,255,0.2);
    }

    h1 {
      font-size: 2.1rem;
      font-weight: 800;
      letter-spacing: -0.04em;
      background: linear-gradient(135deg, #fff 30%, #94a3b8 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }

    .tagline { color: var(--muted); font-size: 0.875rem; margin-top: 6px; }

    .card { padding: 32px; }

    .error-box {
      background: rgba(239,68,68,0.1);
      border: 1px solid rgba(239,68,68,0.25);
      border-radius: 10px;
      padding: 12px 16px;
      color: #fca5a5;
      font-size: 0.85rem;
      margin-bottom: 20px;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .drop-zone {
      position: relative;
      border: 2px dashed var(--border);
      border-radius: 14px;
      padding: 36px 24px;
      text-align: center;
      cursor: pointer;
      transition: border-color 0.2s, background 0.2s;
      margin-bottom: 6px;
    }
    .drop-zone:hover, .drop-zone.dragover {
      border-color: var(--primary);
      background: var(--primary-dim);
    }
    .drop-zone input[type="file"] {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      opacity: 0;
      cursor: pointer;
    }

    .drop-icon {
      width: 44px;
      height: 44px;
      margin: 0 auto 12px;
      background: var(--primary-dim);
      border-radius: 12px;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .drop-label { font-size: 0.95rem; font-weight: 600; }
    .drop-hint  { font-size: 0.8rem; color: var(--muted); margin-top: 4px; }

    .file-info {
      display: none;
      align-items: center;
      gap: 8px;
      margin-top: 10px;
      padding: 8px 12px;
      background: var(--primary-dim);
      border-radius: 8px;
      font-size: 0.82rem;
      color: #a5b4fc;
      font-weight: 500;
    }

    .size-warn {
      display: none;
      align-items: center;
      gap: 6px;
      margin: 8px 0 4px;
      font-size: 0.8rem;
      color: var(--warning);
    }

    .field { margin-bottom: 20px; }
    .field label { display: block; font-size: 0.8rem; font-weight: 600; color: var(--muted); margin-bottom: 7px; letter-spacing: 0.02em; text-transform: uppercase; }

    .tags-container {
      display: none;
      margin-top: 20px;
      background: var(--surface-2);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
    }
    .tags-title {
      font-size: 0.85rem; font-weight: 600; color: var(--text); margin-bottom: 8px;
    }
    .tags-subtitle {
      font-size: 0.75rem; color: var(--muted); margin-bottom: 12px;
    }
    .tag-list {
      list-style: none;
      display: flex;
      flex-direction: column;
      gap: 6px;
      max-height: 200px;
      overflow-y: auto;
      padding-right: 4px;
    }
    .tag-item {
      display: flex;
      align-items: center;
      gap: 10px;
      background: var(--surface);
      padding: 8px 12px;
      border: 1px solid var(--border);
      border-radius: 8px;
      cursor: grab;
      user-select: none;
    }
    .tag-item:active { cursor: grabbing; }
    .tag-item.dragging { opacity: 0.5; background: var(--primary-dim); }
    .tag-checkbox { width: 16px; height: 16px; cursor: pointer; }
    .tag-name { font-size: 0.85rem; color: var(--text); flex: 1; }
    .tag-drag-handle { color: var(--muted); cursor: grab; }

    .submit-btn { width: 100%; font-size: 1rem; padding: 14px; border-radius: 13px; margin-top: 4px; }

    .overlay {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(7,7,17,0.88);
      backdrop-filter: blur(10px);
      -webkit-backdrop-filter: blur(10px);
      z-index: 999;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 28px;
    }
    .overlay.active { display: flex; }

    .spinner {
      width: 56px;
      height: 56px;
      border: 3px solid rgba(124,106,255,0.2);
      border-top-color: var(--primary);
      border-radius: 50%;
      animation: spin 0.75s linear infinite;
    }

    .overlay-body { text-align: center; }
    .overlay-title { font-size: 1.1rem; font-weight: 700; margin-bottom: 6px; }
    .overlay-sub   { font-size: 0.85rem; color: var(--muted); animation: pulse 1.8s ease infinite; }

    .features { display: flex; gap: 12px; margin-top: 20px; flex-wrap: wrap; }
    .feature {
      flex: 1;
      min-width: 120px;
      background: var(--surface-2);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      font-size: 0.78rem;
      color: var(--muted);
      text-align: center;
    }
    .feature strong { display: block; color: var(--text); font-size: 0.85rem; margin-bottom: 2px; }
  </style>
</head>
<body>

<div class="overlay" id="overlay">
  <div class="spinner"></div>
  <div class="overlay-body">
    <p class="overlay-title">Analyzing dataset…</p>
    <p class="overlay-sub" id="overlay-sub">Running fuzzy matching. Please wait.</p>
  </div>
</div>

<div class="wrap">
  <div class="brand">
    <div class="brand-logo">
      <svg width="28" height="28" viewBox="0 0 24 24" fill="white">
        <path d="M4 6h16v2H4zm2 5h12v2H6zm2 5h8v2H8z"/>
      </svg>
    </div>
    <h1>MBSet Merge</h1>
    <p class="tagline">Fuzzy-match &amp; deduplicate question banks with smart priority merging.</p>
  </div>

  <div class="card">
    {% if error %}
    <div class="error-box">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg>
      {{ error }}
    </div>
    {% endif %}

    <form id="upload-form" action="/process" method="POST" enctype="multipart/form-data">
      <div class="drop-zone" id="drop-zone">
        <input type="file" id="file-input" name="file" accept=".xlsx,.xls,.csv" required>
        <div class="drop-icon">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#7c6aff" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5"/>
          </svg>
        </div>
        <p class="drop-label">Drop your dataset here</p>
        <p class="drop-hint">or click to browse &mdash; .xlsx &nbsp;.xls &nbsp;.csv</p>
        <div class="file-info" id="file-info">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
          <span id="file-name"></span>
        </div>
      </div>

      <div class="size-warn" id="size-warn">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z"/></svg>
        Large file &mdash; processing may take a minute or two.
      </div>

      <input type="hidden" id="priority" name="priority" value="">
      
      <div class="tags-container" id="tags-container">
        <div class="tags-title">Configure Sources</div>
        <div class="tags-subtitle">Check the tags that represent sources, and drag to sort them by priority (highest priority at the top).</div>
        <ul class="tag-list" id="tag-list"></ul>
      </div>

      <button type="submit" class="btn btn-primary submit-btn" id="submit-btn" disabled>
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z"/>
        </svg>
        Process Dataset
      </button>
    </form>

    <div class="features">
      <div class="feature"><strong>≥ 95% Fuzzy</strong>RapidFuzz similarity</div>
      <div class="feature"><strong>Priority Tags</strong>Source-aware merging</div>
      <div class="feature"><strong>Manual Review</strong>Resolve conflicts</div>
    </div>
  </div>
</div>

<script>
  const dropZone   = document.getElementById('drop-zone');
  const fileInput  = document.getElementById('file-input');
  const fileInfo   = document.getElementById('file-info');
  const fileName   = document.getElementById('file-name');
  const sizeWarn   = document.getElementById('size-warn');
  const overlay    = document.getElementById('overlay');
  const overlaySub = document.getElementById('overlay-sub');
  const form       = document.getElementById('upload-form');
  const LARGE      = 300 * 1024; // 300 KB

  function validateSources() {
    const checkboxes = document.querySelectorAll('.tag-checkbox');
    const submitBtn = document.getElementById('submit-btn');
    if (checkboxes.length === 0) {
      submitBtn.disabled = false;
      return;
    }
    let anyChecked = false;
    for (let i = 0; i < checkboxes.length; i++) {
      if (checkboxes[i].checked) {
        anyChecked = true;
        break;
      }
    }
    submitBtn.disabled = !anyChecked;
  }

  function onFile(file) {
    if (!file) return;
    fileName.textContent = file.name;
    fileInfo.style.display = 'flex';
    sizeWarn.style.display = file.size > LARGE ? 'flex' : 'none';
    
    const fd = new FormData();
    fd.append('file', file);
    
    document.getElementById('tags-container').style.display = 'none';
    document.getElementById('submit-btn').disabled = true;
    
    fetch('/extract_tags', { method: 'POST', body: fd })
      .then(r => r.json())
      .then(data => {
        if (data.tags && data.tags.length > 0) {
          renderTags(data.tags);
        } else {
          document.getElementById('tag-list').innerHTML = '';
          validateSources();
        }
      })
      .catch(err => {
        console.error(err);
        validateSources();
      });
  }

  function renderTags(tags) {
    const container = document.getElementById('tags-container');
    const list = document.getElementById('tag-list');
    list.innerHTML = '';
    
    tags.forEach(tag => {
      const li = document.createElement('li');
      li.className = 'tag-item';
      li.draggable = true;
      
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.className = 'tag-checkbox';
      cb.value = tag;
      
      const span = document.createElement('span');
      span.className = 'tag-name';
      span.textContent = tag;
      
      const handle = document.createElement('span');
      handle.className = 'tag-drag-handle';
      handle.innerHTML = '☰';
      
      cb.addEventListener('change', validateSources);
      
      li.appendChild(cb);
      li.appendChild(span);
      li.appendChild(handle);
      
      li.addEventListener('dragstart', () => li.classList.add('dragging'));
      li.addEventListener('dragend', () => li.classList.remove('dragging'));
      
      list.appendChild(li);
    });
    
    list.addEventListener('dragover', e => {
      e.preventDefault();
      const afterElement = getDragAfterElement(list, e.clientY);
      const dragging = document.querySelector('.dragging');
      if (afterElement == null) {
        list.appendChild(dragging);
      } else {
        list.insertBefore(dragging, afterElement);
      }
    });
    
    container.style.display = 'block';
    validateSources();
  }
  
  function getDragAfterElement(container, y) {
    const draggableElements = [...container.querySelectorAll('.tag-item:not(.dragging)')];
    
    return draggableElements.reduce((closest, child) => {
      const box = child.getBoundingClientRect();
      const offset = y - box.top - box.height / 2;
      if (offset < 0 && offset > closest.offset) {
        return { offset: offset, element: child };
      } else {
        return closest;
      }
    }, { offset: Number.NEGATIVE_INFINITY }).element;
  }

  fileInput.addEventListener('change', () => onFile(fileInput.files[0]));

  dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('dragover'); });
  dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    const file = e.dataTransfer.files[0];
    if (file) {
      const dt = new DataTransfer();
      dt.items.add(file);
      fileInput.files = dt.files;
      onFile(file);
    }
  });

  form.addEventListener('submit', () => {
    const checked = [];
    document.querySelectorAll('.tag-item').forEach(li => {
      const cb = li.querySelector('.tag-checkbox');
      if (cb && cb.checked) {
        checked.push(cb.value);
      }
    });
    document.getElementById('priority').value = checked.join(',');

    const file = fileInput.files[0];
    if (file && file.size > LARGE) {
      overlaySub.textContent = 'Large file detected — this may take 1–2 minutes.';
    }
    overlay.classList.add('active');
  });
</script>
</body>
</html>"""


# ─── Review Template ──────────────────────────────────────────────────────────

REVIEW_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Manual Review — MBSet Merge</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    """ + _BASE_STYLE + """

    body { display: flex; flex-direction: column; min-height: 100vh; }

    .header {
      position: sticky;
      top: 0;
      z-index: 100;
      background: rgba(7,7,17,0.85);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-bottom: 1px solid var(--border);
      padding: 14px 24px;
      display: flex;
      align-items: center;
      gap: 20px;
    }

    .header-brand {
      display: flex;
      align-items: center;
      gap: 10px;
      text-decoration: none;
      color: var(--text);
      font-weight: 700;
      font-size: 1rem;
      flex-shrink: 0;
    }
    .header-brand svg { color: var(--primary); }

    .progress-wrap { flex: 1; }
    .progress-meta {
      display: flex;
      justify-content: space-between;
      font-size: 0.75rem;
      color: var(--muted);
      margin-bottom: 5px;
    }
    .progress-bar-bg {
      height: 5px;
      background: var(--surface-2);
      border-radius: 9999px;
      overflow: hidden;
    }
    .progress-bar-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--primary), var(--accent));
      border-radius: 9999px;
      transition: width 0.4s ease;
    }

    .header-count {
      font-size: 0.82rem;
      font-weight: 700;
      color: var(--primary);
      flex-shrink: 0;
      background: var(--primary-dim);
      padding: 4px 12px;
      border-radius: 20px;
    }

    .main {
      position: relative;
      z-index: 1;
      flex: 1;
      padding: 32px 24px 120px;
      max-width: 1100px;
      margin: 0 auto;
      width: 100%;
    }

    .conflict-header { margin-bottom: 24px; animation: fadeIn 0.3s ease; }
    .conflict-title  { font-size: 1.3rem; font-weight: 700; margin-bottom: 8px; }
    .conflict-meta   { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .conflict-desc   { font-size: 0.85rem; color: var(--muted); }

    .candidates {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
      animation: fadeIn 0.3s ease;
    }

    .candidate-card {
      background: var(--surface);
      border: 2px solid var(--border);
      border-radius: 18px;
      padding: 22px;
      cursor: pointer;
      transition: border-color 0.2s, box-shadow 0.2s, transform 0.15s;
      position: relative;
      overflow: hidden;
    }
    .candidate-card:hover { border-color: rgba(124,106,255,0.4); box-shadow: 0 0 30px rgba(124,106,255,0.1); transform: translateY(-2px); }
    .candidate-card.selected { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(124,106,255,0.2), 0 8px 40px rgba(124,106,255,0.15); }
    .candidate-card.selected::after {
      content: '✓';
      position: absolute;
      top: 14px; right: 14px;
      width: 26px; height: 26px;
      background: var(--primary);
      border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      font-size: 0.8rem; color: white; font-weight: 700;
    }
    .candidate-card.eliminated { opacity: 0.45; border-color: var(--border); box-shadow: none; transform: none; }

    .q-text { font-size: 0.9rem; line-height: 1.65; color: var(--text); margin-bottom: 16px; max-height: 120px; overflow-y: auto; }
    .opts-label { font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--subtle); margin-bottom: 8px; }
    .opts-list { list-style: none; display: flex; flex-direction: column; gap: 5px; margin-bottom: 14px; }
    .opts-list li { font-size: 0.82rem; color: var(--muted); background: var(--surface-2); border-radius: 7px; padding: 5px 10px; display: flex; gap: 8px; }
    .opts-list li.extra-opt { background: rgba(245,158,11,0.1); color: #fcd34d; border: 1px solid rgba(245,158,11,0.2); }
    .opt-letter { font-weight: 700; color: var(--primary); flex-shrink: 0; }

    .divider { height: 1px; background: var(--border); margin: 12px 0; }
    .meta-row { display: flex; flex-wrap: wrap; gap: 8px; font-size: 0.78rem; }
    .meta-item { display: flex; align-items: center; gap: 5px; color: var(--muted); }
    .meta-item strong { color: var(--text); }

    .correct-badge { display: inline-flex; align-items: center; gap: 5px; padding: 3px 10px; border-radius: 20px; font-size: 0.78rem; font-weight: 700; }
    .correct-badge.match  { background: rgba(16,185,129,0.12); color: #34d399; border: 1px solid rgba(16,185,129,0.25); }
    .correct-badge.differ { background: rgba(245,158,11,0.12); color: #fcd34d; border: 1px solid rgba(245,158,11,0.25); }

    .select-btn {
      width: 100%;
      margin-top: 16px;
      padding: 10px;
      background: var(--primary-dim);
      color: var(--primary);
      border: 1px solid rgba(124,106,255,0.3);
      border-radius: 10px;
      font-family: inherit;
      font-size: 0.88rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.18s;
    }
    .select-btn:hover { background: rgba(124,106,255,0.25); }
    .candidate-card.selected .select-btn { background: var(--primary); color: white; border-color: var(--primary); }

    .footer {
      position: fixed;
      bottom: 0; left: 0; right: 0;
      z-index: 100;
      background: rgba(7,7,17,0.9);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-top: 1px solid var(--border);
      padding: 16px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }

    .nav-btns { display: flex; gap: 10px; }

    .resolved-indicator { font-size: 0.82rem; color: var(--muted); text-align: center; flex: 1; }
    .resolved-indicator span { color: var(--primary); font-weight: 700; }

    .info-strip { display: flex; gap: 12px; margin-bottom: 28px; flex-wrap: wrap; }
    .info-chip {
      display: flex; align-items: center; gap: 6px;
      background: var(--surface); border: 1px solid var(--border);
      border-radius: 10px; padding: 8px 14px; font-size: 0.8rem; color: var(--muted);
    }
    .info-chip strong { color: var(--text); }

    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 99px; }
  </style>
</head>
<body>

<header class="header">
  <a href="/" class="header-brand">
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z"/></svg>
    MBSet Merge
  </a>
  <div class="progress-wrap">
    <div class="progress-meta">
      <span>Manual Review</span>
      <span id="prog-text">0 / {{ conflict_count }} resolved</span>
    </div>
    <div class="progress-bar-bg">
      <div class="progress-bar-fill" id="prog-fill" style="width:0%"></div>
    </div>
  </div>
  <div class="header-count" id="header-count">1 / {{ conflict_count }}</div>
</header>

<main class="main">
  <div class="info-strip">
    <div class="info-chip">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-7 3c1.93 0 3.5 1.57 3.5 3.5S13.93 13 12 13s-3.5-1.57-3.5-3.5S10.07 6 12 6zm7 13H5v-.23c0-.62.28-1.2.76-1.58C7.47 15.82 9.64 15 12 15s4.53.82 6.24 2.19c.48.38.76.97.76 1.58V19z"/></svg>
      <strong>{{ orig_len }}</strong> questions total
    </div>
    <div class="info-chip">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="#f59e0b"><path d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z"/></svg>
      <strong style="color:#fcd34d">{{ conflict_count }}</strong> conflicts need review
    </div>
    <div class="info-chip">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="#10b981"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
      <strong style="color:#34d399">{{ auto_count }}</strong> auto-mergeable
    </div>
  </div>
  <div id="conflict-area"></div>
</main>

<footer class="footer">
  <div class="nav-btns">
    <button class="btn btn-secondary" id="btn-prev" onclick="navigate(-1)" disabled>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path stroke-linecap="round" d="M15 19l-7-7 7-7"/></svg>
      Prev
    </button>
    <button class="btn btn-secondary" id="btn-next" onclick="navigate(1)">
      Next
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path stroke-linecap="round" d="M9 5l7 7-7 7"/></svg>
    </button>
  </div>

  <div class="resolved-indicator">
    <span id="footer-resolved">0</span> of {{ conflict_count }} resolved
    <span id="footer-skipped" style="color:var(--warning);margin-left:6px;"></span>
  </div>

  <button class="btn btn-success" id="btn-complete" onclick="submitReview()">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>
    Complete Review
  </button>
</footer>

<form id="final-form" action="/finalize" method="POST" style="display:none">
  <input type="hidden" name="file_b64"           value="{{ file_b64 }}">
  <input type="hidden" name="filename"            value="{{ filename | e }}">
  <input type="hidden" name="priority"            value="{{ priority | e }}">
  <input type="hidden" name="auto_groups_b64"     value="{{ auto_groups_b64 }}">
  <input type="hidden" name="conflict_groups_b64" value="{{ conflict_groups_b64 }}">
</form>

<script>
  const conflictGroups = {{ conflict_groups | tojson }};
  const LETTERS = ['A','B','C','D','E','F','G','H'];

  let decisions  = new Array(conflictGroups.length).fill(null);
  let currentIdx = 0;

  function renderConflict(idx) {
    const group = conflictGroups[idx];
    const area  = document.getElementById('conflict-area');

    const typeLabel = group.type === 'correct'
      ? '<span class="badge badge-warning">⚠ Correct answer differs</span>'
      : '<span class="badge badge-danger">⚠ Option set differs</span>';

    const correctVals    = group.rows.map(r => r.Correct.toLowerCase()).filter(c => c);
    const correctConflict = new Set(correctVals).size > 1;

    let html = `
      <div class="conflict-header">
        <div class="conflict-title">Conflict ${idx + 1} of ${conflictGroups.length}</div>
        <div class="conflict-meta">
          ${typeLabel}
          <span class="conflict-desc">${group.rows.length} versions found — select the one to keep</span>
        </div>
      </div>
      <div class="candidates" id="cards-${idx}">
    `;

    group.rows.forEach((row, ri) => {
      const isSelected   = decisions[idx] === row.df_index;
      const isEliminated = decisions[idx] !== null && !isSelected;
      let cardClass = 'candidate-card';
      if (isSelected)   cardClass += ' selected';
      if (isEliminated) cardClass += ' eliminated';

      const otherOptSets = group.rows.filter((_, i) => i !== ri).map(r => r.options.map(o => o.toLowerCase()));

      let optsHtml = '';
      if (row.options.length > 0) {
        optsHtml = '<p class="opts-label">Options</p><ul class="opts-list">';
        row.options.forEach((opt, oi) => {
          const letter  = LETTERS[oi] || String(oi + 1);
          const isExtra = group.type === 'options' && otherOptSets.some(set => !set.some(o => o === opt.toLowerCase()));
          optsHtml += `<li class="${isExtra ? 'extra-opt' : ''}"><span class="opt-letter">${letter}.</span> ${escHtml(opt)}</li>`;
        });
        optsHtml += '</ul>';
      }

      let correctHtml = '';
      if (row.Correct) {
        const cls = correctConflict ? 'differ' : 'match';
        correctHtml = `<div class="correct-badge ${cls}">
          <svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
          Correct: ${escHtml(row.Correct)}</div>`;
      }

      const selectLabel = isSelected ? '✓ Selected' : 'Select this version';

      html += `
        <div class="${cardClass}" id="card-${idx}-${ri}" onclick="selectCandidate(${idx}, ${row.df_index})">
          <p class="q-text">${escHtml(row.Text)}</p>
          ${optsHtml}
          ${(row.options.length > 0 || row.Correct || row.Tag || row.Year) ? '<div class="divider"></div>' : ''}
          <div class="meta-row">
            ${correctHtml}
            ${row.Tag  ? `<div class="meta-item"><svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M21.41 11.58l-9-9C12.05 2.22 11.55 2 11 2H4c-1.1 0-2 .9-2 2v7c0 .55.22 1.05.59 1.42l9 9c.36.36.86.58 1.41.58s1.05-.22 1.41-.59l7-7c.37-.36.59-.86.59-1.41s-.23-1.06-.59-1.42zM5.5 7C4.67 7 4 6.33 4 5.5S4.67 4 5.5 4 7 4.67 7 5.5 6.33 7 5.5 7z"/></svg><strong>${escHtml(row.Tag)}</strong></div>` : ''}
            ${row.Year ? `<div class="meta-item"><svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M19 3h-1V1h-2v2H8V1H6v2H5c-1.11 0-1.99.9-1.99 2L3 19c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H5V8h14v11zM7 10h5v5H7z"/></svg><strong>${row.Year}</strong></div>` : ''}
            <div class="meta-item" style="margin-left:auto;font-family:monospace;font-size:0.7rem;color:var(--subtle)">id: ${escHtml(row.id)}</div>
          </div>
          <button class="select-btn" onclick="event.stopPropagation(); selectCandidate(${idx}, ${row.df_index})">${selectLabel}</button>
        </div>
      `;
    });

    html += '</div>';
    area.innerHTML = html;
    updateUI();
  }

  function escHtml(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function selectCandidate(groupIdx, dfIndex) {
    decisions[groupIdx] = dfIndex;
    renderConflict(currentIdx);
    setTimeout(() => {
      const next = decisions.findIndex((d, i) => i > currentIdx && d === null);
      if (next !== -1) { currentIdx = next; renderConflict(currentIdx); }
      else if (currentIdx < conflictGroups.length - 1) { currentIdx++; renderConflict(currentIdx); }
    }, 380);
  }

  function navigate(dir) {
    currentIdx = Math.max(0, Math.min(conflictGroups.length - 1, currentIdx + dir));
    renderConflict(currentIdx);
  }

  function updateUI() {
    const resolved = decisions.filter(d => d !== null).length;
    const skipped  = decisions.filter(d => d === null).length;
    const total    = conflictGroups.length;
    const pct      = total ? (resolved / total * 100) : 0;

    document.getElementById('prog-fill').style.width    = pct + '%';
    document.getElementById('prog-text').textContent    = `${resolved} / ${total} resolved`;
    document.getElementById('header-count').textContent = `${currentIdx + 1} / ${total}`;
    document.getElementById('footer-resolved').textContent = resolved;

    const skipEl = document.getElementById('footer-skipped');
    skipEl.textContent = skipped > 0 ? `· ${skipped} will be skipped` : '';

    document.getElementById('btn-prev').disabled = currentIdx === 0;
    document.getElementById('btn-next').disabled = currentIdx === total - 1;
    document.getElementById('btn-complete').disabled = false;
  }

  function submitReview() {
    const form = document.getElementById('final-form');
    form.querySelectorAll('[data-dynamic]').forEach(el => el.remove());
    decisions.forEach((dfIndex, i) => {
      if (dfIndex !== null) {
        const inp = document.createElement('input');
        inp.type = 'hidden'; inp.name = `conflict_${i}`; inp.value = dfIndex;
        inp.setAttribute('data-dynamic', '1');
        form.appendChild(inp);
      }
    });
    form.submit();
  }

  renderConflict(0);

  document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowLeft') {
      navigate(-1);
    } else if (e.key === 'ArrowRight') {
      navigate(1);
    }
  });
</script>
</body>
</html>"""


# ─── Results Template ─────────────────────────────────────────────────────────

RESULTS_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Results — MBSet Merge</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    """ + _BASE_STYLE + """

    body { display: flex; align-items: flex-start; justify-content: center; padding: 40px 16px 80px; }

    .wrap {
      position: relative;
      z-index: 1;
      width: 100%;
      max-width: 960px;
      animation: fadeIn 0.45s ease;
    }

    .back-link {
      display: inline-flex; align-items: center; gap: 6px;
      color: var(--muted); text-decoration: none;
      font-size: 0.85rem; font-weight: 500; margin-bottom: 28px;
      transition: color 0.18s;
    }
    .back-link:hover { color: var(--text); }

    .success-banner { text-align: center; margin-bottom: 36px; }

    .success-icon {
      width: 64px; height: 64px;
      background: linear-gradient(135deg, var(--success), #059669);
      border-radius: 20px;
      display: inline-flex; align-items: center; justify-content: center;
      margin-bottom: 16px;
      box-shadow: 0 0 50px rgba(16,185,129,0.25);
    }

    h1 { font-size: 1.9rem; font-weight: 800; letter-spacing: -0.04em; }
    .sub { color: var(--muted); font-size: 0.875rem; margin-top: 6px; }

    /* Stats */
    .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 28px; }
    .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 16px; padding: 20px; text-align: center; }
    .stat-val {
      font-size: 2rem; font-weight: 800; letter-spacing: -0.04em;
      background: linear-gradient(135deg, var(--primary), var(--accent));
      -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
    }
    .stat-label { font-size: 0.78rem; color: var(--muted); margin-top: 4px; font-weight: 500; }

    /* Section boxes */
    .section {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
      margin-bottom: 16px;
    }
    .section-head {
      display: flex; align-items: center; justify-content: space-between;
      padding: 14px 18px;
      border-bottom: 1px solid var(--border);
      cursor: pointer; user-select: none; gap: 10px;
    }
    .section-head:hover { background: var(--surface-2); }
    .section-title { font-size: 0.875rem; font-weight: 700; display: flex; align-items: center; gap: 8px; }
    .section-actions { display: flex; align-items: center; gap: 8px; }
    .chevron { transition: transform 0.2s; color: var(--muted); }
    .section.open .chevron { transform: rotate(180deg); }
    .section-body { display: none; padding: 16px 18px; }
    .section.open .section-body { display: block; }

    .copy-btn {
      background: var(--surface-2); border: 1px solid var(--border);
      color: var(--muted); border-radius: 7px;
      padding: 4px 10px; font-size: 0.72rem; font-weight: 600;
      cursor: pointer; font-family: inherit; transition: all 0.18s;
    }
    .copy-btn:hover { background: rgba(255,255,255,0.1); color: var(--text); }

    /* ── Interactive Report ── */
    .report-toolbar {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 14px;
    }
    .search-wrap {
      flex: 1;
      position: relative;
    }
    .search-wrap svg {
      position: absolute;
      left: 10px; top: 50%;
      transform: translateY(-50%);
      color: var(--subtle);
      pointer-events: none;
    }
    .search-input {
      width: 100%;
      padding: 9px 12px 9px 34px;
      background: rgba(255,255,255,0.04);
      border: 1px solid var(--border);
      border-radius: 9px;
      color: var(--text);
      font-family: inherit;
      font-size: 0.85rem;
      outline: none;
      transition: border-color 0.18s;
    }
    .search-input:focus { border-color: var(--primary); }
    .search-input::placeholder { color: var(--subtle); }

    .report-count {
      font-size: 0.78rem;
      color: var(--muted);
      white-space: nowrap;
    }

    /* Report cards */
    .rcard {
      border: 1px solid var(--border);
      border-radius: 13px;
      overflow: hidden;
      margin-bottom: 8px;
      transition: border-color 0.18s;
    }
    .rcard:hover { border-color: var(--border-strong); }
    .rcard.open  { border-color: rgba(124,106,255,0.3); }

    .rcard-head {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 12px 16px;
      cursor: pointer;
      user-select: none;
      background: var(--surface);
      transition: background 0.15s;
    }
    .rcard-head:hover { background: var(--surface-2); }

    .rcard-num {
      font-size: 0.72rem;
      font-weight: 700;
      color: var(--subtle);
      background: var(--surface-2);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 2px 7px;
      flex-shrink: 0;
    }

    .rcard-preview {
      flex: 1;
      font-size: 0.85rem;
      color: var(--muted);
      overflow: hidden;
      white-space: nowrap;
      text-overflow: ellipsis;
    }

    .rcard-badges { display: flex; align-items: center; gap: 6px; flex-shrink: 0; }

    .rcard-chevron { color: var(--subtle); transition: transform 0.2s; flex-shrink: 0; }
    .rcard.open .rcard-chevron { transform: rotate(180deg); }

    .rcard-body {
      display: none;
      padding: 16px;
      background: rgba(0,0,0,0.15);
      border-top: 1px solid var(--border);
      animation: slideDown 0.2s ease;
    }
    .rcard.open .rcard-body { display: block; }

    /* Row display */
    .row-block {
      border-radius: 10px;
      padding: 14px 16px;
      margin-bottom: 10px;
    }
    .row-block:last-child { margin-bottom: 0; }

    .row-block.kept    { background: rgba(16,185,129,0.07);  border: 1px solid rgba(16,185,129,0.2); }
    .row-block.removed { background: rgba(239,68,68,0.05);   border: 1px solid rgba(239,68,68,0.15); opacity: 0.85; }

    .row-label {
      font-size: 0.7rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 8px;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .row-label.kept-label    { color: #34d399; }
    .row-label.removed-label { color: #f87171; }

    .row-text {
      font-size: 0.875rem;
      line-height: 1.65;
      color: var(--text);
      margin-bottom: 10px;
    }

    .row-opts {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 10px;
    }
    .row-opt {
      font-size: 0.78rem;
      background: var(--surface-2);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 3px 9px;
      color: var(--muted);
    }
    .row-opt .opt-key { font-weight: 700; color: var(--primary); margin-right: 4px; }
    .row-opt.extra    { background: rgba(245,158,11,0.1); border-color: rgba(245,158,11,0.25); color: #fcd34d; }

    .row-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      font-size: 0.78rem;
      color: var(--muted);
    }
    .row-meta-item {
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .row-meta-item strong { color: var(--text); }

    .removed-header {
      font-size: 0.8rem;
      font-weight: 700;
      color: #f87171;
      margin-bottom: 8px;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .empty-report {
      text-align: center;
      padding: 32px 16px;
      color: var(--muted);
      font-size: 0.875rem;
    }

    .download-btn { width: 100%; padding: 15px; font-size: 1.05rem; border-radius: 14px; margin-top: 4px; }
  </style>
</head>
<body>
<div class="wrap">
  <a href="/" class="back-link">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z"/></svg>
    Process another file
  </a>

  <div class="success-banner">
    <div class="success-icon">
      <svg width="32" height="32" viewBox="0 0 24 24" fill="white"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
    </div>
    <h1>Merge Complete</h1>
    <p class="sub">
      {% if conflict_count > 0 %}
        {{ conflict_count }} conflict{{ 's' if conflict_count != 1 else '' }} reviewed manually &middot;
      {% endif %}
      {{ merged_count }} group{{ 's' if merged_count != 1 else '' }} merged.
    </p>
  </div>

  <div class="stats">
    <div class="stat-card"><div class="stat-val">{{ orig_len }}</div><div class="stat-label">Original questions</div></div>
    <div class="stat-card"><div class="stat-val">{{ merged_count }}</div><div class="stat-label">Groups merged</div></div>
    <div class="stat-card"><div class="stat-val">{{ final_len }}</div><div class="stat-label">Final questions</div></div>
    <div class="stat-card"><div class="stat-val">{{ orig_len - final_len }}</div><div class="stat-label">Duplicates removed</div></div>
  </div>

  <!-- Removed IDs -->
  <div class="section open" id="sec-ids">
    <div class="section-head" onclick="toggleSection('sec-ids')">
      <div class="section-title">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><path d="M17.63 5.84C17.27 5.33 16.67 5 16 5L5 5.01C3.9 5.01 3 5.9 3 7v10c0 1.1.9 1.99 2 1.99L16 19c.67 0 1.27-.33 1.63-.84L22 12l-4.37-6.16z"/></svg>
        Removed IDs
      </div>
      <div class="section-actions">
        <button class="copy-btn" onclick="event.stopPropagation(); copyEl('removed-pre')">Copy</button>
        <svg class="chevron" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" d="M19 9l-7 7-7-7"/></svg>
      </div>
    </div>
    <div class="section-body">
      <pre id="removed-pre">{{ removed_str }}</pre>
    </div>
  </div>

  <!-- Interactive Merge Report -->
  <div class="section open" id="sec-report">
    <div class="section-head" onclick="toggleSection('sec-report')">
      <div class="section-title">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-5 14H7v-2h7v2zm3-4H7v-2h10v2zm0-4H7V7h10v2z"/></svg>
        Merge Report
        <span id="report-badge" class="badge badge-info" style="margin-left:4px;">{{ merged_count }}</span>
      </div>
      <div class="section-actions">
        <svg class="chevron" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" d="M19 9l-7 7-7-7"/></svg>
      </div>
    </div>
    <div class="section-body">
      <div class="report-toolbar">
        <div class="search-wrap">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>
          <input class="search-input" id="report-search" placeholder="Search by question text or ID…" oninput="filterReport(this.value)">
        </div>
        <span class="report-count" id="report-count"></span>
      </div>
      <div id="report-list"></div>
    </div>
  </div>

  <button class="btn btn-success download-btn" onclick="downloadExcel('{{ excel_b64 }}', 'merged_output.xlsx')">
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/>
    </svg>
    Download Merged Excel (.xlsx)
  </button>
</div>

<script>
  // ── Data ────────────────────────────────────────────────────────────────────
  const richReport = {{ rich_report | tojson }};
  const LETTERS    = ['A','B','C','D','E','F','G','H'];

  let filteredReport = richReport.slice();
  let openCards      = new Set();

  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // ── Render ──────────────────────────────────────────────────────────────────

  function renderReport(groups) {
    const list = document.getElementById('report-list');
    document.getElementById('report-count').textContent =
      `${groups.length} of ${richReport.length} group${richReport.length !== 1 ? 's' : ''}`;

    if (groups.length === 0) {
      list.innerHTML = '<div class="empty-report">No matching merge groups.</div>';
      return;
    }
    list.innerHTML = groups.map((g, i) => renderCard(g, i)).join('');
  }

  function renderCard(group, i) {
    const isOpen    = openCards.has(i);
    const preview   = group.kept.text.substring(0, 90) + (group.kept.text.length > 90 ? '…' : '');
    const rmCount   = group.removed.length;
    const scoresStr = group.removed.map(r => r.score + '%').join(', ');

    return `
      <div class="rcard ${isOpen ? 'open' : ''}" id="rcard-${i}">
        <div class="rcard-head" onclick="toggleCard(${i})">
          <span class="rcard-num">#${i + 1}</span>
          <span class="rcard-preview">${escHtml(preview)}</span>
          <div class="rcard-badges">
            <span class="badge" style="background: rgba(255,255,255,0.05); color: var(--muted); border: 1px solid var(--border); font-weight: 500;">~ ${scoresStr}</span>
            <span class="badge badge-danger">−${rmCount} removed</span>
          </div>
          <svg class="rcard-chevron" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" d="M19 9l-7 7-7-7"/>
          </svg>
        </div>
        ${isOpen ? renderCardBody(group) : ''}
      </div>
    `;
  }

  function renderRowBlock(row, cls, labelText, labelCls, allOptSets) {
    const optsHtml = row.options.map((opt, oi) => {
      const letter  = LETTERS[oi] || (oi + 1);
      const isExtra = cls === 'removed' && allOptSets &&
        !allOptSets[0].some(o => o === opt.toLowerCase());
      return `<span class="row-opt${isExtra ? ' extra' : ''}"><span class="opt-key">${letter}.</span>${escHtml(opt)}</span>`;
    }).join('');

    const correctHtml = row.correct
      ? `<span class="row-meta-item"><svg width="11" height="11" viewBox="0 0 24 24" fill="#34d399"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg> Correct: <strong>${escHtml(row.correct)}</strong></span>` : '';
    const tagHtml  = row.tag  ? `<span class="row-meta-item">Tag: <strong>${escHtml(row.tag)}</strong></span>` : '';
    const yearHtml = row.year ? `<span class="row-meta-item">Year: <strong>${row.year}</strong></span>` : '';
    const idHtml   = `<span class="row-meta-item" style="margin-left:auto;font-family:monospace;color:var(--subtle);font-size:0.72rem">id: ${escHtml(row.id)}</span>`;

    return `
      <div class="row-block ${cls}">
        <div class="row-label ${labelCls}">${labelText}</div>
        <p class="row-text">${escHtml(row.text)}</p>
        ${row.options.length ? `<div class="row-opts">${optsHtml}</div>` : ''}
        <div class="row-meta">${correctHtml}${tagHtml}${yearHtml}${idHtml}</div>
      </div>
    `;
  }

  function renderCardBody(group) {
    const keptOpts    = group.kept.options.map(o => o.toLowerCase());
    const removedHtml = group.removed.map((r, ri) =>
      renderRowBlock(r, 'removed',
        `<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg> Removed <span style="margin-left: 8px; font-size: 0.72rem; color: var(--subtle); font-weight: normal; text-transform: none; letter-spacing: 0;">(Similarity: ${r.score}%)</span>`,
        'removed-label',
        [keptOpts])
    ).join('');

    return `
      <div class="rcard-body">
        ${renderRowBlock(group.kept, 'kept',
          '<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg> Kept',
          'kept-label', null)}
        ${removedHtml}
      </div>
    `;
  }

  // ── Toggle ──────────────────────────────────────────────────────────────────

  function toggleCard(i) {
    if (openCards.has(i)) openCards.delete(i);
    else openCards.add(i);
    renderReport(filteredReport);
  }

  // ── Filter ──────────────────────────────────────────────────────────────────

  function filterReport(query) {
    const q = query.toLowerCase().trim();
    filteredReport = q
      ? richReport.filter(g =>
          g.kept.text.toLowerCase().includes(q) ||
          g.kept.id.toLowerCase().includes(q)   ||
          g.removed.some(r => r.text.toLowerCase().includes(q) || r.id.toLowerCase().includes(q)))
      : richReport.slice();
    openCards.clear();
    renderReport(filteredReport);
  }

  // ── Utilities ────────────────────────────────────────────────────────────────

  function toggleSection(id) {
    document.getElementById(id).classList.toggle('open');
  }

  function copyEl(id) {
    const text = document.getElementById(id).textContent;
    navigator.clipboard.writeText(text).then(() => {
      const btn = event.target;
      const orig = btn.textContent;
      btn.textContent = 'Copied!';
      btn.style.color = '#34d399';
      setTimeout(() => { btn.textContent = orig; btn.style.color = ''; }, 2000);
    });
  }

  function downloadExcel(b64, filename) {
    const bytes = atob(b64);
    const arr   = new Uint8Array(bytes.length);
    for (let i = 0; i < bytes.length; i++) arr[i] = bytes.charCodeAt(i);
    const blob = new Blob([arr], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const a    = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
  }

  // ── Init ─────────────────────────────────────────────────────────────────────
  renderReport(richReport);
</script>
</body>
</html>"""
