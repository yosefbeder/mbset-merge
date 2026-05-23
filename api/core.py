"""
core.py — Data processing: loading, fuzzy analysis, decision application,
           output packaging.
"""
import zipfile
import base64
import io
from collections import defaultdict

import pandas as pd
from rapidfuzz import fuzz

THRESHOLD = 95
OPTION_COLS_ORDER = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']


# ─── Low-level helpers ────────────────────────────────────────────────────────

def load_df(file_bytes, filename):
    """Load CSV or Excel bytes into a DataFrame."""
    if filename.lower().endswith(('.xlsx', '.xls')):
        return pd.read_excel(io.BytesIO(file_bytes))
    try:
        return pd.read_csv(io.BytesIO(file_bytes))
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(file_bytes), encoding='latin1')


def sv(v):
    """Safe string: return stripped str or '' for NA/None."""
    try:
        if pd.isna(v):
            return ''
    except (TypeError, ValueError):
        pass
    return str(v).strip()


def get_opts(row, opt_cols):
    """Return non-empty option strings for a row."""
    return [sv(row[c]) for c in opt_cols if sv(row.get(c))]


def opts_fuzzy_match(a, b):
    """True if all sorted option pairs exceed THRESHOLD similarity."""
    if len(a) != len(b):
        return False
    for x, y in zip(sorted(a, key=str.lower), sorted(b, key=str.lower)):
        if fuzz.ratio(x.lower(), y.lower()) < THRESHOLD:
            return False
    return True


def tag_prio(tag, pmap):
    """Return lowest priority rank found in the tag string (lower = better)."""
    s = sv(tag)
    if not s:
        return 999
    return min((p for src, p in pmap.items() if src in s), default=999)


def merge_tags(tag_strings):
    """Merge comma-separated tag strings into a sorted unique set."""
    tags = set()
    for t in tag_strings:
        s = sv(t)
        if s:
            tags.update(x.strip() for x in s.split(',') if x.strip())
    return ', '.join(sorted(tags))


def _row_data(df, idx, opt_cols, has):
    """Serialize a single DataFrame row into a plain dict for the UI."""
    r = df.iloc[idx]
    yr = r.get('Year') if has['Year'] else None
    return {
        'id':      (sv(r.get('id', '')) or f'Row {idx + 1}') if has['id'] else f'Row {idx + 1}',
        'text':    sv(r.get('Text', '')),
        'options': get_opts(r, opt_cols),
        'correct': sv(r.get('Correct', '')) if has['Correct'] else '',
        'tag':     sv(r.get('Tag', ''))     if has['Tag']     else '',
        'year':    str(int(yr)) if yr is not None and not pd.isna(yr) else '',
    }


# ─── Fuzzy Analysis ───────────────────────────────────────────────────────────

def analyze(file_bytes, filename, pmap):
    """
    Load the dataset, cluster duplicates via RapidFuzz fuzzy matching,
    and split groups into auto-mergeable and conflict (needs review) groups.

    Returns:
        df:              original DataFrame (RangeIndex 0..n-1)
        auto_groups:     list of {winner_idx, all_idxs, merged_tags, best_year}
        conflict_groups: list of {type: 'correct'|'options', rows: [...]}
    """
    df = load_df(file_bytes, filename)
    if 'Year' in df.columns:
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    if 'Text' not in df.columns:
        raise ValueError("'Text' column not found in the dataset.")

    opt_cols = [c for c in OPTION_COLS_ORDER if c in df.columns]
    has = {k: k in df.columns for k in ['Correct', 'Tag', 'Year', 'id']}
    n = len(df)
    texts = df['Text'].fillna('').astype(str).str.lower().tolist()

    # Union-Find (path halving)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # O(n²) pairwise fuzzy comparison
    pair_rel = {}  # (i, j) -> 'auto' | 'correct' | 'options'
    for i in range(n):
        ti = texts[i]
        for j in range(i + 1, n):
            if fuzz.ratio(ti, texts[j]) < THRESHOLD:
                continue
            oi = get_opts(df.iloc[i], opt_cols)
            oj = get_opts(df.iloc[j], opt_cols)
            k = (i, j)
            if len(oi) != len(oj):
                pair_rel[k] = 'options'
            elif not opts_fuzzy_match(oi, oj):
                pair_rel[k] = 'options'
            elif has['Correct']:
                ci = sv(df.iloc[i].get('Correct', '')).lower()
                cj = sv(df.iloc[j].get('Correct', '')).lower()
                pair_rel[k] = 'correct' if (ci and cj and ci != cj) else 'auto'
            else:
                pair_rel[k] = 'auto'
            union(i, j)

    # Build groups from union-find
    raw = defaultdict(list)
    for i in range(n):
        raw[find(i)].append(i)

    auto_groups, conflict_groups = [], []

    for _root, idxs in raw.items():
        if len(idxs) <= 1:
            continue

        # Determine worst conflict type in this group
        ct = None
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                kk = (min(idxs[a], idxs[b]), max(idxs[a], idxs[b]))
                if pair_rel.get(kk, 'auto') != 'auto':
                    ct = pair_rel[kk]
                    break
            if ct:
                break

        # Serialize each row's data
        rows = []
        for idx in idxs:
            r = df.iloc[idx]
            yr = r.get('Year') if has['Year'] else None
            rows.append({
                'df_index': int(idx),
                'Text':    sv(r.get('Text', '')),
                'options': get_opts(r, opt_cols),
                'Correct': sv(r.get('Correct', '')) if has['Correct'] else '',
                'Tag':     sv(r.get('Tag', ''))     if has['Tag']     else '',
                'Year':    str(int(yr)) if yr is not None and not pd.isna(yr) else '',
                'id':      (sv(r.get('id', '')) or f'Row {idx + 1}') if has['id'] else f'Row {idx + 1}',
            })

        if ct:
            conflict_groups.append({'type': ct, 'rows': rows})
        else:
            # Determine auto-winner by tag priority then latest year
            sub = df.iloc[idxs].copy()
            sub['_p'] = sub['Tag'].apply(lambda t: tag_prio(t, pmap)) if has['Tag'] else 999
            sc, sa = ['_p'], [True]
            if has['Year']:
                sc.append('Year')
                sa.append(False)
            ss = sub.sort_values(sc, ascending=sa, na_position='last')
            winner = int(ss.index[0])
            by = None
            if has['Year']:
                for y in ss['Year']:
                    if not pd.isna(y):
                        by = float(y)
                        break
            tvs = [sv(df.iloc[i].get('Tag', '')) for i in idxs] if has['Tag'] else []
            auto_groups.append({
                'winner_idx':  winner,
                'all_idxs':    [int(i) for i in idxs],
                'merged_tags': merge_tags(tvs),
                'best_year':   by,
            })

    return df, auto_groups, conflict_groups


# ─── Apply Decisions ──────────────────────────────────────────────────────────

def apply_decisions(df, auto_groups, review_decisions):
    """
    Apply auto-merge and manual review decisions to the DataFrame.

    review_decisions: list of {winner_idx, all_idxs}
        Skipped conflicts (unresolved) are simply absent — all their rows
        remain in the output unchanged.

    Returns:
        final_df:     DataFrame with duplicates removed and tags/year merged
        removed_ids:  list of removed id strings (for the IDs text box)
        rich_report:  list of {kept, removed} dicts for the interactive UI
    """
    opt_cols = [c for c in OPTION_COLS_ORDER if c in df.columns]
    has = {k: k in df.columns for k in ['Tag', 'Year', 'id', 'Correct']}
    to_remove = set()
    removed_ids = []
    rich_report = []

    all_groups = [
        (g['winner_idx'], g['all_idxs'], g['merged_tags'], g['best_year'])
        for g in auto_groups
    ]

    for dec in review_decisions:
        w, ai = dec['winner_idx'], dec['all_idxs']
        tvs = [sv(df.iloc[i].get('Tag', '')) for i in ai] if has['Tag'] else []
        mt = merge_tags(tvs)
        by = None
        if has['Year']:
            for idx in ai:
                y = df.iloc[idx].get('Year')
                if y is not None and not pd.isna(y):
                    by = float(y)
                    break
        all_groups.append((w, ai, mt, by))

    updated_indices = set()
    for winner, idxs, mt, by in all_groups:
        updated_indices.add(winner)
        
        # 1. Write merged values back into the DataFrame
        if has['Tag']:
            df.at[winner, 'Tag'] = mt
        if has['Year'] and by is not None:
            df.at[winner, 'Year'] = by

        # 2. Snapshot the kept row (after merge, so tag/year reflect merged values)
        kept = _row_data(df, winner, opt_cols, has)

        # 3. Collect removed rows
        removed_rows = []
        winner_text = sv(df.iloc[winner].get('Text', '')).lower()
        for idx in idxs:
            if idx != winner:
                to_remove.add(idx)
                if has['id']:
                    v = sv(df.iloc[idx].get('id', ''))
                    if v:
                        removed_ids.append(v)
                
                # Compute similarity score
                removed_text = sv(df.iloc[idx].get('Text', '')).lower()
                score = round(fuzz.ratio(winner_text, removed_text), 1)
                
                rdata = _row_data(df, idx, opt_cols, has)
                rdata['score'] = score
                removed_rows.append(rdata)

        rich_report.append({
            'kept':    kept,
            'removed': removed_rows,
        })

    keep = sorted(updated_indices)
    return df.iloc[keep].copy(), removed_ids, rich_report


# ─── Output Packaging ─────────────────────────────────────────────────────────

def build_output(orig_len, final_df, removed_ids):
    """Package the final DataFrame into a ZIP and build the removed-IDs string."""
    buf = io.BytesIO()
    final_df.to_excel(buf, index=False)
    zb = io.BytesIO()
    with zipfile.ZipFile(zb, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('merged_output.xlsx', buf.getvalue())
    z64 = base64.b64encode(zb.getvalue()).decode()
    rem_str = '\n'.join(removed_ids) if removed_ids else 'No IDs recorded.'
    return z64, rem_str
