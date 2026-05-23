from flask import Flask, request, render_template_string
import base64
import json
import pandas as pd

try:
    from api.core import analyze, apply_decisions, build_output, load_df
    from api.templates import UPLOAD_TEMPLATE, REVIEW_TEMPLATE, RESULTS_TEMPLATE
except ImportError:
    from core import analyze, apply_decisions, build_output, load_df
    from templates import UPLOAD_TEMPLATE, REVIEW_TEMPLATE, RESULTS_TEMPLATE

app = Flask(__name__)


@app.route('/')
def index():
    return render_template_string(UPLOAD_TEMPLATE)


@app.route('/process', methods=['POST'])
def process():
    if 'file' not in request.files:
        return render_template_string(UPLOAD_TEMPLATE, error='No file uploaded.')
    f = request.files['file']
    if not f.filename:
        return render_template_string(UPLOAD_TEMPLATE, error='No file selected.')

    priority_str = request.form.get('priority', 'Exams, Department, Guyton')
    plist = [p.strip() for p in priority_str.split(',') if p.strip()]
    pmap  = {src: rank for rank, src in enumerate(plist, 1)}

    file_bytes = f.read()
    try:
        df, auto_groups, conflict_groups = analyze(file_bytes, f.filename, pmap)
    except ValueError as e:
        return render_template_string(UPLOAD_TEMPLATE, error=str(e))

    orig_len = len(df)

    # No conflicts — auto-merge everything and show results
    if not conflict_groups:
        final_df, rem_ids, rich_report = apply_decisions(df, auto_groups, [])
        z64, rem_str = build_output(orig_len, final_df, rem_ids)
        return render_template_string(
            RESULTS_TEMPLATE,
            zip_b64=z64, removed_str=rem_str, rich_report=rich_report,
            orig_len=orig_len, final_len=len(final_df),
            merged_count=len(auto_groups), conflict_count=0,
        )

    # Conflicts found — serve interactive review page
    file_b64 = base64.b64encode(file_bytes).decode()
    ag_b64   = base64.b64encode(json.dumps(auto_groups).encode()).decode()
    cg_b64   = base64.b64encode(json.dumps(conflict_groups).encode()).decode()

    return render_template_string(
        REVIEW_TEMPLATE,
        file_b64=file_b64,
        filename=f.filename,
        priority=priority_str,
        auto_groups_b64=ag_b64,
        conflict_groups_b64=cg_b64,
        conflict_groups=conflict_groups,   # for tojson in JS
        conflict_count=len(conflict_groups),
        auto_count=len(auto_groups),
        orig_len=orig_len,
    )


@app.route('/finalize', methods=['POST'])
def finalize():
    file_b64 = request.form.get('file_b64', '')
    filename = request.form.get('filename', 'file.xlsx')
    ag_b64   = request.form.get('auto_groups_b64', '')
    cg_b64   = request.form.get('conflict_groups_b64', '')

    file_bytes      = base64.b64decode(file_b64)
    auto_groups     = json.loads(base64.b64decode(ag_b64))
    conflict_groups = json.loads(base64.b64decode(cg_b64))

    df = load_df(file_bytes, filename)
    if 'Year' in df.columns:
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce')

    orig_len = len(df)

    # Parse manual decisions (one per conflict group)
    review_decisions = []
    for i, group in enumerate(conflict_groups):
        winner_str = request.form.get(f'conflict_{i}')
        if winner_str is not None:
            try:
                review_decisions.append({
                    'winner_idx': int(winner_str),
                    'all_idxs':  [row['df_index'] for row in group['rows']],
                })
            except (ValueError, KeyError):
                pass

    final_df, rem_ids, rich_report = apply_decisions(df, auto_groups, review_decisions)
    z64, rem_str = build_output(orig_len, final_df, rem_ids)

    return render_template_string(
        RESULTS_TEMPLATE,
        zip_b64=z64, removed_str=rem_str, rich_report=rich_report,
        orig_len=orig_len, final_len=len(final_df),
        merged_count=len(auto_groups) + len(review_decisions),
        conflict_count=len(conflict_groups),
    )


if __name__ == '__main__':
    app.run(debug=True, port=5001)
