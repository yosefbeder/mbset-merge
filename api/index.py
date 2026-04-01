from flask import Flask, request, render_template_string, redirect, url_for
import os
import sys
import zipfile
import tempfile
import base64
from pathlib import Path

# Add root directory to path so we can import app.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import merge_duplicate_questions

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MBSet Merge - Dataset Processor</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #6366f1;
            --primary-hover: #4f46e5;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text: #1e293b;
            --text-muted: #64748b;
            --border: #e2e8f0;
            --success: #22c55e;
        }

        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg);
            color: var(--text);
            margin: 0;
            display: flex;
            justify-content: center;
            align-items: flex-start; /* Better for long pages */
            min-height: 100vh;
            padding: 20px;
            box-sizing: border-box;
        }

        .container {
            width: 100%;
            max-width: 1000px;
            background: var(--card-bg);
            padding: 40px;
            border-radius: 24px;
            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 8px 10px -6px rgba(0, 0, 0, 0.1);
            margin: 20px 0;
        }

        h1 {
            font-weight: 700;
            font-size: 2.25rem;
            margin-bottom: 8px;
            text-align: center;
            background: linear-gradient(to right, #6366f1, #a855f7);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .subtitle {
            text-align: center;
            color: var(--text-muted);
            margin-bottom: 32px;
        }

        .form-group {
            margin-bottom: 24px;
        }

        label {
            display: block;
            font-weight: 600;
            margin-bottom: 8px;
            color: var(--text);
        }

        input[type="text"], input[type="file"] {
            width: 100%;
            padding: 12px;
            border: 2px solid var(--border);
            border-radius: 12px;
            font-size: 1rem;
            transition: all 0.2s;
            box-sizing: border-box;
        }

        input[type="text"]:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 4px rgba(99, 102, 241, 0.1);
        }

        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 12px 24px;
            background-color: var(--primary);
            color: white;
            font-weight: 600;
            border: none;
            border-radius: 12px;
            cursor: pointer;
            transition: all 0.2s;
            width: 100%;
            font-size: 1.1rem;
        }

        .btn:hover {
            background-color: var(--primary-hover);
            transform: translateY(-1px);
        }

        /* Results Grid */
        .results {
            margin-top: 40px;
            display: grid;
            grid-template-columns: 1fr 1fr; /* Two columns */
            gap: 20px;
        }

        /* Responsive Breakpoint for Side-by-Side */
        @media (max-width: 768px) {
            .results {
                grid-template-columns: 1fr; /* Stack on small screens */
            }
            .container {
                padding: 24px; /* Less padding on small screens */
            }
            h1 {
                font-size: 1.75rem;
            }
        }

        details {
            background: #f1f5f9;
            padding: 16px;
            border-radius: 12px;
            border: 1px solid var(--border);
            height: fit-content;
        }

        summary {
            font-weight: 600;
            cursor: pointer;
            padding: 4px;
            user-select: none;
            outline: none;
        }

        pre {
            background: #1e293b;
            color: #e2e8f0;
            padding: 16px;
            border-radius: 8px;
            overflow-x: auto;
            font-size: 0.875rem;
            max-height: 500px;
            margin-top: 12px;
            white-space: pre-wrap; /* Better for mobile wrap */
            word-break: break-word;
        }

        .download-btn {
            background-color: var(--success);
            margin-top: 24px;
        }

        .download-btn:hover {
            background-color: #16a34a;
        }

        .header-actions {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
            flex-wrap: wrap;
            gap: 8px;
        }

        .copy-btn {
            background: var(--text-muted);
            color: white;
            border: none;
            padding: 4px 12px;
            border-radius: 6px;
            font-size: 0.75rem;
            cursor: pointer;
            transition: background 0.2s;
        }

        .copy-btn:hover {
            background: var(--text);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>MBSet Merge</h1>
        <p class="subtitle">Clean and merge duplicate questions with priority-based tagging.</p>

        <form action="/process" method="post" enctype="multipart/form-data">
            <div class="form-group">
                <label for="file">Dataset (.xlsx / .csv)</label>
                <input type="file" id="file" name="file" accept=".xlsx, .csv" required>
            </div>
            <div class="form-group">
                <label for="priority">Tag Priority (comma separated)</label>
                <input type="text" id="priority" name="priority" value="Exams, Department, Guyton" placeholder="e.g. Exams, Department, Guyton">
            </div>
            <button type="submit" class="btn">Process Dataset</button>
        </form>

        {% if results %}
        <div class="results">
            <div class="result-column">
                <details open>
                    <summary>Removed IDs</summary>
                    <div class="header-actions">
                        <span style="font-size: 0.75rem; color: var(--text-muted);">Duplicate IDs removed.</span>
                        <button class="copy-btn" onclick="copyToClipboard('removed-ids')">Copy to Clipboard</button>
                    </div>
                    <pre id="removed-ids">{{ results.removed_ids_content }}</pre>
                </details>
            </div>
            <div class="result-column">
                <details open>
                    <summary>Merge Report</summary>
                    <p style="font-size: 0.875rem; color: var(--text-muted); margin-bottom: 8px;">Detailed analysis of the merge process.</p>
                    <pre>{{ results.report_content }}</pre>
                </details>
            </div>
        </div>
        <button onclick="downloadZip('{{ results.base64_zip }}', '{{ results.zip_filename }}')" class="btn download-btn">Download Merged Excel (.zip)</button>
        {% endif %}
    </div>

    <script>
        function copyToClipboard(elementId) {
            const text = document.getElementById(elementId).innerText;
            navigator.clipboard.writeText(text).then(() => {
                const btn = event.target;
                const originalText = btn.innerText;
                btn.innerText = 'Copied!';
                btn.style.backgroundColor = '#22c55e';
                setTimeout(() => {
                    btn.innerText = originalText;
                    btn.style.backgroundColor = '';
                }, 2000);
            });
        }

        function downloadZip(base64, filename) {
            const byteCharacters = atob(base64);
            const byteNumbers = new Array(byteCharacters.length);
            for (let i = 0; i < byteCharacters.length; i++) {
                byteNumbers[i] = byteCharacters.charCodeAt(i);
            }
            const byteArray = new Uint8Array(byteNumbers);
            const blob = new Blob([byteArray], {type: 'application/zip'});
            
            const link = document.createElement('a');
            link.href = window.URL.createObjectURL(blob);
            link.download = filename;
            link.click();
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/process', methods=['POST'])
def process():
    if 'file' not in request.files:
        return redirect(url_for('index'))
    
    file = request.files['file']
    if file.filename == '':
        return redirect(url_for('index'))

    priority_str = request.form.get('priority', 'Exams, Department, Guyton')
    priority_list = [p.strip() for p in priority_str.split(',') if p.strip()]
    priority_dict = {source: rank for rank, source in enumerate(priority_list, start=1)}

    # Work in /tmp for Vercel
    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, file.filename)
        file.save(input_path)

        # Process
        output_dir = os.path.join(temp_dir, "output")
        res_paths = merge_duplicate_questions(input_path, priority_dict, output_dir=output_dir)

        # Read contents for display (safely)
        if os.path.exists(res_paths['removed_ids']):
            with open(res_paths['removed_ids'], 'r') as f:
                removed_ids_content = f.read()
            if not removed_ids_content.strip():
                removed_ids_content = "No duplicate IDs removed."
        else:
            removed_ids_content = "No duplicate IDs removed."
        
        if os.path.exists(res_paths['merge_report']):
            with open(res_paths['merge_report'], 'r') as f:
                report_content = f.read()
        else:
            report_content = "No merge report generated (no duplicates found)."

        # Create ZIP with ONLY the merged excel
        zip_path = os.path.join(temp_dir, "merged_output.zip")
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            zipf.write(res_paths['merged_excel'], arcname=os.path.basename(res_paths['merged_excel']))

        # Convert ZIP to Base64 for stateless transmission
        with open(zip_path, "rb") as f:
            base64_zip = base64.b64encode(f.read()).decode()

        zip_filename = Path(res_paths['merged_excel']).stem + ".zip"

        results = {
            'removed_ids_content': removed_ids_content,
            'report_content': report_content,
            'base64_zip': base64_zip,
            'zip_filename': zip_filename
        }

        # The temp_dir will be deleted automatically here, but we have everything we need in memory.
        return render_template_string(HTML_TEMPLATE, results=results)

if __name__ == '__main__':
    app.run(debug=True)
