import pandas as pd
import argparse
import sys
import os
import re

def load_dataset(input_file):
    """Loads CSV or Excel file into a pandas DataFrame."""
    try:
        if input_file.lower().endswith(('.xlsx', '.xls')):
            return pd.read_excel(input_file)
        else:
            try:
                return pd.read_csv(input_file)
            except UnicodeDecodeError:
                print("UTF-8 decoding failed, trying 'latin1'...")
                return pd.read_csv(input_file, encoding='latin1')
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

def normalize_text(text):
    """Standardizes text for comparison by removing punctuation and extra whitespaces."""
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def get_normalized_options(row, existing_option_cols):
    """Returns a sorted tuple of normalized option strings (punctuation removed)."""
    opts = []
    for c in existing_option_cols:
        val = row[c]
        if pd.isna(val):
            opts.append("")
        else:
            val_str = str(val).strip().lower()
            val_str = re.sub(r'[^\w\s]', '', val_str)
            val_str = re.sub(r'\s+', ' ', val_str).strip()
            opts.append(val_str)
    return tuple(sorted(opts))

def get_tag_priority_score(tag_string, source_priority):
    """Returns the best priority score for a given tag string."""
    if pd.isna(tag_string):
        return 999
    best_prio = 999
    for source, prio in source_priority.items():
        if source in str(tag_string):
            if prio < best_prio:
                best_prio = prio
    return best_prio

def get_merged_tags(group):
    """Aggregates and sorts all unique tags from a group of rows."""
    all_tags = set()
    if 'Tag' in group.columns:
        for tag_val in group['Tag'].dropna():
            tags = [t.strip() for t in str(tag_val).split(',') if t.strip()]
            all_tags.update(tags)
    return ", ".join(sorted(all_tags))

def get_display_id(idx, df):
    """Returns a string identifier for a row (ID value or Row number)."""
    row = df.loc[idx]
    if 'id' in df.columns:
        val = row['id']
        if not pd.isna(val) and str(val).strip() != "":
            return str(val)
    return f"Row {idx+1}"

def merge_duplicate_questions(input_file, source_priority, output_dir="output"):
    print(f"Processing: {input_file}")
    
    # Setup paths
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    os.makedirs(output_dir, exist_ok=True)
    merged_output_file = os.path.join(output_dir, f"{base_name}_merged.xlsx")
    removed_ids_txt = os.path.join(output_dir, f"{base_name}_removed_ids.txt")
    merge_report_txt = os.path.join(output_dir, f"{base_name}_merge_report.txt")

    df = load_dataset(input_file)
    
    if 'Year' in df.columns:
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    
    if 'Text' not in df.columns:
        print("Error: 'Text' column not found.")
        sys.exit(1)

    # Prepare grouping keys
    option_cols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    existing_option_cols = [c for c in option_cols if c in df.columns]
    
    df['_norm_opts'] = df.apply(lambda r: get_normalized_options(r, existing_option_cols), axis=1)
    df['_norm_text'] = df['Text'].apply(normalize_text)
    
    indices_to_keep = []
    ids_to_remove = []
    merge_report_data = []

    # Group and Process
    grouped = df.groupby(['_norm_text', '_norm_opts'], dropna=False)
    
    for _, group in grouped:
        if len(group) <= 1:
            continue
            
        group_indices = group.index.tolist()
        
        # 1. Determine the "Winner" (for Tag/Year extraction)
        group_eval = group.copy()
        group_eval['priority'] = group_eval['Tag'].apply(lambda t: get_tag_priority_score(t, source_priority)) if 'Tag' in group.columns else 999
        
        sort_cols = ['priority']
        sort_orders = [True]
        if 'Year' in group_eval.columns:
            sort_cols.append('Year')
            sort_orders.append(False) # Latest year
            
        sort_evaled = group_eval.sort_values(by=sort_cols, ascending=sort_orders, na_position='last')
        winner_idx = sort_evaled.index[0]
        
        anchor_idx = winner_idx # Original row we keep
        indices_to_keep.append(anchor_idx)
        
        # 2. Collect Merged Data
        merged_tags = get_merged_tags(group)
        
        # Find the best Year: the first non-null year in the priority-sorted list
        best_year = None
        if 'Year' in df.columns:
            for y_val in sort_evaled['Year']:
                if not pd.isna(y_val):
                    best_year = y_val
                    break
        
        # 3. Update ONLY Tag and Year in the anchor row
        if 'Tag' in df.columns:
            df.at[anchor_idx, 'Tag'] = merged_tags
        if 'Year' in df.columns and not pd.isna(best_year):
            df.at[anchor_idx, 'Year'] = best_year

        # 4. Record for Report
        merge_report_data.append({
            'text': str(df.at[anchor_idx, 'Text'])[:100],
            'anchor_id': get_display_id(anchor_idx, df),
            'removed_ids': [get_display_id(idx, df) for idx in group_indices if idx != anchor_idx],
            'final_tags': merged_tags,
            'final_year': best_year
        })

        # 5. Mark duplicates for removal listing
        for idx in group_indices:
            if idx != anchor_idx:
                if 'id' in df.columns:
                    val = group.loc[idx, 'id']
                    if not pd.isna(val) and str(val).strip() != "":
                        ids_to_remove.append(val)

    # Final cleanup: Only keep rows from groups that had duplicates (indices_to_keep)
    # This filters out "unchanged" questions that were unique
    merged_df = df.loc[sorted(indices_to_keep)].copy()
    merged_df.drop(columns=['_norm_opts', '_norm_text'], inplace=True, errors='ignore')

    # Save outputs
    merged_df.to_excel(merged_output_file, index=False)
    print(f"✅ Saved merged dataset with {len(merged_df)} questions to: {merged_output_file}")
    
    with open(removed_ids_txt, 'w') as f:
        for q_id in ids_to_remove:
            f.write(f"{q_id}\n")
    print(f"✅ Saved {len(ids_to_remove)} removed duplicate IDs to: {removed_ids_txt}")

    # Total rows in the groups minus the kept anchors = number of actual removed duplicate records
    total_removed = sum(len(group) - 1 for _, group in grouped if len(group) > 1)

    if merge_report_data:
        write_report(merge_report_txt, input_file, len(df), total_removed, len(merged_df), merge_report_data)
        print(f"✅ Saved merge report to: {merge_report_txt}")

    return {
        "merged_excel": merged_output_file,
        "removed_ids": removed_ids_txt,
        "merge_report": merge_report_txt
    }

def write_report(path, source, total, removed, final, details):
    with open(path, 'w') as f:
        f.write("MERGE REPORT\n============\n\n")
        f.write(f"Source File: {source}\nTotal questions: {total}\nMerged groups: {removed}\nFinal modified questions: {final}\n\n")
        f.write("Merge Details:\n" + "-"*60 + "\n")
        for i, entry in enumerate(details, 1):
            f.write(f"{i}. TEXT: {entry['text']}\n")
            f.write(f"   POSITION KEPT: {entry['anchor_id']}\n")
            f.write(f"   REMOVED IDS:   {', '.join(map(str, entry['removed_ids']))}\n")
            f.write(f"   FINAL YEAR:    {entry['final_year'] if entry['final_year'] is not None else 'None'}\n")
            f.write(f"   FINAL TAGS:    {entry['final_tags']}\n")
            f.write("-" * 60 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Merge duplicate questions while preserving all columns except Tag and Year.")
    parser.add_argument('-i', '--input', required=True, help="Path to input file.")
    parser.add_argument('-p', '--priority', nargs='+', default=["Exams", "Department", "Guyton"], help="Tag priority.")
    args = parser.parse_args()
    
    priority_dict = {source: rank for rank, source in enumerate(args.priority, start=1)}
    merge_duplicate_questions(args.input, priority_dict)

if __name__ == "__main__":
    main()