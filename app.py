import pandas as pd
import argparse
import sys

import os

def merge_duplicate_questions(
    input_file, 
    source_priority
):
    print(f"Loading data from: {input_file}")
    
    # Derive output filenames
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    merged_output_file = f"output/{base_name}_merged.xlsx"
    removed_ids_txt = f"output/{base_name}_removed_ids.txt"
    
    try:
        if input_file.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file)
        else:
            try:
                df = pd.read_csv(input_file)
            except UnicodeDecodeError:
                print("UTF-8 decoding failed, trying 'latin1'...")
                df = pd.read_csv(input_file, encoding='latin1')
    except FileNotFoundError:
        print(f"Error: Could not find the file '{input_file}'")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)
    
    if 'Year' in df.columns:
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    
    if 'Text' not in df.columns:
        print("Error: 'Text' column not found.")
        sys.exit(1)

    # Identifiers for grouping
    option_cols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    existing_option_cols = [c for c in option_cols if c in df.columns]
    
    def normalize_options(row):
        opts = []
        for c in existing_option_cols:
            val = row[c]
            if pd.isna(val):
                opts.append("")
            else:
                opts.append(str(val).strip().lower())
        return tuple(sorted(opts))

    # Create a temporary grouping key
    df['_norm_opts'] = df.apply(normalize_options, axis=1)
    df['_norm_text'] = df['Text'].fillna("").apply(lambda x: str(x).strip().lower())
    
    ids_to_remove = []
    merged_rows = []
    
    grouped = df.groupby(['_norm_text', '_norm_opts'], dropna=False)
    
    for _, group in grouped:
        if len(group) == 1:
            merged_rows.append(group.iloc[0])
            continue
            
        # --- Handle Duplicates ---
        all_tags = set()
        if 'Tag' in group.columns:
            for tag_val in group['Tag'].dropna():
                tags = [t.strip() for t in str(tag_val).split(',') if t.strip()]
                all_tags.update(tags)
        merged_tags_str = ", ".join(sorted(all_tags))
        
        def get_priority(tag_string):
            if pd.isna(tag_string):
                return 999
            best_prio = 999
            for source, prio in source_priority.items():
                if source in str(tag_string):
                    if prio < best_prio:
                        best_prio = prio
            return best_prio
        
        group_sorted = group.copy()
        if 'Tag' in group_sorted.columns:
            group_sorted['priority'] = group_sorted['Tag'].apply(get_priority)
        else:
            group_sorted['priority'] = 999
        
        sort_cols = ['priority']
        sort_orders = [True]
        if 'Year' in group_sorted.columns:
            sort_cols.append('Year')
            sort_orders.append(False)
            
        group_sorted = group_sorted.sort_values(
            by=sort_cols, 
            ascending=sort_orders,
            na_position='last'
        )
        
        winner = group_sorted.iloc[0].copy()
        if 'Tag' in winner:
            winner['Tag'] = merged_tags_str
        
        # Clean up temp columns
        for col in ['priority', '_norm_opts', '_norm_text']:
            if col in winner:
                del winner[col]
                
        merged_rows.append(winner)
        
        if 'id' in winner and 'id' in group.columns:
            winner_id = winner['id']
            for q_id in group['id']:
                if q_id != winner_id and not pd.isna(q_id):
                    ids_to_remove.append(q_id)

    if not merged_rows:
        print("No data to save.")
        return

    merged_df = pd.DataFrame(merged_rows)
    
    # Final cleanup
    for col in ['_norm_opts', '_norm_text']:
        if col in merged_df.columns:
            merged_df.drop(columns=[col], inplace=True)

    merged_df.to_excel(merged_output_file, index=False)
    print(f"✅ Saved merged dataset with {len(merged_df)} questions to: {merged_output_file}")
    
    with open(removed_ids_txt, 'w') as f:
        for q_id in ids_to_remove:
            f.write(f"{q_id}\n")
    print(f"✅ Saved {len(ids_to_remove)} removed duplicate IDs to: {removed_ids_txt}")


def main():
    parser = argparse.ArgumentParser(
        description="Merge duplicate questions based on Text and Options (ignoring order)."
    )
    
    parser.add_argument(
        '-i', '--input', 
        required=True, 
        help="Path to the input CSV or Excel file."
    )
    
    parser.add_argument(
        '-p', '--priority', 
        nargs='+', 
        default=["Exams", "Department", "Guyton"],
        help="Space-separated list of tags in order of priority."
    )

    args = parser.parse_args()
    priority_dict = {source: rank for rank, source in enumerate(args.priority, start=1)}
    
    print("--- Configuration ---")
    print(f"Input File:      {args.input}")
    print(f"Source Priority: {priority_dict}")
    print("---------------------\n")

    merge_duplicate_questions(
        input_file=args.input,
        source_priority=priority_dict
    )

if __name__ == "__main__":
    main()