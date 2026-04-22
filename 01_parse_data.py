"""
01_parse_data.py
Parse SQL dump files into pandas DataFrames and save as CSV.
Run this first before the analysis script.
"""

import re
import pandas as pd
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHART_DIR = os.path.join(BASE_DIR, "charts")
os.makedirs(CHART_DIR, exist_ok=True)

# ------------------------------------------------------------------ #
#  SQL parser                                                          #
# ------------------------------------------------------------------ #

def extract_columns(line):
    """Extract column names from INSERT INTO line."""
    m = re.search(r'\(([^)]+)\)\s+VALUES', line)
    if not m:
        return []
    return [c.strip().strip('`') for c in m.group(1).split(',')]


def parse_value_line(line):
    """
    Parse one VALUES row like: (1, 'hello', NULL, 3.14),
    Handles: NULLs, single-quoted strings with escaped quotes, numbers.
    """
    line = line.strip().rstrip(',').rstrip(';')
    if line.startswith('('):
        line = line[1:]
    if line.endswith(')'):
        line = line[:-1]

    values = []
    i = 0
    while i < len(line):
        c = line[i]
        if c == ' ' or c == '\t':
            i += 1
            continue
        if line[i:i+4] == 'NULL':
            values.append(None)
            i += 4
            if i < len(line) and line[i] == ',':
                i += 1
        elif c == "'":
            # find end of quoted string, respecting \'
            j = i + 1
            s = []
            while j < len(line):
                if line[j] == '\\' and j + 1 < len(line):
                    s.append(line[j+1])
                    j += 2
                elif line[j] == "'":
                    j += 1
                    break
                else:
                    s.append(line[j])
                    j += 1
            values.append(''.join(s))
            i = j
            if i < len(line) and line[i] == ',':
                i += 1
        else:
            # number or unquoted value
            j = i
            while j < len(line) and line[j] != ',':
                j += 1
            tok = line[i:j].strip()
            try:
                values.append(int(tok))
            except ValueError:
                try:
                    values.append(float(tok))
                except ValueError:
                    values.append(tok if tok else None)
            i = j
            if i < len(line) and line[i] == ',':
                i += 1
    return values


def parse_sql_file(filepath):
    """Parse a SQL dump file into a DataFrame."""
    print(f"\n  Parsing: {os.path.basename(filepath)}")
    t0 = time.time()

    columns = []
    rows = []
    n_lines = 0

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('INSERT INTO'):
                columns = extract_columns(line)
            elif line.startswith('(') and columns:
                try:
                    vals = parse_value_line(line)
                    if len(vals) == len(columns):
                        rows.append(vals)
                    n_lines += 1
                except Exception:
                    pass

    df = pd.DataFrame(rows, columns=columns)
    elapsed = time.time() - t0
    print(f"  -> {len(df):,} rows | {len(df.columns)} columns | {elapsed:.1f}s")
    return df


# ------------------------------------------------------------------ #
#  Load & Clean                                                        #
# ------------------------------------------------------------------ #

def load_all():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    contracts = parse_sql_file(os.path.join(base, "contracts_pl_oct24_to_dec24.sql"))
    signups   = parse_sql_file(os.path.join(base, "contract_signup_details_pl_oct24_to_dec24.sql"))
    billings  = parse_sql_file(os.path.join(base, "billings_pl_oct24_to_jan25.sql"))

    # ---------- contracts ----------
    dt_cols_c = ['signed_at','terminated_at','created_at','updated_at',
                 'billable_after','valid_through','last_billed_at',
                 'terminate_at','send_subscription_reminder_at',
                 'trial_started_at','consent_at']
    for col in dt_cols_c:
        if col in contracts.columns:
            contracts[col] = pd.to_datetime(contracts[col], errors='coerce')

    # ---------- billing_histories ----------
    dt_cols_b = ['created_at','updated_at']
    for col in dt_cols_b:
        if col in billings.columns:
            billings[col] = pd.to_datetime(billings[col], errors='coerce')

    # ---------- sign_up_details ----------
    date_cols_s = ['signed_at_date','terminated_at_date']
    for col in date_cols_s:
        if col in signups.columns:
            signups[col] = pd.to_datetime(signups[col], errors='coerce')

    print("\n  All files parsed successfully.")
    return contracts, signups, billings


if __name__ == "__main__":
    print("=" * 60)
    print("  Data Parsing — PL Assignment")
    print("=" * 60)
    contracts, signups, billings = load_all()
    print("\n  Sample — contracts:")
    print(contracts[['id','state','product_identifier','created_at','terminated_at']].head(3).to_string())
    print("\n  Sample — billings:")
    print(billings[['id','contract_id','status','amount_in_euro_cents','created_at']].head(3).to_string())
    print("\nDone.")
