"""
02_full_analysis.py
Complete analysis: Subscription, Revenue, Billing, Retention, Marketing
Generates charts + saves summary data as CSVs
"""

import csv, re, os, sys, time, warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
warnings.filterwarnings('ignore')

# ── Paths ────────────────────────────────────────────────────────────
BASE  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHARTS = os.path.join(BASE, "charts")
os.makedirs(CHARTS, exist_ok=True)

# ── Style ────────────────────────────────────────────────────────────
plt.rcParams.update({
    'figure.facecolor': '#f8f9fa',
    'axes.facecolor':   '#ffffff',
    'axes.grid':        True,
    'grid.alpha':       0.35,
    'axes.spines.top':  False,
    'axes.spines.right':False,
    'font.size':        11,
})
PALETTE = sns.color_palette("tab10")

def savefig(name):
    path = os.path.join(CHARTS, name)
    plt.savefig(path, dpi=130, bbox_inches='tight')
    plt.close()
    print(f"   saved → charts/{name}")


# ════════════════════════════════════════════════════════════════════ #
#  PARSER                                                              #
# ════════════════════════════════════════════════════════════════════ #

PLACEHOLDER = '\x01'   # temp stand-in for escaped single-quotes

def parse_sql(filepath):
    print(f"\n  Parsing {os.path.basename(filepath)} …", end=" ", flush=True)
    t0 = time.time()
    cols, rows = None, []
    expected = None
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('INSERT INTO'):
                m = re.search(r'\(([^)]+)\)\s+VALUES', line)
                if m:
                    cols = [c.strip().strip('`') for c in m.group(1).split(',')]
                    expected = len(cols)
            elif line.startswith('(') and cols:
                line2 = line.rstrip().rstrip(',').rstrip(';')
                # Step 1: hide escaped single-quotes so csv.reader won't split on them
                line2 = line2.replace("\\'", PLACEHOLDER)
                # Step 2: replace bare NULL with empty double-quoted string
                line2 = re.sub(r"(?<!['\w])NULL(?!['\w])", '""', line2)
                try:
                    r = list(csv.reader([line2], quotechar="'", skipinitialspace=True))[0]
                    if r:
                        r[0]  = r[0].lstrip('(')
                        r[-1] = r[-1].rstrip(')')
                        # restore escaped quotes and only keep rows with correct col count
                        r = [v.replace(PLACEHOLDER, "'") if isinstance(v, str) else v
                             for v in r]
                        if len(r) == expected:
                            rows.append(r)
                        # else: silently skip malformed rows
                except Exception:
                    pass
    df = pd.DataFrame(rows, columns=cols)
    df.replace('', np.nan, inplace=True)
    print(f"{len(df):,} rows | {time.time()-t0:.1f}s")
    return df


# ════════════════════════════════════════════════════════════════════ #
#  LOAD & CLEAN                                                        #
# ════════════════════════════════════════════════════════════════════ #

def load_data():
    print("\n" + "="*60)
    print("  STEP 1 – Loading Data")
    print("="*60)

    C = parse_sql(os.path.join(BASE, "contracts_pl_oct24_to_dec24.sql"))
    S = parse_sql(os.path.join(BASE, "contract_signup_details_pl_oct24_to_dec24.sql"))
    B = parse_sql(os.path.join(BASE, "billings_pl_oct24_to_jan25.sql"))

    # — contracts dtypes
    for col in ['signed_at','terminated_at','created_at','updated_at',
                'billable_after','last_billed_at','trial_started_at','consent_at']:
        if col in C.columns:
            C[col] = pd.to_datetime(C[col], errors='coerce')
    for col in ['billing_histories_count','billing_histories_sum_in_euro_cents',
                'payment_provider_config_profile_id','id']:
        if col in C.columns:
            C[col] = pd.to_numeric(C[col], errors='coerce')

    # — billings dtypes
    for col in ['created_at','updated_at']:
        B[col] = pd.to_datetime(B[col], errors='coerce')
    for col in ['amount_in_euro_cents','amount_in_cents','payout_amount_in_euro_cents',
                'conversion_rate','id','contract_id']:
        if col in B.columns:
            B[col] = pd.to_numeric(B[col], errors='coerce')

    # — signups dtypes
    for col in ['signed_at_date','terminated_at_date']:
        if col in S.columns:
            S[col] = pd.to_datetime(S[col], errors='coerce')
    if 'contract_id' in S.columns:
        S['contract_id'] = pd.to_numeric(S['contract_id'], errors='coerce')

    # — derived columns on contracts
    C['signup_month'] = C['created_at'].dt.to_period('M')
    C['churned']      = C['terminated_at'].notna()
    C['lifetime_days']= (
        C['terminated_at'].fillna(pd.Timestamp('2025-01-31')) - C['created_at']
    ).dt.days.clip(lower=0)

    # — derived on billings
    B['is_success']   = B['status'].str.lower().str.strip().isin(['ok', 'success', 'succeeded'])
    B['billing_month']= B['created_at'].dt.to_period('M')

    # successful billings only
    B_ok = B[B['is_success']].copy()

    print(f"\n  Contracts  : {len(C):,}")
    print(f"  Signups    : {len(S):,}")
    print(f"  Billings   : {len(B):,}  (successful: {len(B_ok):,})")

    return C, S, B, B_ok


# ════════════════════════════════════════════════════════════════════ #
#  1. SUBSCRIPTION OVERVIEW                                            #
# ════════════════════════════════════════════════════════════════════ #

def analysis_subscriptions(C, S):
    print("\n" + "="*60)
    print("  STEP 2 – Subscription Overview")
    print("="*60)
    results = {}

    # total
    total = len(C)
    results['total_subscriptions'] = total
    print(f"  Total subscriptions: {total:,}")

    # by product
    by_prod = C.groupby('product_identifier').size().sort_values(ascending=False)
    results['by_product'] = by_prod
    print(f"\n  Top 5 products:\n{by_prod.head(5).to_string()}")

    # signup trend by month
    monthly = C.groupby('signup_month').size()
    results['monthly_signups'] = monthly

    # by state
    by_state = C['state'].value_counts()
    results['by_state'] = by_state

    # ── CHARTS ──────────────────────────────────────────────────────

    # 1a – Monthly signups
    fig, ax = plt.subplots(figsize=(12,4))
    monthly.plot(kind='bar', ax=ax, color=PALETTE[0], edgecolor='white')
    ax.set_title("Monthly New Subscriptions", fontsize=14, fontweight='bold')
    ax.set_xlabel("Month"); ax.set_ylabel("Subscriptions")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'{int(x):,}'))
    plt.xticks(rotation=30, ha='right')
    plt.tight_layout()
    savefig("01_monthly_signups.png")

    # 1b – Top 15 products
    fig, ax = plt.subplots(figsize=(12,5))
    by_prod.head(15).plot(kind='barh', ax=ax, color=PALETTE[1])
    ax.invert_yaxis()
    ax.set_title("Top 15 Products by Subscriptions", fontsize=14, fontweight='bold')
    ax.set_xlabel("Subscriptions")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'{int(x):,}'))
    plt.tight_layout()
    savefig("02_top_products_subs.png")

    # 1c – subscription state pie
    fig, ax = plt.subplots(figsize=(7,7))
    by_state.plot(kind='pie', ax=ax, autopct='%1.1f%%',
                  colors=sns.color_palette("Set2", len(by_state)),
                  startangle=140, wedgeprops={'edgecolor':'white','linewidth':1.5})
    ax.set_ylabel('')
    ax.set_title("Subscription States", fontsize=14, fontweight='bold')
    plt.tight_layout()
    savefig("03_subscription_states.png")

    # 1d – by OS family (from signups)
    if 'os_family' in S.columns:
        os_data = S['os_family'].value_counts().head(10)
        fig, ax = plt.subplots(figsize=(10,4))
        os_data.plot(kind='bar', ax=ax, color=PALETTE[2], edgecolor='white')
        ax.set_title("Subscriptions by OS Family", fontsize=14, fontweight='bold')
        ax.set_xlabel("OS Family"); ax.set_ylabel("Signups")
        plt.xticks(rotation=30, ha='right')
        plt.tight_layout()
        savefig("04_signups_by_os.png")
        results['by_os'] = os_data

    # 1e – top 15 campaigns
    if 'campaign_id' in S.columns:
        camp_data = S['campaign_id'].value_counts().head(15)
        results['by_campaign'] = camp_data

    # 1f – top 15 publishers
    if 'publisher_id' in S.columns:
        pub_data = S['publisher_id'].value_counts().head(15)
        results['by_publisher'] = pub_data

    return results


# ════════════════════════════════════════════════════════════════════ #
#  2. REVENUE ANALYSIS                                                 #
# ════════════════════════════════════════════════════════════════════ #

def analysis_revenue(C, S, B_ok):
    print("\n" + "="*60)
    print("  STEP 3 – Revenue Analysis")
    print("="*60)
    results = {}

    EUR = 100.0   # euro cents → euros

    # total revenue
    total_rev = B_ok['amount_in_euro_cents'].sum() / EUR
    results['total_revenue_eur'] = total_rev
    print(f"  Total Revenue: €{total_rev:,.2f}")

    # revenue per product
    rev_prod = (B_ok.groupby('product_identifier')['amount_in_euro_cents']
                .sum().sort_values(ascending=False) / EUR)
    results['revenue_by_product'] = rev_prod
    print(f"\n  Top 5 products by revenue:\n{rev_prod.head(5).to_string()}")

    # monthly revenue trend
    rev_monthly = (B_ok.groupby('billing_month')['amount_in_euro_cents']
                   .sum() / EUR)
    results['monthly_revenue'] = rev_monthly

    # avg revenue per subscription
    subs_with_rev = (B_ok.groupby('contract_id')['amount_in_euro_cents']
                     .sum() / EUR)
    avg_rev = subs_with_rev.mean()
    results['avg_revenue_per_sub'] = avg_rev
    print(f"  Avg Revenue/Subscription: €{avg_rev:.2f}")

    # revenue by campaign
    if 'campaign_id' in S.columns:
        merged = S[['contract_id','campaign_id']].merge(
            B_ok[['contract_id','amount_in_euro_cents']], on='contract_id', how='inner')
        rev_camp = (merged.groupby('campaign_id')['amount_in_euro_cents']
                    .sum().sort_values(ascending=False).head(15) / EUR)
        results['revenue_by_campaign'] = rev_camp

    # revenue by publisher
    if 'publisher_id' in S.columns:
        merged2 = S[['contract_id','publisher_id']].merge(
            B_ok[['contract_id','amount_in_euro_cents']], on='contract_id', how='inner')
        rev_pub = (merged2.groupby('publisher_id')['amount_in_euro_cents']
                   .sum().sort_values(ascending=False).head(15) / EUR)
        results['revenue_by_publisher'] = rev_pub

    # ── CHARTS ──────────────────────────────────────────────────────

    # 2a – monthly revenue
    fig, ax = plt.subplots(figsize=(12,4))
    rev_monthly.plot(kind='bar', ax=ax, color=PALETTE[3], edgecolor='white')
    ax.set_title("Monthly Revenue (Successful Billings)", fontsize=14, fontweight='bold')
    ax.set_xlabel("Month"); ax.set_ylabel("Revenue (EUR)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'€{x:,.0f}'))
    plt.xticks(rotation=30, ha='right')
    plt.tight_layout()
    savefig("05_monthly_revenue.png")

    # 2b – top 15 products by revenue
    fig, ax = plt.subplots(figsize=(12,5))
    rev_prod.head(15).plot(kind='barh', ax=ax, color=PALETTE[3])
    ax.invert_yaxis()
    ax.set_title("Top 15 Products by Revenue", fontsize=14, fontweight='bold')
    ax.set_xlabel("Revenue (EUR)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'€{x:,.0f}'))
    plt.tight_layout()
    savefig("06_revenue_by_product.png")

    # 2c – revenue distribution per sub (histogram)
    fig, ax = plt.subplots(figsize=(10,4))
    vals = subs_with_rev.clip(upper=subs_with_rev.quantile(0.99))
    ax.hist(vals, bins=50, color=PALETTE[4], edgecolor='white')
    ax.axvline(avg_rev, color='red', linestyle='--', linewidth=1.5, label=f'Mean €{avg_rev:.1f}')
    ax.set_title("Revenue Distribution per Subscription", fontsize=14, fontweight='bold')
    ax.set_xlabel("Revenue (EUR)"); ax.set_ylabel("Subscriptions")
    ax.legend()
    plt.tight_layout()
    savefig("07_revenue_distribution.png")

    return results


# ════════════════════════════════════════════════════════════════════ #
#  3. BILLING PERFORMANCE                                              #
# ════════════════════════════════════════════════════════════════════ #

def analysis_billing(B, B_ok, S):
    print("\n" + "="*60)
    print("  STEP 4 – Billing Performance")
    print("="*60)
    results = {}

    total_attempts = len(B)
    total_success  = len(B_ok)
    success_rate   = total_success / total_attempts * 100 if total_attempts else 0
    results['total_attempts']  = total_attempts
    results['total_success']   = total_success
    results['overall_success_rate'] = success_rate
    print(f"  Total attempts : {total_attempts:,}")
    print(f"  Successful     : {total_success:,} ({success_rate:.1f}%)")

    # success rate by product
    sr_prod = (B.groupby('product_identifier')['is_success']
               .agg(['sum','count'])
               .rename(columns={'sum':'success','count':'total'}))
    sr_prod['rate'] = sr_prod['success'] / sr_prod['total'] * 100
    sr_prod = sr_prod.sort_values('total', ascending=False)
    results['billing_by_product'] = sr_prod

    # failure reasons
    fail_reasons = B[~B['is_success']]['reason'].value_counts().head(10)
    results['failure_reasons'] = fail_reasons
    print(f"\n  Top failure reasons:\n{fail_reasons.head(5).to_string()}")

    # monthly success rate
    monthly_sr = (B.groupby('billing_month')['is_success']
                  .agg(['sum','count'])
                  .rename(columns={'sum':'success','count':'total'}))
    monthly_sr['rate'] = monthly_sr['success'] / monthly_sr['total'] * 100
    results['monthly_success_rate'] = monthly_sr

    # success rate by publisher
    if 'publisher_id' in S.columns:
        merged = S[['contract_id','publisher_id']].merge(
            B[['contract_id','is_success']], on='contract_id', how='inner')
        sr_pub = (merged.groupby('publisher_id')['is_success']
                  .agg(['sum','count'])
                  .rename(columns={'sum':'success','count':'total'}))
        sr_pub['rate'] = sr_pub['success'] / sr_pub['total'] * 100
        sr_pub = sr_pub[sr_pub['total'] >= 50].sort_values('rate', ascending=False)
        results['billing_rate_by_publisher'] = sr_pub

    # ── CHARTS ──────────────────────────────────────────────────────

    # 3a – success vs failure donut
    fig, ax = plt.subplots(figsize=(7,7))
    sizes = [total_success, total_attempts - total_success]
    labels = [f'Success\n{total_success:,}', f'Failed\n{total_attempts - total_success:,}']
    ax.pie(sizes, labels=labels, autopct='%1.1f%%',
           colors=[PALETTE[2], PALETTE[3]],
           startangle=90, wedgeprops={'edgecolor':'white','linewidth':2})
    ax.set_title("Billing Attempts: Success vs Failure", fontsize=14, fontweight='bold')
    plt.tight_layout()
    savefig("08_billing_success_rate.png")

    # 3b – top products success rate (bubble: size=total attempts)
    top_prods = sr_prod.head(15).copy()
    fig, ax = plt.subplots(figsize=(12,5))
    colors = [PALETTE[2] if r >= 30 else PALETTE[1] if r >= 15 else PALETTE[3]
              for r in top_prods['rate']]
    bars = ax.barh(top_prods.index, top_prods['rate'], color=colors, edgecolor='white')
    ax.axvline(success_rate, color='red', linestyle='--', linewidth=1.5,
               label=f'Overall {success_rate:.1f}%')
    ax.set_title("Billing Success Rate by Product (Top 15)", fontsize=14, fontweight='bold')
    ax.set_xlabel("Success Rate (%)")
    ax.invert_yaxis()
    ax.legend()
    plt.tight_layout()
    savefig("09_billing_success_by_product.png")

    # 3c – monthly success rate trend
    fig, ax = plt.subplots(figsize=(12,4))
    monthly_sr['rate'].plot(kind='line', ax=ax, marker='o', color=PALETTE[0], linewidth=2)
    ax.set_title("Monthly Billing Success Rate", fontsize=14, fontweight='bold')
    ax.set_xlabel("Month"); ax.set_ylabel("Success Rate (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'{x:.0f}%'))
    ax.set_xticklabels([str(p) for p in monthly_sr.index], rotation=30, ha='right')
    plt.tight_layout()
    savefig("10_monthly_billing_success.png")

    # 3d – failure reasons
    fig, ax = plt.subplots(figsize=(12,4))
    fail_reasons.head(8).plot(kind='barh', ax=ax, color=PALETTE[3], edgecolor='white')
    ax.invert_yaxis()
    ax.set_title("Top Billing Failure Reasons", fontsize=14, fontweight='bold')
    ax.set_xlabel("Count")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'{int(x):,}'))
    plt.tight_layout()
    savefig("11_failure_reasons.png")

    return results


# ════════════════════════════════════════════════════════════════════ #
#  4. RETENTION & CHURN                                                #
# ════════════════════════════════════════════════════════════════════ #

def analysis_retention(C, S):
    print("\n" + "="*60)
    print("  STEP 5 – Retention & Churn")
    print("="*60)
    results = {}

    # churn rate
    churn_rate = C['churned'].mean() * 100
    avg_lifetime = C['lifetime_days'].mean()
    results['churn_rate']    = churn_rate
    results['avg_lifetime']  = avg_lifetime
    print(f"  Churn rate        : {churn_rate:.1f}%")
    print(f"  Avg lifetime      : {avg_lifetime:.1f} days")

    # lifetime distribution
    lt_stats = C['lifetime_days'].describe()
    results['lifetime_stats'] = lt_stats

    # cohort analysis – survival at 30/60/90 days
    cohorts = []
    for month, grp in C.groupby('signup_month'):
        total = len(grp)
        for days in [7, 14, 30, 60, 90]:
            alive = (grp['lifetime_days'] >= days).sum()
            cohorts.append({'month': str(month), 'days': days,
                            'retention_pct': alive / total * 100})
    cohort_df = pd.DataFrame(cohorts)
    cohort_pivot = cohort_df.pivot(index='month', columns='days', values='retention_pct')
    results['cohort_retention'] = cohort_pivot

    # churn by campaign
    if 'campaign_id' in S.columns:
        merged = S[['contract_id','campaign_id']].merge(
            C[['id','churned','lifetime_days']].rename(columns={'id':'contract_id'}),
            on='contract_id', how='inner')
        churn_camp = (merged.groupby('campaign_id')
                      .agg(total=('contract_id','count'),
                           churned=('churned','sum'),
                           avg_lt=('lifetime_days','mean'))
                      .assign(churn_rate=lambda x: x['churned']/x['total']*100))
        churn_camp = churn_camp[churn_camp['total'] >= 30].sort_values('churn_rate')
        results['churn_by_campaign'] = churn_camp

    # churn by OS
    if 'os_family' in S.columns:
        merged_os = S[['contract_id','os_family']].merge(
            C[['id','churned','lifetime_days']].rename(columns={'id':'contract_id'}),
            on='contract_id', how='inner')
        churn_os = (merged_os.groupby('os_family')
                   .agg(total=('contract_id','count'),
                        churned=('churned','sum'),
                        avg_lt=('lifetime_days','mean'))
                   .assign(churn_rate=lambda x: x['churned']/x['total']*100))
        results['churn_by_os'] = churn_os

    # churn by publisher
    if 'publisher_id' in S.columns:
        merged_pub = S[['contract_id','publisher_id']].merge(
            C[['id','churned','lifetime_days']].rename(columns={'id':'contract_id'}),
            on='contract_id', how='inner')
        churn_pub = (merged_pub.groupby('publisher_id')
                    .agg(total=('contract_id','count'),
                         churned=('churned','sum'),
                         avg_lt=('lifetime_days','mean'))
                    .assign(churn_rate=lambda x: x['churned']/x['total']*100))
        churn_pub = churn_pub[churn_pub['total'] >= 30].sort_values('churn_rate')
        results['churn_by_publisher'] = churn_pub

    # ── CHARTS ──────────────────────────────────────────────────────

    # 4a – cohort heatmap
    fig, ax = plt.subplots(figsize=(12,6))
    sns.heatmap(cohort_pivot.astype(float), annot=True, fmt='.0f',
                cmap='RdYlGn', ax=ax, linewidths=0.5,
                cbar_kws={'label': 'Retention %'})
    ax.set_title("Cohort Retention (%) by Days After Signup", fontsize=14, fontweight='bold')
    ax.set_xlabel("Days Since Signup")
    ax.set_ylabel("Signup Month")
    plt.tight_layout()
    savefig("12_cohort_retention_heatmap.png")

    # 4b – lifetime distribution
    fig, ax = plt.subplots(figsize=(10,4))
    lt_clipped = C['lifetime_days'].clip(upper=C['lifetime_days'].quantile(0.98))
    ax.hist(lt_clipped, bins=60, color=PALETTE[0], edgecolor='white')
    ax.axvline(avg_lifetime, color='red', linestyle='--', linewidth=1.5,
               label=f'Mean {avg_lifetime:.0f}d')
    ax.set_title("Subscription Lifetime Distribution", fontsize=14, fontweight='bold')
    ax.set_xlabel("Days"); ax.set_ylabel("Count")
    ax.legend()
    plt.tight_layout()
    savefig("13_lifetime_distribution.png")

    # 4c – churn by OS
    if 'churn_by_os' in results:
        fig, ax = plt.subplots(figsize=(10,4))
        co = results['churn_by_os'].sort_values('churn_rate', ascending=True).head(12)
        bars = ax.barh(co.index, co['churn_rate'],
                       color=[PALETTE[2] if r < churn_rate else PALETTE[3]
                              for r in co['churn_rate']])
        ax.axvline(churn_rate, color='red', linestyle='--',
                   label=f'Overall {churn_rate:.1f}%')
        ax.set_title("Churn Rate by OS Family", fontsize=14, fontweight='bold')
        ax.set_xlabel("Churn Rate (%)")
        ax.legend()
        plt.tight_layout()
        savefig("14_churn_by_os.png")

    return results


# ════════════════════════════════════════════════════════════════════ #
#  5. MARKETING EFFECTIVENESS                                          #
# ════════════════════════════════════════════════════════════════════ #

def analysis_marketing(C, S, B_ok):
    print("\n" + "="*60)
    print("  STEP 6 – Marketing Effectiveness")
    print("="*60)
    results = {}
    EUR = 100.0

    # join signups + contracts + revenue
    base = S[['contract_id','campaign_id','publisher_id',
              'os_family','referrer','referrer_host']].copy()
    c_slim = C[['id','churned','lifetime_days']].rename(columns={'id':'contract_id'})
    base = base.merge(c_slim, on='contract_id', how='left')

    rev_per_contract = (B_ok.groupby('contract_id')['amount_in_euro_cents']
                        .sum() / EUR).reset_index()
    rev_per_contract.columns = ['contract_id','revenue']
    base = base.merge(rev_per_contract, on='contract_id', how='left')
    base['revenue'] = base['revenue'].fillna(0)

    def segment_summary(df, col, min_subs=50):
        g = df.groupby(col).agg(
            subscriptions=('contract_id','count'),
            total_revenue=('revenue','sum'),
            churn_rate=('churned', lambda x: x.mean()*100),
            avg_lifetime=('lifetime_days','mean'),
            avg_revenue=('revenue','mean')
        ).reset_index()
        g = g[g['subscriptions'] >= min_subs]
        g['revenue_per_sub'] = g['total_revenue'] / g['subscriptions']
        return g.sort_values('total_revenue', ascending=False)

    # Campaign analysis
    if 'campaign_id' in base.columns:
        camp_summary = segment_summary(base, 'campaign_id', min_subs=30)
        results['campaign_summary'] = camp_summary
        print(f"\n  Campaigns analyzed: {len(camp_summary)}")

    # Publisher analysis
    if 'publisher_id' in base.columns:
        pub_summary = segment_summary(base, 'publisher_id', min_subs=30)
        results['publisher_summary'] = pub_summary
        print(f"  Publishers analyzed: {len(pub_summary)}")

    # OS analysis
    if 'os_family' in base.columns:
        os_summary = segment_summary(base, 'os_family', min_subs=10)
        results['os_summary'] = os_summary

    # ── CHARTS ──────────────────────────────────────────────────────

    # 5a – Campaign: Revenue vs Churn bubble chart
    if 'campaign_summary' in results:
        cs = results['campaign_summary'].copy()
        top_cs = cs.nlargest(20, 'subscriptions')
        fig, ax = plt.subplots(figsize=(12,7))
        sc = ax.scatter(
            top_cs['churn_rate'],
            top_cs['avg_revenue'],
            s=top_cs['subscriptions']/top_cs['subscriptions'].max()*1500+50,
            c=top_cs['total_revenue'],
            cmap='RdYlGn', alpha=0.75, edgecolors='grey', linewidths=0.5)
        plt.colorbar(sc, ax=ax, label='Total Revenue (EUR)')
        for _, row in top_cs.iterrows():
            ax.annotate(str(row['campaign_id'])[:12],
                        (row['churn_rate'], row['avg_revenue']),
                        fontsize=7, alpha=0.8)
        ax.set_xlabel("Churn Rate (%)")
        ax.set_ylabel("Avg Revenue per Sub (EUR)")
        ax.set_title("Campaign Quality: Revenue vs Churn\n(bubble = subscription volume)",
                     fontsize=13, fontweight='bold')
        plt.tight_layout()
        savefig("15_campaign_quality.png")

        # Top 15 campaigns by revenue
        fig, ax = plt.subplots(figsize=(12,5))
        cs.head(15).set_index('campaign_id')['total_revenue'].plot(
            kind='barh', ax=ax, color=PALETTE[1])
        ax.invert_yaxis()
        ax.set_title("Top 15 Campaigns by Revenue", fontsize=14, fontweight='bold')
        ax.set_xlabel("Total Revenue (EUR)")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'€{x:,.0f}'))
        plt.tight_layout()
        savefig("16_top_campaigns_revenue.png")

    # 5b – Publisher: Revenue vs Churn bubble
    if 'publisher_summary' in results:
        ps = results['publisher_summary'].copy()
        top_ps = ps.nlargest(20, 'subscriptions')
        fig, ax = plt.subplots(figsize=(12,7))
        sc = ax.scatter(
            top_ps['churn_rate'],
            top_ps['avg_revenue'],
            s=top_ps['subscriptions']/top_ps['subscriptions'].max()*1500+50,
            c=top_ps['total_revenue'],
            cmap='RdYlGn', alpha=0.75, edgecolors='grey', linewidths=0.5)
        plt.colorbar(sc, ax=ax, label='Total Revenue (EUR)')
        for _, row in top_ps.iterrows():
            ax.annotate(str(row['publisher_id'])[:14],
                        (row['churn_rate'], row['avg_revenue']),
                        fontsize=7, alpha=0.8)
        ax.set_xlabel("Churn Rate (%)")
        ax.set_ylabel("Avg Revenue per Sub (EUR)")
        ax.set_title("Publisher Quality: Revenue vs Churn\n(bubble = subscription volume)",
                     fontsize=13, fontweight='bold')
        plt.tight_layout()
        savefig("17_publisher_quality.png")

    # 5c – OS family: revenue + churn side-by-side
    if 'os_summary' in results:
        os_s = results['os_summary'].copy()
        fig, axes = plt.subplots(1, 2, figsize=(14,5))
        os_s.set_index('os_family')['total_revenue'].sort_values(ascending=False).head(10)\
            .plot(kind='barh', ax=axes[0], color=PALETTE[2])
        axes[0].invert_yaxis()
        axes[0].set_title("Revenue by OS Family", fontweight='bold')
        axes[0].set_xlabel("Total Revenue (EUR)")
        axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'€{x:,.0f}'))

        os_s.set_index('os_family')['churn_rate'].sort_values(ascending=True).head(10)\
            .plot(kind='barh', ax=axes[1], color=PALETTE[3])
        axes[1].invert_yaxis()
        axes[1].set_title("Churn Rate by OS Family", fontweight='bold')
        axes[1].set_xlabel("Churn Rate (%)")
        plt.suptitle("OS Family Analysis", fontsize=14, fontweight='bold')
        plt.tight_layout()
        savefig("18_os_analysis.png")

    return results, base


# ════════════════════════════════════════════════════════════════════ #
#  SAVE SUMMARY CSVs                                                   #
# ════════════════════════════════════════════════════════════════════ #

def save_summaries(sub_r, rev_r, bill_r, ret_r, mkt_r):
    print("\n" + "="*60)
    print("  STEP 7 – Saving Summary CSVs")
    print("="*60)
    out = os.path.join(BASE, "analysis")

    def save(df, name):
        path = os.path.join(out, name)
        if isinstance(df, pd.Series):
            df.to_csv(path)
        else:
            df.to_csv(path)
        print(f"   saved → analysis/{name}")

    save(sub_r['by_product'],          "subs_by_product.csv")
    save(sub_r['monthly_signups'],     "subs_monthly.csv")
    save(rev_r['revenue_by_product'],  "revenue_by_product.csv")
    save(rev_r['monthly_revenue'],     "revenue_monthly.csv")
    save(bill_r['billing_by_product'], "billing_by_product.csv")
    save(bill_r['failure_reasons'],    "billing_failure_reasons.csv")
    save(ret_r['cohort_retention'],    "cohort_retention.csv")
    if 'campaign_summary' in mkt_r:
        save(mkt_r['campaign_summary'], "campaign_summary.csv")
    if 'publisher_summary' in mkt_r:
        save(mkt_r['publisher_summary'],"publisher_summary.csv")
    if 'os_summary' in mkt_r:
        save(mkt_r['os_summary'],       "os_summary.csv")


# ════════════════════════════════════════════════════════════════════ #
#  MAIN                                                                #
# ════════════════════════════════════════════════════════════════════ #

if __name__ == "__main__":
    t_start = time.time()
    print("PL Assignment -- Full Data Analysis")
    C, S, B, B_ok = load_data()
    sub_r  = analysis_subscriptions(C, S)
    rev_r  = analysis_revenue(C, S, B_ok)
    bill_r = analysis_billing(B, B_ok, S)
    ret_r  = analysis_retention(C, S)
    mkt_r, base_df = analysis_marketing(C, S, B_ok)
    save_summaries(sub_r, rev_r, bill_r, ret_r, mkt_r)
    elapsed = time.time() - t_start
    print("ALL DONE in %.1fs" % elapsed)
