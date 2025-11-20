# app.py
"""
BankSight — Full Streamlit dashboard (single-file, multipage)
Features:
 - Auto-create SQLite DB on first run from data/ CSVs & JSONs
 - View Tables (all datasets)
 - Filter Data (multi-column filters)
 - CRUD operations (create/read/update/delete)
 - Credit/Debit simulation (with min balance enforcement)
 - Analytical Insights (15 SQL queries)
 - Intro & About pages

Place your source files in ./data:
 - customers.csv
 - accounts.csv
 - transactions.csv
 - branches.json
 - loans.json
 - credit_cards.json
 - support_tickets.json

Run:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine, text
import sqlite3
import datetime

# ---------------------------
# Paths & DB connection setup
# ---------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "banksight.db"

def get_engine():
    # Use absolute path to avoid relative-path issues when running Streamlit
    return create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})

# ---------------------------
# Utilities: JSON reader (handles JSON or JSON lines)
# ---------------------------
def read_json_file(path: Path) -> pd.DataFrame:
    """Read JSON file. Supports standard JSON array or newline-delimited JSON (ndjson)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        return df
    except json.JSONDecodeError:
        # try JSON lines
        records = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except Exception:
                    # skip bad line
                    continue
        return pd.json_normalize(records)

# ---------------------------
# Auto-create DB if missing
# ---------------------------
def create_database_from_data():
    """Create banksight.db from CSV and JSON files in data/ if DB does not exist."""
    if DB_PATH.exists():
        return False  # DB already exists -> nothing to do

    # Make sure data dir exists
    if not DATA_DIR.exists():
        st.warning(f"Data folder not found at {DATA_DIR}. Place your CSV/JSON files there.")
        return False

    engine = get_engine()

    # Expected files and table mapping
    csv_files = {
        "customers.csv": "customers",
        "accounts.csv": "accounts",
        "transactions.csv": "transactions",
    }
    json_files = {
        "branches.json": "branches",
        "loans.json": "loans",
        "credit_cards.json": "credit_cards",
        "support_tickets.json": "support_tickets",
    }

    # Load CSVs
    for fname, table in csv_files.items():
        p = DATA_DIR / fname
        if p.exists():
            try:
                df = pd.read_csv(p)
                # Basic cleaning: ensure columns lowercase / consistent
                df.columns = [c.strip() for c in df.columns]
                df.to_sql(table, engine, index=False, if_exists="replace")
            except Exception as e:
                st.error(f"Failed to load {fname}: {e}")
        else:
            st.warning(f"{fname} not found in data/ — {table} table will not be created.")

    # Load JSONs
    for fname, table in json_files.items():
        p = DATA_DIR / fname
        if p.exists():
            try:
                df = read_json_file(p)
                df.columns = [c.strip() for c in df.columns]
                df.to_sql(table, engine, index=False, if_exists="replace")
            except Exception as e:
                st.error(f"Failed to load {fname}: {e}")
        else:
            st.warning(f"{fname} not found in data/ — {table} table will not be created.")

    # Create helpful indexes (optional, will not fail if columns missing)
    with engine.connect() as conn:
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_accounts_customer ON accounts(customer_id);"))
        except Exception:
            pass
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_txn_customer ON transactions(customer_id);"))
        except Exception:
            pass

    st.success(f"Database created at {DB_PATH}")
    return True

# ---------------------------
# Cached DB reads (do NOT pass engine as a parameter)
# ---------------------------
@st.cache_data(show_spinner=False)
def read_table(table_name: str, limit: int = 1000) -> pd.DataFrame:
    """Read a table from the DB (cached). Do not pass 'engine' to avoid hashing issues."""
    engine = get_engine()
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    try:
        df = pd.read_sql_query(text(query), engine)
    except Exception:
        # Return empty DF if table missing
        df = pd.DataFrame()
    return df

@st.cache_data(show_spinner=False)
def read_sql_query(query: str) -> pd.DataFrame:
    engine = get_engine()
    try:
        return pd.read_sql_query(text(query), engine)
    except Exception:
        return pd.DataFrame()

# ---------------------------
# CRUD helpers
# ---------------------------
def insert_record(table: str, record: dict):
    engine = get_engine()
    cols = ", ".join(record.keys())
    vals = ", ".join([f":{k}" for k in record.keys()])
    stmt = text(f"INSERT INTO {table} ({cols}) VALUES ({vals})")
    with engine.begin() as conn:
        conn.execute(stmt, record)
    # clear cache
    read_table.clear()
    read_sql_query.clear()

def update_record(table: str, pk_col: str, pk_val, updates: dict):
    engine = get_engine()
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
    params = updates.copy()
    params["pk_val"] = pk_val
    stmt = text(f"UPDATE {table} SET {set_clause} WHERE {pk_col} = :pk_val")
    with engine.begin() as conn:
        conn.execute(stmt, params)
    read_table.clear()
    read_sql_query.clear()

def delete_record(table: str, pk_col: str, pk_val):
    engine = get_engine()
    stmt = text(f"DELETE FROM {table} WHERE {pk_col} = :v")
    with engine.begin() as conn:
        conn.execute(stmt, {"v": pk_val})
    read_table.clear()
    read_sql_query.clear()

# ---------------------------
# Analytical SQL queries (Q1..Q15)
# ---------------------------
SQL_QUERIES = {
    "Q1: Customers per city & avg balance":
        """
        SELECT c.city,
               COUNT(*) AS total_customers,
               ROUND(AVG(a.account_balance),2) AS avg_balance
        FROM customers c
        JOIN accounts a ON c.customer_id = a.customer_id
        GROUP BY c.city
        ORDER BY avg_balance DESC;
        """,

    "Q2: Account type holding highest total balance":
        """
        SELECT c.account_type,
               SUM(a.account_balance) AS total_balance
        FROM customers c
        JOIN accounts a ON c.customer_id = a.customer_id
        GROUP BY c.account_type
        ORDER BY total_balance DESC;
        """,

    "Q3: Top 10 customers by total balance":
        """
        SELECT c.customer_id, c.name, c.city, a.account_balance
        FROM customers c
        JOIN accounts a ON c.customer_id = a.customer_id
        ORDER BY a.account_balance DESC
        LIMIT 10;
        """,

    "Q4: Customers in 2023 with balance > 100000":
        """
        SELECT c.customer_id, c.name, c.city, c.join_date, a.account_balance
        FROM customers c
        JOIN accounts a ON c.customer_id = a.customer_id
        WHERE c.join_date LIKE '2023%' AND a.account_balance > 100000;
        """,

    "Q5: Total transaction volume by type":
        """
        SELECT txn_type, SUM(amount) AS total_volume
        FROM transactions
        GROUP BY txn_type
        ORDER BY total_volume DESC;
        """,

    "Q6: Accounts with >3 failed txns in a month":
        """
        SELECT customer_id, strftime('%Y-%m', txn_time) AS month, COUNT(*) AS failed_count
        FROM transactions
        WHERE LOWER(status) = 'failed'
        GROUP BY customer_id, month
        HAVING COUNT(*) > 3;
        """,

    "Q7: Top 5 branches by txn volume (last 6 months)":
        """
        SELECT b.Branch_Name, SUM(t.amount) AS total_volume
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        JOIN branches b ON c.city = b.City
        WHERE DATE(t.txn_time) >= DATE('now','-6 months')
        GROUP BY b.Branch_Name
        ORDER BY total_volume DESC
        LIMIT 5;
        """,

    "Q8: Accounts with >=5 high-value txns (>200000)":
        """
        SELECT customer_id, COUNT(*) AS high_value_count
        FROM transactions
        WHERE amount > 200000
        GROUP BY customer_id
        HAVING COUNT(*) >= 5;
        """,

    "Q9: Avg loan amount & interest by loan type":
        """
        SELECT Loan_Type, AVG(Loan_Amount) AS avg_amount, AVG(Interest_Rate) AS avg_rate
        FROM loans
        GROUP BY Loan_Type;
        """,

    "Q10: Customers holding >1 active/approved loan":
        """
        SELECT Customer_ID, COUNT(*) AS active_loans
        FROM loans
        WHERE Loan_Status IN ('Active', 'Approved')
        GROUP BY Customer_ID
        HAVING COUNT(*) > 1;
        """,

    "Q11: Top 5 customers with highest outstanding loan amount":
        """
        SELECT Customer_ID, SUM(Loan_Amount) AS total_outstanding
        FROM loans
        WHERE Loan_Status != 'Closed'
        GROUP BY Customer_ID
        ORDER BY total_outstanding DESC
        LIMIT 5;
        """,

    "Q12: Branch with highest total account balance":
        """
        SELECT b.Branch_Name, SUM(a.account_balance) AS total_balance
        FROM accounts a
        JOIN customers c ON a.customer_id = c.customer_id
        JOIN branches b ON c.city = b.City
        GROUP BY b.Branch_Name
        ORDER BY total_balance DESC
        LIMIT 1;
        """,

    "Q13: Branch performance summary":
        """
        SELECT b.Branch_Name,
               COUNT(DISTINCT c.customer_id) AS total_customers,
               COUNT(DISTINCT l.Loan_ID) AS total_loans,
               COALESCE(SUM(t.amount),0) AS transaction_volume
        FROM branches b
        LEFT JOIN customers c ON c.city = b.City
        LEFT JOIN loans l ON l.Branch = b.Branch_Name OR l.Branch = b.City
        LEFT JOIN transactions t ON t.customer_id = c.customer_id
        GROUP BY b.Branch_Name;
        """,

    "Q14: Issue categories with longest avg resolution time":
        """
        SELECT Issue_Category,
               AVG(julianday(Date_Closed) - julianday(Date_Opened)) AS avg_days
        FROM support_tickets
        WHERE Date_Closed IS NOT NULL
        GROUP BY Issue_Category
        ORDER BY avg_days DESC;
        """,

    "Q15: Support agents resolving most critical tickets (rating >=4)":
        """
        SELECT Support_Agent, COUNT(*) AS resolved_critical
        FROM support_tickets
        WHERE Priority = 'Critical' AND Customer_Rating >= 4
        GROUP BY Support_Agent
        ORDER BY resolved_critical DESC;
        """
}

# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="BankSight", layout="wide")
st.title("🏦 BankSight: Transaction Intelligence Dashboard")

# Auto-create DB if missing (runs once)
if not DB_PATH.exists():
    with st.spinner("Creating SQLite database from data/ — this runs only on first start..."):
        created = create_database_from_data()
        if not created:
            st.warning("Database was not created. Fix data in data/ folder and refresh.")
else:
    # show small note
    st.info("Using existing database at " + str(DB_PATH))

# Sidebar navigation (multipage)
page = st.sidebar.radio("Navigation", [
    "🏠 Introduction",
    "📊 View Tables",
    "🔍 Filter Data",
    "✏️ CRUD Operations",
    "💰 Credit / Debit Simulation",
    "🧠 Analytical Insights",
    "👩‍💻 About Creator"
])

# ---------------------------
# Page: Introduction
# ---------------------------
if page == "🏠 Introduction":
    st.header("BankSight — Transaction Intelligence Dashboard")
    st.markdown("""
    **Purpose:** Explore customers, accounts, transactions, loans, credit cards, branches, and support tickets.
    This dashboard reads from a local SQLite database (created from files in `data/`).
    """)
    st.markdown("**Datasets included:** customers, accounts, transactions, branches, loans, credit_cards, support_tickets.")
    st.markdown("Use the left sidebar to navigate between pages.")

# ---------------------------
# Page: View Tables
# ---------------------------
elif page == "📊 View Tables":
    st.header("📊 View Tables")
    tables_available = []
    # check what tables exist
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables_available = [r[0] for r in res.fetchall()]
    st.write("Available tables:", tables_available)

    selected = st.selectbox("Select table to view", options=tables_available if tables_available else ["customers", "accounts", "transactions"])
    limit = st.number_input("Rows to load", min_value=50, max_value=200000, value=1000, step=50)
    df = read_table(selected, limit=int(limit))
    if df.empty:
        st.warning("No data found in table: " + selected)
    else:
        st.dataframe(df)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(), f"{selected}.csv", "text/csv")

# ---------------------------
# Page: Filter Data
# ---------------------------
elif page == "🔍 Filter Data":
    st.header("🔍 Filter Data (multi-column filters)")
    # let user choose a table
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables = [r[0] for r in res.fetchall()]
    table = st.selectbox("Choose dataset", tables)
    # load moderate-sized df
    df = read_table(table, limit=50000)
    if df.empty:
        st.warning("No data to filter for table: " + table)
    else:
        st.write("Columns:", df.columns.tolist())
        # choose columns to show
        show_cols = st.multiselect("Columns to show", options=df.columns.tolist(), default=df.columns.tolist()[:6])
        # build filters
        filters = {}
        for col in show_cols:
            if pd.api.types.is_numeric_dtype(df[col]):
                mn = float(df[col].min(skipna=True)) if pd.notna(df[col].min(skipna=True)) else 0.0
                mx = float(df[col].max(skipna=True)) if pd.notna(df[col].max(skipna=True)) else 0.0
                r = st.slider(f"{col} range", mn, mx, (mn, mx))
                filters[col] = lambda d, c=col, r=r: d[(d[c] >= r[0]) & (d[c] <= r[1])]
            elif pd.api.types.is_datetime64_any_dtype(df[col]) or "date" in col.lower() or "time" in col.lower():
                dmin = pd.to_datetime(df[col], errors='coerce').min()
                dmax = pd.to_datetime(df[col], errors='coerce').max()
                start = st.date_input(f"{col} start", value=(dmin.date() if pd.notna(dmin) else datetime.date.today()))
                end = st.date_input(f"{col} end", value=(dmax.date() if pd.notna(dmax) else datetime.date.today()))
                filters[col] = lambda d, c=col, s=start, e=end: d[(pd.to_datetime(d[c]).dt.date >= s) & (pd.to_datetime(d[c]).dt.date <= e)]
            else:
                vals = df[col].dropna().unique().tolist()[:200]
                sel = st.multiselect(f"{col} values", options=vals, default=vals[:5] if vals else [])
                if sel:
                    filters[col] = lambda d, c=col, sel=sel: d[d[c].isin(sel)]
        # apply filters
        result = df.copy()
        for f in filters.values():
            result = f(result)
        st.write(f"Filtered rows: {len(result)}")
        st.dataframe(result[show_cols])

# ---------------------------
# Page: CRUD Operations
# ---------------------------
elif page == "✏️ CRUD Operations":
    st.header("✏️ CRUD Operations")
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
        tables = [r[0] for r in res.fetchall()]

    table = st.selectbox("Choose table for CRUD", options=tables)
    mode = st.radio("Operation", ["Create", "Read", "Update", "Delete"])
    schema_df = read_sql_query(f"PRAGMA table_info({table});")
    cols = [row['name'] for _, row in schema_df.iterrows()] if not schema_df.empty else []

    if mode == "Create":
        st.subheader("Create new record")
        new = {}
        for c in cols:
            new[c] = st.text_input(c, key=f"create_{c}")
        if st.button("Insert record"):
            # remove empty keys if primary autoinc should be skipped
            payload = {k: v for k, v in new.items() if v != ""}
            try:
                insert_record(table, payload)
                st.success("Inserted record.")
            except Exception as e:
                st.error("Insert failed: " + str(e))

    elif mode == "Read":
        st.subheader("Read / Browse")
        limit = st.number_input("Rows", 10, 10000, 500)
        df = read_table(table, limit=int(limit))
        st.dataframe(df)

    elif mode == "Update":
        st.subheader("Update record by primary key")
        # Attempt to find a primary key column (heuristic)
        pk_col = None
        if not schema_df.empty:
            for _, row in schema_df.iterrows():
                if row.get('pk') == 1 or row.get('pk') == '1' or 'id' in row['name'].lower():
                    pk_col = row['name']
                    break
        if not pk_col and cols:
            pk_col = cols[0]  # fallback
        st.write("Primary key column used:", pk_col)
        pk_val = st.text_input("Primary key value to update")
        updates = {}
        for c in cols:
            if c == pk_col: 
                continue
            updates[c] = st.text_input(f"New value for {c}", key=f"upd_{c}")
        if st.button("Execute update"):
            # prepare only non-empty updates
            upd_payload = {k: v for k, v in updates.items() if v != ""}
            try:
                update_record(table, pk_col, pk_val, upd_payload)
                st.success("Updated successfully.")
            except Exception as e:
                st.error("Update failed: " + str(e))

    elif mode == "Delete":
        st.subheader("Delete record by primary key")
        # use same pk_col heuristic
        pk_col = None
        if not schema_df.empty:
            for _, row in schema_df.iterrows():
                if row.get('pk') == 1 or 'id' in row['name'].lower():
                    pk_col = row['name']
                    break
        if not pk_col and cols:
            pk_col = cols[0]
        st.write("Primary key column used:", pk_col)
        pk_val = st.text_input("Primary key value to delete")
        if st.button("Delete"):
            try:
                delete_record(table, pk_col, pk_val)
                st.success("Deleted (if existed).")
            except Exception as e:
                st.error("Delete failed: " + str(e))

# ---------------------------
# Page: Credit / Debit Simulation
# ---------------------------
elif page == "💰 Credit / Debit Simulation":
    st.header("💰 Credit / Debit Simulation")
    st.markdown("Enter a `customer_id` to load the account balance, then deposit or withdraw (simulation updates DB).")

    cid = st.text_input("Customer ID")
    if st.button("Load balance"):
        df_acc = read_sql_query(f"SELECT * FROM accounts WHERE customer_id = '{cid}'")
        if df_acc.empty:
            st.error("Account not found for customer_id: " + str(cid))
        else:
            st.session_state["curr_balance"] = float(df_acc.iloc[0]["account_balance"])
            st.success(f"Loaded balance: ₹{st.session_state['curr_balance']:.2f}")

    amt = st.number_input("Amount", min_value=0.0, format="%.2f")
    op = st.selectbox("Operation", ["Deposit", "Withdraw"])
    if st.button("Execute transaction"):
        if "curr_balance" not in st.session_state:
            st.error("Load balance first")
        else:
            cur = st.session_state["curr_balance"]
            if op == "Deposit":
                newbal = cur + amt
            else:
                newbal = cur - amt
                if newbal < 1000:
                    st.error("Minimum balance ₹1000 would be violated. Operation aborted.")
                    newbal = cur
            if newbal != cur:
                try:
                    engine = get_engine()
                    with engine.begin() as conn:
                        conn.execute(text("UPDATE accounts SET account_balance = :bal, last_updated = :dt WHERE customer_id = :cid"),
                                     {"bal": newbal, "dt": datetime.datetime.utcnow().isoformat(), "cid": cid})
                    st.session_state["curr_balance"] = newbal
                    read_table.clear()
                    read_sql_query.clear()
                    st.success(f"Balance updated: ₹{newbal:.2f}")
                except Exception as e:
                    st.error("Failed to update balance: " + str(e))

# ---------------------------
# Page: Analytical Insights
# ---------------------------
elif page == "🧠 Analytical Insights":
    st.header("🧠 Analytical Insights (select a question)")
    selected = st.selectbox("Choose insight", list(SQL_QUERIES.keys()))
    if selected:
        q = SQL_QUERIES[selected]
        st.code(q)
        dfq = read_sql_query(q)
        if dfq.empty:
            st.warning("Query returned no rows (or table missing).")
        else:
            st.dataframe(dfq)
            st.download_button("⬇️ Download result as CSV", dfq.to_csv(index=False).encode(), "insight.csv", "text/csv")

# ---------------------------
# Page: About Creator
# ---------------------------
elif page == "👩‍💻 About Creator":
    st.header("About / Contact")
    st.markdown("""
    **Creator:** Devendra Kumar
    
    **Skills:** Python, SQL, Streamlit, Data Analysis, Banking Analytics
    
    **Contact:** devendragkp45@gmail.com
    """)
    st.info("This app is a demo. Do not store production PII in a public repo.")

# ---------------------------
# End of app
# ---------------------------
