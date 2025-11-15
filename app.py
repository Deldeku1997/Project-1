# app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Connect to the SQLite database
engine = create_engine("sqlite:///banksight.db", connect_args={"check_same_thread": False})

# Streamlit page setup
st.set_page_config(page_title="BankSight Dashboard", layout="wide")
st.title("🏦 BankSight: Transaction Intelligence Dashboard")

st.sidebar.title("Navigation")
menu = st.sidebar.radio(
    "Select Table to View",
    [
        "Home",
        "Customers",
        "Accounts",
        "Transactions",
        "Branches",
        "Credit Cards",
        "Loans",
        "Support Tickets"
    ]
)

# Helper function to show tables
def show_table(table_name, limit=1000):
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    df = pd.read_sql(query, engine)
    st.write(f"### {table_name.capitalize()} Table")
    st.dataframe(df)
    st.download_button("⬇️ Download as CSV", df.to_csv(index=False).encode(), f"{table_name}.csv", "text/csv")

# Navigation
if menu == "Home":
    st.subheader("Welcome 👋")
    st.markdown("""
    You have successfully connected your **SQLite database** with **Streamlit**.
    
    Use the sidebar to explore different tables from your data:
    - 🧍 Customers  
    - 💳 Credit Cards  
    - 🏦 Accounts & Transactions  
    - 🏢 Branches  
    - 💰 Loans  
    - 🎟️ Support Ticketss
    """)
    st.success("Database loaded successfully!")
else:
    table_name = menu.lower().replace(" ", "_")
    show_table(table_name)
