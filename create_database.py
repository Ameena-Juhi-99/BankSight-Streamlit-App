import sqlite3
import pandas as pd
import json

# ===================== FILE PATHS =====================
CUSTOMERS_CSV = "customers.csv"
ACCOUNTS_CSV = "accounts.csv"
TRANSACTIONS_CSV = "transactions.csv"
BRANCHES_CSV = "branches.csv"

LOANS_JSON = "loans.json"
SUPPORT_TICKETS_JSON = "support_tickets.json"
CREDIT_CARDS_JSON = "credit_cards.json"

DB_NAME = "banksight.db"

# ===================== TABLE SCHEMAS =====================

CREATE_TABLE_QUERIES = {
    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT,
            gender TEXT,
            age INTEGER,
            city TEXT,
            account_type TEXT,
            join_date TEXT
        );
    """,

    "accounts": """
        CREATE TABLE IF NOT EXISTS accounts (
            customer_id TEXT PRIMARY KEY,
            account_balance REAL,
            last_updated TEXT
        );
    """,

    "transactions": """
        CREATE TABLE IF NOT EXISTS transactions (
            txn_id TEXT PRIMARY KEY,
            customer_id TEXT,
            txn_type TEXT,
            amount REAL,
            txn_time TEXT,
            status TEXT
        );
    """,

    "branches": """
        CREATE TABLE IF NOT EXISTS branches (
            Branch_ID INTEGER PRIMARY KEY,
            Branch_Name TEXT,
            City TEXT,
            Manager_Name TEXT,
            Total_Employees INTEGER,
            Branch_Revenue REAL,
            Opening_Date TEXT,
            Performance_Rating INTEGER
        );
    """,

    "loans": """
        CREATE TABLE IF NOT EXISTS loans (
            Loan_ID INTEGER PRIMARY KEY,
            Customer_ID INTEGER,
            Account_ID INTEGER,
            Branch TEXT,
            Loan_Type TEXT,
            Loan_Amount REAL,
            Interest_Rate REAL,
            Loan_Term_Months INTEGER,
            Start_Date TEXT,
            End_Date TEXT,
            Loan_Status TEXT
        );
    """,

    "support_tickets": """
        CREATE TABLE IF NOT EXISTS support_tickets (
            Ticket_ID TEXT PRIMARY KEY,
            Customer_ID TEXT,
            Account_ID TEXT,
            Loan_ID TEXT,
            Branch_Name TEXT,
            Issue_Category TEXT,
            Description TEXT,
            Date_Opened TEXT,
            Date_Closed TEXT,
            Priority TEXT,
            Status TEXT,
            Resolution_Remarks TEXT,
            Support_Agent TEXT,
            Channel TEXT,
            Customer_Rating REAL
        );
    """,

    "credit_cards": """
        CREATE TABLE IF NOT EXISTS credit_cards (
            Card_ID INTEGER PRIMARY KEY,
            Customer_ID INTEGER,
            Account_ID INTEGER,
            Branch TEXT,
            Card_Number TEXT,
            Card_Type TEXT,
            Card_Network TEXT,
            Credit_Limit REAL,
            Current_Balance REAL,
            Issued_Date TEXT,
            Expiry_Date TEXT,
            Status TEXT
        );
    """
}

# ===================== DATA LOADING HELPERS =====================

def load_csv(conn, path, table):
    print(f"Loading CSV → {table}")
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df.to_sql(table, conn, if_exists="append", index=False)
    print(f"✔ Loaded {len(df)} rows into {table}")


def load_json(conn, path, table):
    print(f"Loading JSON → {table}")
    with open(path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df.columns = df.columns.str.strip()
    df.to_sql(table, conn, if_exists="append", index=False)
    print(f"✔ Loaded {len(df)} rows into {table}")

# ===================== MAIN =====================

def main():
    print("Creating database:", DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create & clear tables
    for table, query in CREATE_TABLE_QUERIES.items():
        print(f"Creating table → {table}")
        cursor.execute(query)
        cursor.execute(f"DELETE FROM {table}")

    conn.commit()

    # Load CSV data
    load_csv(conn, CUSTOMERS_CSV, "customers")
    load_csv(conn, ACCOUNTS_CSV, "accounts")
    load_csv(conn, TRANSACTIONS_CSV, "transactions")
    load_csv(conn, BRANCHES_CSV, "branches")

    # Load JSON data
    load_json(conn, LOANS_JSON, "loans")
    load_json(conn, SUPPORT_TICKETS_JSON, "support_tickets")
    load_json(conn, CREDIT_CARDS_JSON, "credit_cards")

    conn.commit()
    conn.close()

    print("\n🎉 banksight.db successfully created!")


if __name__ == "__main__":
    main()
