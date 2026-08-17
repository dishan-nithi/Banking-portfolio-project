import os
import random
import time
import yaml
import psycopg2
import psycopg2.errors
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
fake = Faker()

DB_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "banking"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)


def load_config(path="config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def weighted_choice(options: dict) -> str:
    labels = list(options.keys())
    weights = list(options.values())
    return random.choices(labels, weights=weights, k=1)[0]


def create_customer(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO customers (first_name, last_name, email, status)
            VALUES (%s, %s, %s, 'PENDING')
            RETURNING id
            """,
            (fake.first_name(), fake.last_name(), fake.unique.email()),
        )
        customer_id = cur.fetchone()[0]
    conn.commit()
    print(f"[new_customer] created customer {customer_id}, status PENDING")


def create_account(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM customers WHERE status = 'PENDING' ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            print("[new_account] no pending customers available, skipping")
            return
        customer_id = row[0]

        account_type = random.choice(["SAVINGS", "CHECKING"])
        cur.execute(
            """
            INSERT INTO accounts (customer_id, account_type)
            VALUES (%s, %s)
            RETURNING id
            """,
            (customer_id, account_type),
        )
        account_id = cur.fetchone()[0]

        cur.execute(
            "UPDATE customers SET status = 'ACTIVE' WHERE id = %s", (customer_id,)
        )

        # The account starts at balance 0, matching the schema default. Any
        # opening funds travel through a real transaction, so the trigger
        # records them properly, rather than being written directly into
        # accounts.balance with no transaction behind them.
        opening_deposit = round(random.uniform(20, 200), 2)
        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount) VALUES (%s, 'DEPOSIT', %s)",
            (account_id, opening_deposit),
        )
    conn.commit()
    print(f"[new_account] customer {customer_id} opened account {account_id} ({account_type}), opening deposit {opening_deposit}")


def update_customer_email(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM customers WHERE status = 'ACTIVE' ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            print("[update_customer_email] no active customers available, skipping")
            return
        customer_id = row[0]
        new_email = fake.unique.email()
        cur.execute(
            "UPDATE customers SET email = %s WHERE id = %s", (new_email, customer_id)
        )
    conn.commit()
    print(f"[update_customer_email] customer {customer_id} email changed")


def close_account(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM accounts WHERE status = 'ACTIVE' AND balance = 0 ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            print("[close_account] no zero-balance active accounts available, skipping")
            return
        account_id = row[0]
        cur.execute(
            "UPDATE accounts SET status = 'CLOSED', closed_at = now() WHERE id = %s",
            (account_id,),
        )
    conn.commit()
    print(f"[close_account] account {account_id} closed")


def delete_pending_customer(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id FROM customers c
            LEFT JOIN accounts a ON a.customer_id = c.id
            WHERE c.status = 'PENDING' AND a.id IS NULL
            ORDER BY random() LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None:
            print("[delete_pending_customer] no deletable customers available, skipping")
            return
        customer_id = row[0]
        cur.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
    conn.commit()
    print(f"[delete_pending_customer] customer {customer_id} deleted")


def create_transaction(conn, cfg):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, balance FROM accounts WHERE status = 'ACTIVE' ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            print("[transaction] no active accounts available, skipping")
            return
        account_id, balance = row

        txn_type = weighted_choice(cfg["transaction_types"]).upper()
        amount = round(random.uniform(cfg["amount_min"], cfg["amount_max"]), 2)

        try:
            if txn_type == "DEPOSIT":
                cur.execute(
                    "INSERT INTO transactions (account_id, txn_type, amount) VALUES (%s, %s, %s)",
                    (account_id, txn_type, amount),
                )

            elif txn_type == "WITHDRAWAL":
                amount = min(amount, float(balance)) if balance else 0
                if amount <= 0:
                    print(f"[transaction] account {account_id} has no balance to withdraw, skipping")
                    conn.rollback()
                    return
                cur.execute(
                    "INSERT INTO transactions (account_id, txn_type, amount) VALUES (%s, %s, %s)",
                    (account_id, txn_type, amount),
                )

            elif txn_type == "TRANSFER":
                cur.execute(
                    "SELECT id FROM accounts WHERE status = 'ACTIVE' AND id != %s ORDER BY random() LIMIT 1",
                    (account_id,),
                )
                related = cur.fetchone()
                if related is None:
                    print("[transaction] no second account available for transfer, skipping")
                    conn.rollback()
                    return
                related_id = related[0]
                amount = min(amount, float(balance)) if balance else 0
                if amount <= 0:
                    print(f"[transaction] account {account_id} has no balance to transfer, skipping")
                    conn.rollback()
                    return
                cur.execute(
                    """
                    INSERT INTO transactions (account_id, txn_type, amount, related_account_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (account_id, txn_type, amount, related_id),
                )

            conn.commit()
            print(f"[transaction] {txn_type} of {amount} on account {account_id}")

        except (psycopg2.errors.CheckViolation, psycopg2.errors.RaiseException) as e:
            conn.rollback()
            print(f"[transaction] rejected by the database (as expected): {e.diag.message_primary}")


def run_iteration(conn, cfg):
    action = weighted_choice(cfg["actions"])
    try:
        if action == "new_customer":
            create_customer(conn)
        elif action == "new_account":
            create_account(conn)
        elif action == "update_customer_email":
            update_customer_email(conn)
        elif action == "close_account":
            close_account(conn)
        elif action == "delete_pending_customer":
            delete_pending_customer(conn)
        elif action == "transaction":
            create_transaction(conn, cfg)
    except psycopg2.errors.ForeignKeyViolation as e:
        conn.rollback()
        print(f"[{action}] rejected by the database (as expected): {e.diag.message_primary}")

def seed_initial_data(conn, count=8):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM accounts")
        existing = cur.fetchone()[0]
    if existing > 0:
        return
    print(f"No existing accounts found, seeding {count} starter customers and accounts...")
    for _ in range(count):
        create_customer(conn)
        create_account(conn)
        
def main(iterations=None):
    cfg = load_config()
    conn = psycopg2.connect(**DB_CONFIG)
    seed_initial_data(conn)
    count = 0
    try:
        while iterations is None or count < iterations:
            run_iteration(conn, cfg)
            count += 1
            if count % 200 == 0:
                fake.unique.clear()  # avoid ever exhausting the unique email pool on a long run
            time.sleep(cfg["sleep_seconds"])
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        conn.close()


if __name__ == "__main__":
    main()