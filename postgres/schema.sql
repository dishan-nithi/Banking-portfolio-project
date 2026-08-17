-- ============================================
-- Customers
-- ============================================
CREATE TABLE customers (
    id            BIGSERIAL PRIMARY KEY,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'ACTIVE', 'CLOSED')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================
-- Accounts
-- ============================================
CREATE TABLE accounts (
    id            BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT NOT NULL REFERENCES customers(id) ON DELETE RESTRICT,
    account_type  VARCHAR(20) NOT NULL
                  CHECK (account_type IN ('SAVINGS', 'CHECKING')),
    balance       NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    currency      CHAR(3) NOT NULL DEFAULT 'USD',
    status        VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
                  CHECK (status IN ('ACTIVE', 'CLOSED')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at     TIMESTAMPTZ NULL
);

-- ============================================
-- Transactions
-- ============================================
CREATE TABLE transactions (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    txn_type            VARCHAR(20) NOT NULL
                        CHECK (txn_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')),
    amount              NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    related_account_id  BIGINT NULL REFERENCES accounts(id),
    status              VARCHAR(20) NOT NULL DEFAULT 'COMPLETED'
                        CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_transactions_account_created
    ON transactions (account_id, created_at);

-- ============================================
-- updated_at trigger
-- ============================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================
-- balance trigger
-- ============================================
CREATE OR REPLACE FUNCTION apply_transaction_to_balance()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status <> 'COMPLETED' THEN
        RETURN NEW;
    END IF;

    IF NEW.txn_type = 'DEPOSIT' THEN
        UPDATE accounts SET balance = balance + NEW.amount
        WHERE id = NEW.account_id;

    ELSIF NEW.txn_type = 'WITHDRAWAL' THEN
        UPDATE accounts SET balance = balance - NEW.amount
        WHERE id = NEW.account_id;

    ELSIF NEW.txn_type = 'TRANSFER' THEN
        IF NEW.related_account_id IS NULL THEN
            RAISE EXCEPTION 'TRANSFER requires related_account_id';
        END IF;
        UPDATE accounts SET balance = balance - NEW.amount
        WHERE id = NEW.account_id;
        UPDATE accounts SET balance = balance + NEW.amount
        WHERE id = NEW.related_account_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_apply_transaction
    AFTER INSERT ON transactions
    FOR EACH ROW EXECUTE FUNCTION apply_transaction_to_balance();