
-- Таблицы
CREATE TABLE customers (
    id          NUMBER(10) PRIMARY KEY,
    full_name   VARCHAR2(100) NOT NULL,
    email       VARCHAR2(150),
    created_at  DATE DEFAULT SYSDATE
);

CREATE TABLE accounts (
    id          NUMBER(12) PRIMARY KEY,
    customer_id NUMBER(10) NOT NULL,
    balance     NUMBER(19,2) DEFAULT 0.00,
    currency    VARCHAR2(3) DEFAULT 'RUB',
    status      VARCHAR2(20) DEFAULT 'ACTIVE',
    opened_at   DATE DEFAULT SYSDATE,
    CONSTRAINT fk_account_customer FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE transactions (
    id            NUMBER(18) PRIMARY KEY,
    from_account  NUMBER(12),
    to_account    NUMBER(12),
    amount        NUMBER(19,2) NOT NULL,
    description   VARCHAR2(255),
    trans_date    DATE DEFAULT SYSDATE
);

CREATE TABLE audit_log (
    log_id      NUMBER(18) PRIMARY KEY,
    action      VARCHAR2(50) NOT NULL,
    details     VARCHAR2(4000),
    logged_at   DATE DEFAULT SYSDATE
);

-- Индексы
CREATE INDEX idx_accounts_customer ON accounts(customer_id);
CREATE INDEX idx_trans_from ON transactions(from_account);
CREATE INDEX idx_trans_to ON transactions(to_account);

-- Последовательности
CREATE SEQUENCE seq_customer_id START WITH 1000 INCREMENT BY 1;
CREATE SEQUENCE seq_account_id START WITH 20000 INCREMENT BY 1;
CREATE SEQUENCE seq_trans_id START WITH 100000 INCREMENT BY 1;
CREATE SEQUENCE seq_log_id START WITH 1 INCREMENT BY 1;

-- PL/SQL: функция
CREATE OR REPLACE FUNCTION get_balance(p_account_id IN NUMBER) RETURN NUMBER IS
    v_bal NUMBER;
BEGIN
    SELECT NVL(balance, 0) INTO v_bal FROM accounts WHERE id = p_account_id;
    RETURN v_bal;
END;
/

-- PL/SQL: процедура
CREATE OR REPLACE PROCEDURE transfer_funds(
    p_from_acc IN NUMBER,
    p_to_acc   IN NUMBER,
    p_amount   IN NUMBER
) AS
    v_from_bal NUMBER;
BEGIN
    SELECT balance INTO v_from_bal FROM accounts WHERE id = p_from_acc FOR UPDATE;
    IF v_from_bal < p_amount THEN
        RAISE_APPLICATION_ERROR(-20001, 'Insufficient funds');
    END IF;

    UPDATE accounts SET balance = balance - p_amount WHERE id = p_from_acc;
    UPDATE accounts SET balance = balance + p_amount WHERE id = p_to_acc;

    INSERT INTO audit_log (log_id, action, details, logged_at)
    VALUES (seq_log_id.NEXTVAL, 'TRANSFER',
            'From ' || p_from_acc || ' to ' || p_to_acc || ', amount=' || p_amount,
            SYSDATE);
END;
/

-- Данные
INSERT INTO customers (id, full_name, email) VALUES (seq_customer_id.NEXTVAL, 'Иван Петров', 'ivan@example.com');
INSERT INTO customers (id, full_name, email) VALUES (seq_customer_id.NEXTVAL, 'Мария Сидорова', 'maria@example.com');

INSERT INTO accounts (id, customer_id, balance) VALUES (seq_account_id.NEXTVAL, 1000, 10000.50);
INSERT INTO accounts (id, customer_id, balance) VALUES (seq_account_id.NEXTVAL, 1001, 5000.00);

INSERT INTO transactions (id, from_account, to_account, amount, description)
VALUES (seq_trans_id.NEXTVAL, 20000, 20001, 1000.00, 'Оплата услуг');

COMMIT;