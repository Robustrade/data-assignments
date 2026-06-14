CREATE TABLE customers (
  customer_id STRING NOT NULL,
  email STRING NOT NULL,
  full_name STRING NOT NULL,
  phone_number STRING,
  kyc_level STRING,
  customer_status STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT customers_pk PRIMARY KEY (customer_id),
  CONSTRAINT customers_status_chk CHECK (customer_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
  CONSTRAINT customers_kyc_chk CHECK (kyc_level IS NULL OR kyc_level IN ('BASIC', 'ENHANCED'))
);

CREATE TABLE wallet_accounts (
  wallet_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  currency STRING NOT NULL,
  balance_minor BIGINT NOT NULL,
  wallet_status STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT wallet_accounts_pk PRIMARY KEY (wallet_id),
  CONSTRAINT wallet_customer_fk FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT wallet_balance_non_negative CHECK (balance_minor >= 0),
  CONSTRAINT wallet_currency_chk CHECK (currency IN ('USD', 'EUR', 'GBP')),
  CONSTRAINT wallet_status_chk CHECK (wallet_status IN ('OPEN', 'FROZEN', 'CLOSED'))
);

CREATE TABLE merchants (
  merchant_id STRING NOT NULL,
  merchant_name STRING NOT NULL,
  merchant_category STRING NOT NULL,
  merchant_status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT merchants_pk PRIMARY KEY (merchant_id),
  CONSTRAINT merchant_status_chk CHECK (merchant_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED'))
);

CREATE TABLE payment_transactions (
  transaction_id STRING NOT NULL,
  wallet_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  merchant_id STRING,
  amount_minor BIGINT NOT NULL,
  currency STRING NOT NULL,
  transaction_status STRING NOT NULL,
  failure_code STRING,
  event_time TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT payment_transactions_pk PRIMARY KEY (transaction_id),
  CONSTRAINT payment_wallet_fk FOREIGN KEY (wallet_id) REFERENCES wallet_accounts(wallet_id),
  CONSTRAINT payment_customer_fk FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT payment_merchant_fk FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
  CONSTRAINT payment_amount_positive CHECK (amount_minor > 0),
  CONSTRAINT payment_currency_chk CHECK (currency IN ('USD', 'EUR', 'GBP')),
  CONSTRAINT payment_status_chk CHECK (transaction_status IN ('AUTHORIZED', 'SETTLED', 'FAILED', 'REVERSED'))
);

CREATE TABLE payment_attempts (
  attempt_id STRING NOT NULL,
  transaction_id STRING NOT NULL,
  processor_name STRING NOT NULL,
  attempt_status STRING NOT NULL,
  failure_code STRING,
  attempted_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT payment_attempts_pk PRIMARY KEY (attempt_id),
  CONSTRAINT attempt_transaction_fk FOREIGN KEY (transaction_id) REFERENCES payment_transactions(transaction_id),
  CONSTRAINT attempt_status_chk CHECK (attempt_status IN ('REQUESTED', 'APPROVED', 'DECLINED', 'ERROR'))
);

CREATE INDEX customers_email_idx ON customers(email);
CREATE INDEX wallet_accounts_customer_idx ON wallet_accounts(customer_id);
CREATE INDEX payment_transactions_wallet_time_idx ON payment_transactions(wallet_id, event_time);
CREATE INDEX payment_transactions_customer_time_idx ON payment_transactions(customer_id, event_time);
CREATE INDEX payment_attempts_transaction_idx ON payment_attempts(transaction_id);

