# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS robustrade_dev;
# MAGIC USE CATALOG robustrade_dev;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS audit;

# COMMAND ----------

# MAGIC %md
# MAGIC Catalog Check   

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN robustrade_dev;

# COMMAND ----------

# MAGIC %md
# MAGIC # Source Model
# MAGIC
# MAGIC This project models a wallet and payments source system with five tables:
# MAGIC
# MAGIC - customers
# MAGIC - wallet_accounts
# MAGIC - merchants
# MAGIC - payment_transactions
# MAGIC - payment_attempts
# MAGIC
# MAGIC Strong entities:
# MAGIC - customers
# MAGIC - wallet_accounts
# MAGIC - merchants
# MAGIC - payment_transactions
# MAGIC
# MAGIC Weak entity:
# MAGIC - payment_attempts
# MAGIC
# MAGIC payment_attempts is weak because each attempt depends on a parent payment transaction.
# MAGIC
# MAGIC Relationships:
# MAGIC - customers -> wallet_accounts
# MAGIC - customers -> payment_transactions
# MAGIC - wallet_accounts -> payment_transactions
# MAGIC - merchants -> payment_transactions
# MAGIC - payment_transactions -> payment_attempts
# MAGIC
# MAGIC Important invariants:
# MAGIC - wallet balances must be non-negative
# MAGIC - payment amounts must be positive
# MAGIC - statuses must be from allowed values
# MAGIC - payments must reference valid customers, wallets, and merchants
# MAGIC - attempts must reference valid payment transactions