\# CDC Lakehouse



A local CDC lakehouse implementation demonstrating change data capture,

durable event storage, checkpointing, current-state warehouse processing,

historical recovery, schema-change safety, validation parity, and dataset

cataloging.



The project uses SQLite as the simulated transactional source, JSONL files

as the historical CDC lake, and DuckDB as the current-state warehouse.



\## Architecture



The pipeline is organized into the following stages:



1\. Source database

2\. CDC event generation

3\. Durable CDC lake

4\. Change log and checkpointing

5\. Current-state DuckDB warehouse

6\. Historical recovery

7\. Schema safety validation

8\. Warehouse validation parity

9\. Dataset catalog



The CDC lake preserves the complete change history, while the warehouse

maintains the latest state of the source tables.



\## Source Domain



The source system contains:



\- customers

\- products

\- orders

\- order\_items

\- payments



Relationships and source validations are defined in `DESIGN.md` and

`sql/schema.sql`.



\## Repository Structure



```text

.

├── DESIGN.md

├── README.md

├── requirements.txt

├── sql/

│   ├── schema.sql

│   └── seed.sql

├── scripts/

│   └── init\_source\_db.py

├── src/

│   ├── cdc/

│   ├── lake/

│   ├── warehouse/

│   ├── recovery/

│   ├── schema/

│   ├── validation/

│   └── catalog/

├── tests/

├── data/

├── lake/

├── warehouse/

└── catalog/

