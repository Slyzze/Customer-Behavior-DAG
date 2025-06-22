"""
Customer Behavior Analysis DAG
================================
This Airflow DAG orchestrates an end-to-end pipeline that builds analytical
objects for monitoring customer engagement, repeat-purchase patterns, and
cohort retention on an e-commerce platform (e.g. Zalando).

Pipeline overview
-----------------
1. **Stage raw events** – incremental load of click-stream & order events.
2. **Create session-level metrics** – derives behavioural KPIs per session.
3. **Aggregate customer metrics** – builds customer-level facts (frequency,
   recency, monetary value, purchase cadence, etc.).
4. **Publish reporting view** – exposes a semantic layer for BI tools / OKRs.

Airflow DAGs
------------
In Airflow, a DAG (Directed Acyclic Graph) defines a workflow as a set of tasks
with clear dependencies and execution order. This script uses Airflow's
SQLExecuteQueryOperator to orchestrate SQL-based transformations, typically used
in ELT pipelines within a modern data stack (e.g., Snowflake + dbt + BI).

Each task runs a SQL script, promoting modularity and reusability. The use of
Task Groups improves readability by logically grouping transformation steps.

Prerequisites
-------------
* Airflow ≥2.8
* Warehouse connection ID (Snowflake, Redshift, Postgres…)
* SQL scripts stored under the project’s `sql/` directory (see README).

Usage
-----
Place the file inside your `dags/` folder and create a connection called
`snowflake_conn_id` (or update `WAREHOUSE_CONN_ID` below). The DAG runs daily
at 03:00 and has catchup disabled for simplicity.
"""

from __future__ import annotations

from airflow.decorators import dag, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data_platform",
    "retries": 1,
    "retry_delay": 300,  # seconds (5 min)
}

WAREHOUSE_CONN_ID = "snowflake_conn_id"  # Adjust if using Redshift / Postgres


@dag(
    schedule_interval="0 3 * * *",  # Run daily at 03:00 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["analytics", "customer_behavior"],
    description="Builds behavioural metrics and retention KPIs for customers.",
)
def customer_behavior():
    """Main DAG callable."""

    # 1️⃣ Stage raw session & order events (incremental load)
    stage_sessions = SQLExecuteQueryOperator(
        task_id="stage_sessions",
        conn_id=WAREHOUSE_CONN_ID,
        sql="sql/staging/load_sessions_incremental.sql",
    )

    # 2️⃣ Transformations grouped for readability
    with task_group(group_id="transforms") as transforms:
        # Session-level metrics (per visit)
        session_metrics = SQLExecuteQueryOperator(
            task_id="session_metrics",
            conn_id=WAREHOUSE_CONN_ID,
            sql="sql/transforms/create_session_metrics.sql",
        )

        # Customer-level aggregations (RFM, retention, cohorts…)
        customer_metrics = SQLExecuteQueryOperator(
            task_id="customer_metrics",
            conn_id=WAREHOUSE_CONN_ID,
            sql="sql/transforms/create_customer_metrics.sql",
        )

        # Task dependencies inside the group
        session_metrics >> customer_metrics

    # 3️⃣ Publish semantic/reporting view consumed by BI tools
    build_report = SQLExecuteQueryOperator(
        task_id="build_behavior_report",
        conn_id=WAREHOUSE_CONN_ID,
        sql="sql/reporting/customer_behavior_report.sql",
    )

    # DAG dependencies
    stage_sessions >> transforms >> build_report


customer_behavior_dag = customer_behavior()
