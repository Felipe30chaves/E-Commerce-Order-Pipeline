from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# ====== AJUSTE DE CAMINHOS (Windows + Git-Bash) ======
PROJECT_ROOT = '/c/Users/felip/Desktop/End-to-End E-Commerce Data Pipeline with Snowflake dbt & Airflow  Delayed Orders Alterting'
VENV_BASH    = f'{PROJECT_ROOT}/airflow_venv_39/Scripts/activate'   # <- se seu venv ficar em outro local, mude aqui
DBT_DIR      = f'{PROJECT_ROOT}/dbt_ecommerce'
CHECK_SCRIPT = f'{PROJECT_ROOT}/airflow_project/dags/utils/check_delayed_orders.py'
# =====================================================

default_args = {
    'owner': 'airflow',
    'email': ['imamzafar100@gmail.com'],  # troque se quiser
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='order_monitoring_dag16',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command=f'source "{VENV_BASH}" && cd "{DBT_DIR}" && dbt run'
    )

    check_orders = BashOperator(
        task_id='check_delayed_orders',
        bash_command=f'source "{VENV_BASH}" && python "{CHECK_SCRIPT}"'
    )

    dbt_run >> check_orders
