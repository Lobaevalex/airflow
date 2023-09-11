from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag = DAG(
    dag_id='dwh_update',
    description='Template',
    schedule_interval=None,
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    params={},
    default_args={},
    on_failure_callback=msteams_notify_on_failure # Notification on Microsoft Teams -> additional functionality
)

# Tables variable configuration with table types
tables_configuration = {
  "dimensions": {
    "table_dimension_1": {
      "source_table": "source_dimension_table_1"
    },
    ...
  },
  "facts": {
    "table_fact_1": {
      "source_table": "source_fact_table_1"
    },
    ...
  },
  "datamarts": {
    "table_datamart_1": { 
      "source_table": "source_datamart_table_1"
    },
    ...
  },
  "others": {
    "table_other_1": {
      "source_table": "source_other_table_1"
    },
    ...
  }
}

# Environments variable with connection names
connections = [
   {"connection": "Connection_1",
    "environment": "Environment_1",
    "environment_alias": "Env_1"},
   {"connection": "Connection_2",
    "environment": "Environment_2",
    "environment_alias": "Env_2"},
    ...
]

# LVL1. Main group
# - required for reloading full DAG
@task_group(group_id=f"reload", dag=dag)
def reload():
    group_task_types = {}
    group_regions = {}

    # LVL2. Groups by table type
    # - required for dependency between table types: [dimensions load before facts, ...]
    for table_type in tables_configuration:
        @task_group(group_id=table_type, dag=dag)
        def reload_tables_by_types():

            # LVL3. Groups by tables
            # - required for reloading exact tables 
            for target_table in tables_configuration[table_type]:
                @task_group(group_id=target_table, dag=dag)
                def reload_tables(target_table, table_type):

                    # LVL4. Task by regions
                    # - required if there are multiple environments 
                    for connection_name in connections:
                        @task(task_id=f"{target_table}_{connection_name['region_alias']}", dag=dag, provide_context=True, trigger_rule=TriggerRule.ALL_DONE)
                        def reload_region(target_table, table_type, connection):
                            # Preparation
                            # - preparing variables for future use, initialising class for using methods, ...
                            source_table = tables_configuration[table_type][target_table]['source_table']
                            ...

                            # 0. Check schedule of the table
                            def check_schedule():
                                # - Some tasks need to be run in different schedule rather than DAG schedule -> additional functionality dependent on scheduled time
                                ...

                            # 1. Getting data from source database
                            def extract(source_table):
                                # - Extracting data from source database in file (.json/.csv/dataframe)
                                print('\n# 1️⃣ Getting data from source database')
                                ...
                                return files_list

                            # 2. Preparing Stage
                            def prepare_stag(files_list):
                                # - Preparing stage/datalake schema in datawarehouse for incoming data
                                print('\n# 2️⃣ Preparing Snowflake Stage')
                                ...
                                return data

                            # 3. Processing/validating data
                            def validate(data):
                                # - Validating incoming data in stage/datalake schema
                                print('\n# 3️⃣ Processing/validating data')
                                ...
                                return data_validated

                            # 4. Saving results
                            def load(data_validated):
                                # - Loading results into final datawarehouse table
                                ...

                            load(
                                validate(
                                    prepare_stag(
                                        extract(
                                            check_schedule()
                                            )
                                    )
                                )
                            )
                           
                        # LVL4. Adding environments to dictionary for creating dependency
                        group_regions[connection_name['region_alias']] = reload_region(target_table=target_table, table_type=table_type, connection=connection_name)

                    # LVL4. Task dependency between environments
                    group_regions['Environment_1'] >> group_regions['Environment_2'] >> group_regions['Environment_3']

                # LVL3. Task group execution of tables
                reload_tables(target_table=target_table, table_type=table_type)

        # LVL2. Adding table types to dictionary for creating dependency
        group_task_types[table_type] = reload_tables_by_types()

    # LVL2. Task group dependency between table_types
    group_task_types['dimensions'] >> group_task_types['facts'] >> group_task_types['others'] >> group_task_types['datamarts']

# LVL1
reload()