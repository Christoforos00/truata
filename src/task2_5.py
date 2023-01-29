from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

#empty instead of dummy
def task2_5():
    with DAG("my_dag", start_date=datetime.now()) as dag:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = EmptyOperator(task_id="task_2")
        task_3 = EmptyOperator(task_id="task_3")
        task_4 = EmptyOperator(task_id="task_4")
        task_5 = EmptyOperator(task_id="task_5")
        task_6 = EmptyOperator(task_id="task_6")

        task_1 >> [task_2, task_3]
        task_2 >> [task_4, task_5, task_6]
        task_3 >> [task_4, task_5, task_6]


if __name__ == "__main__":
    task2_5()
