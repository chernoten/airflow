import ftplib
import io
import os
import re
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor

last_cdr = 'base_file'
server_timezone = pytz.timezone("Europe/Moscow")
def get_cdr():
    ftp = ftplib.FTP('10.246.47.2')
    ftp.login('EXPERT', 'EXPERTS')
    ftp.cwd('VIDAST')
    ls = ftp.retrlines('LIST ' + 'CF*', get_cdr_name)
    #print(f'last_cdr is {last_cdr}')
    server_time =  datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    local_cdr = open("dags/radio-sorm/data/dvptus/in/{}_".format(server_time) + last_cdr, 'wb')
    ftp.retrbinary('RETR ' + last_cdr, local_cdr.write)
    ftp.quit()
    return local_cdr.name

def get_cdr_name(info):
    ls_out = info.split()
    if re.match(r'CF\d{4}.D00', ls_out[-1]):
        try:
            td = datetime.now() - datetime.strptime(" ".join(ls_out[:2]), '%H.%M:%S %d.%m.%Y')
            if td <= timedelta(minutes=435):
                global last_cdr
                last_cdr = ls_out[-1]
        except ValueError:
            print(f'Date parse error {" ".join(ls_out[:2])}')
    return None

dag = DAG('CDR_Parser_DVTUS', description='get and parse CDR',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2020, 1, 1), catchup=False)


get_operator = PythonOperator(task_id='get_CDR', python_callable=get_cdr, dag=dag)

parser_operator =  BashOperator(
        task_id= 'parser_task',
        bash_command='python3 ~/dags/radio-sorm/sorm/main.py --ptus=DV ~/{{ task_instance.xcom_pull(task_ids="get_CDR") }}',
        dag=dag
)

#file_name = FileSensor(
#    task_id='file_sensor_task_id',
#    filepath='{{ task_instance.xcom_pull(task_ids="get_CDR") }}',
#    fs_conn_id="fs_default" # default one, commented because not needed
#    poke_interval= 20,
#    dag=dag
#)

get_operator >> parser_operator


