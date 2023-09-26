def get_date(**kwargs) -> str:
    ti = kwargs['ti']
    value1 = str(datetime.now())
    ti.xcom_push(key='Time1', value=value1)