def save_date(ti) -> None:
    dt = ti.xcom_pull(task_ids=['xcom_push_method'])
    if not dt:
        raise ValueError('No value currently stored in XComs.')