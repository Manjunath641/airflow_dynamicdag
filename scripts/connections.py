from airflow import settings
from airflow.models import Connection
import yaml
import os
import sys

#connection_file = sys.argv[1]
connection_config_path = f'projects/dataops/tobe_deployed/config/'

def create_connection(conn_data):
    connection = Connection(
        conn_id=conn_data['name'],
        conn_type=conn_data['conn_type'],
        host=conn_data.get('host', ''),
        login=conn_data.get('login', ''),
        password=conn_data.get('password', ''),
        schema=conn_data.get('schema', ''),
        port=conn_data.get('port', ''),
        extra=conn_data.get('extra','')
    )
    session = settings.Session()
    existing_connection = session.query(Connection).filter(Connection.conn_id == connection.conn_id).first()
    if existing_connection is None:
        session.add(connection)
        session.commit()
    session.close()

def create_connections_from_yaml(yaml_file):
    with open('/opt/airflow/scripts/projects/dataops/tobe_deployed/config/connection.yml', 'r') as stream:
        connections_data = yaml.safe_load(stream)

    for conn_data in connections_data:
        create_connection(conn_data)

if __name__ == '__main__':
    yaml_file = 'airflow_connections.yaml'
    create_connections_from_yaml(yaml_file)