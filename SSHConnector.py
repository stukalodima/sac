import psycopg2
from sshtunnel import SSHTunnelForwarder


# noinspection PyBroadException
class SSHConnector:
    tableResult = []

    def __init__(self, text_sql):
        try:
            with SSHTunnelForwarder(
                    ('s2.sngtrans.com.ua', 22),
                    ssh_username="uit.dima",
                    ssh_password="D!m@$tuk@l0",
                    remote_bind_address=('sac', 5432)) as server:
                server.start()
                print("server connected")

                params = {
                    'database': 'sac_db',
                    'user': 'postgres',
                    'password': 'postgres',
                    'host': '127.0.0.1',
                    'port': server.local_bind_port
                }

                connection = psycopg2.connect(**params)
                cursor = connection.cursor()
                print("database connected")

                cursor.execute(text_sql)
                self.tableResult = cursor.fetchall()

                connection.close()
        except:
            print("Connection Failed")

    def __del__(self):
        self.tableResult = None
