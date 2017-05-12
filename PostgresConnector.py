import psycopg2


# noinspection PyBroadException
class PostgresConnector:
    tablesResult = []

    def __init__(self, text_sql):
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
            }

            conn = psycopg2.connect(**params)
            curs = conn.cursor()

            print("database connected")

            curs.execute(text_sql)
            self.tablesResult = curs.fetchall()

        except:
            print("Connection Failed")

    def __del__(self):
        self.tablesResult = None
