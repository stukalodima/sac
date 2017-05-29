import psycopg2
from sshtunnel import SSHTunnelForwarder


# noinspection PyBroadException
class SSHConnector:
    use_ssh = False
    tableResult = []
    adm_level1 = dict()
    adm_level2 = dict()
    settlement = dict()
    streets = dict()
    chains_adm_level = dict()

    def __init__(self, use_ssh):
        self.use_ssh = use_ssh

    def get_adm_level1_word(self, use_ssh):
        self.use_ssh = use_ssh
        text = (" SELECT w.word AS word, w.id AS wordID, mp.id AS mapID, st_astext(mp.centroid) AS centroid, n.name\n"
                "FROM adm_level1 a\n"
                "  INNER JOIN links l ON a.id = l.object_id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 5\n"
                "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'ru'\n"
                "  INNER JOIN words w ON w.id = l.word_id")
        try:
            with SSHTunnelForwarder(
                    ('s2.sngtrans.com.ua', 22),
                    ssh_username="uit.dima",
                    ssh_password="D!m@$tuk@l0",
                    remote_bind_address=('sac', 5432)) as server:
                server.start()

                params = {
                    'database': 'sac_db',
                    'user': 'postgres',
                    'password': 'postgres',
                    'host': '127.0.0.1',
                    'port': server.local_bind_port
                }

                connection = psycopg2.connect(**params)
                cursor = connection.cursor()

                cursor.execute(text)
                row = cursor.fetchone()
                while row is not None:
                    if row[0] in self.adm_level1.keys():
                        el = self.adm_level1.get(row[0])
                        el.append(row)
                        self.adm_level1.update({row[0]: el})
                    else:
                        self.adm_level1.update({row[0]: [row]})
                    row = cursor.fetchone()

                connection.close()
        except:
            print("Connection Failed: " + text)

    def get_adm_level2_word(self, use_ssh):
        self.use_ssh = use_ssh
        text = ("SELECT w.word AS word, w.id AS wordID, mp.id AS mapID, st_astext(mp.centroid) AS centroid, n.name\n"
                "FROM adm_level2 a\n"
                "  INNER JOIN links l ON a.id = l.object_id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 4\n"
                "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'ru'\n"
                "  INNER JOIN words w ON w.id = l.word_id")
        try:
            with SSHTunnelForwarder(
                    ('s2.sngtrans.com.ua', 22),
                    ssh_username="uit.dima",
                    ssh_password="D!m@$tuk@l0",
                    remote_bind_address=('sac', 5432)) as server:
                server.start()

                params = {
                    'database': 'sac_db',
                    'user': 'postgres',
                    'password': 'postgres',
                    'host': '127.0.0.1',
                    'port': server.local_bind_port
                }

                connection = psycopg2.connect(**params)
                cursor = connection.cursor()

                cursor.execute(text)
                row = cursor.fetchone()
                while row is not None:
                    if row[0] in self.adm_level2.keys():
                        el = self.adm_level2.get(row[0])
                        el.append(row)
                        self.adm_level2.update({row[0]: el})
                    else:
                        self.adm_level2.update({row[0]: [row]})
                    row = cursor.fetchone()

                connection.close()
        except:
            print("Connection Failed: " + text)

    def get_settlement_word(self, use_ssh):
        self.use_ssh = use_ssh
        text = (" SELECT w.word AS word, w.id AS wordID, mp.id AS mapID, st_astext(mp.centroid) AS centroid, n.name,a.id\n"
                "FROM settlements a\n"
                "  INNER JOIN links l ON a.id = l.object_id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 3\n"
                "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'ru'\n"
                "  INNER JOIN words w ON w.id = l.word_id")
        try:
            with SSHTunnelForwarder(
                    ('s2.sngtrans.com.ua', 22),
                    ssh_username="uit.dima",
                    ssh_password="D!m@$tuk@l0",
                    remote_bind_address=('sac', 5432)) as server:
                server.start()

                params = {
                    'database': 'sac_db',
                    'user': 'postgres',
                    'password': 'postgres',
                    'host': '127.0.0.1',
                    'port': server.local_bind_port
                }

                connection = psycopg2.connect(**params)
                cursor = connection.cursor()

                cursor.execute(text)
                row = cursor.fetchone()
                while row is not None:
                    if row[0] in self.settlement.keys():
                        el = self.settlement.get(row[0])
                        el.append(row)
                        self.settlement.update({row[0]: el})
                    else:
                        self.settlement.update({row[0]: [row]})
                    row = cursor.fetchone()

                connection.close()
        except:
            print("Connection Failed: " + text)

    def get_streets_word(self, use_ssh):
        self.use_ssh = use_ssh
        text = ("SELECT w.word AS word, w.id AS wordID, mp.id AS mapID, st_astext(mp.centroid) AS centroid, n.name AS name  \n"
                "FROM map_objects mp\n"
                "INNER JOIN links l ON mp.id = l.object_id\n"
                "INNER JOIN words w ON w.id = l.word_id\n"
                "INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'ru'\n"
                "WHERE mp.category_n = 2")
        try:
            with SSHTunnelForwarder(
                    ('s2.sngtrans.com.ua', 22),
                    ssh_username="uit.dima",
                    ssh_password="D!m@$tuk@l0",
                    remote_bind_address=('sac', 5432)) as server:
                server.start()

                params = {
                    'database': 'sac_db',
                    'user': 'postgres',
                    'password': 'postgres',
                    'host': '127.0.0.1',
                    'port': server.local_bind_port
                }

                connection = psycopg2.connect(**params)
                cursor = connection.cursor()

                cursor.execute(text)
                row = cursor.fetchone()
                while row is not None:
                    if row[0] in self.streets.keys():
                        el = self.streets.get(row[0])
                        el.append(row)
                        self.streets.update({row[0]: el})
                    else:
                        self.streets.update({row[0]: [row]})
                    row = cursor.fetchone()

                connection.close()
        except:
            print("Connection Failed: " + text)

    def get_chains_adm_level(self, use_ssh):
        self.use_ssh = use_ssh
        text = ("SELECT s.id, s.level1_id, s.level2_id, s.class_n\n"
                "FROM settlements s\n")
        try:
            with SSHTunnelForwarder(
                    ('s2.sngtrans.com.ua', 22),
                    ssh_username="uit.dima",
                    ssh_password="D!m@$tuk@l0",
                    remote_bind_address=('sac', 5432)) as server:
                server.start()

                params = {
                    'database': 'sac_db',
                    'user': 'postgres',
                    'password': 'postgres',
                    'host': '127.0.0.1',
                    'port': server.local_bind_port
                }

                connection = psycopg2.connect(**params)
                cursor = connection.cursor()

                cursor.execute(text)
                row = cursor.fetchone()
                while row is not None:
                    self.chains_adm_level.update({row[0]: row})
                    row = cursor.fetchone()

                connection.close()
        except:
            print("Connection Failed: " + text)

    def __del__(self):
        self.tableResult = None
        self.adm_level1 = None
