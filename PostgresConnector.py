import psycopg2


# noinspection PyBroadException
class PostgresConnector:
    use_ssh = False
    tableResult = []
    adm_level1 = dict()
    adm_level2 = dict()
    settlement = dict()
    settlement_by_index = dict()
    streets = dict()
    streets_by_index = dict()
    indexes = dict()

    def __init__(self, use_ssh):
        self.use_ssh = use_ssh

    def get_adm_level1_word(self, use_ssh):
        self.use_ssh = use_ssh
        text = (" SELECT w.word AS word, w.id AS wordID, mp.id AS mapID, st_astext(mp.centroid) AS centroid, n.name,\n"
                "a.adm_center_id, 0\n"
                "FROM adm_level1 a\n"
                "  INNER JOIN links l ON a.id = l.object_id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 5\n"
                "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'uk'\n"
                "  INNER JOIN words w ON w.id = l.word_id")
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
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
        text = ("SELECT w.word AS word, w.id AS wordID, mp.id AS mapID, st_astext(mp.centroid) AS centroid, n.name,\n"
                "a.level1_id,0\n"
                "FROM adm_level2 a\n"
                "  INNER JOIN links l ON a.id = l.object_id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 4\n"
                "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'uk'\n"
                "  INNER JOIN words w ON w.id = l.word_id")
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
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
        text = (" SELECT w.word AS word, w.id AS wordID, mp.id AS mapID, st_astext(mp.centroid) AS centroid, n.name,\n"
                "a.level2_id, a.level1_id, a.class_n\n"
                "FROM settlements a\n"
                "  INNER JOIN links l ON a.id = l.object_id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 3\n"
                "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'uk'\n"
                "  INNER JOIN words w ON w.id = l.word_id")
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
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
        text = ("SELECT w.word word, w.id wordID, mp.id mapID, st_astext(mp.centroid) centroid,\n"
                "n.name, s.settlement_id, t.name\n"
                "FROM streets AS s\n"
                "  INNER JOIN links l ON s.id = l.object_id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 2\n"
                "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'uk'\n"
                "  INNER JOIN types t ON t.id = s.type_id AND  t.lang ='uk'\n"
                "  INNER JOIN words w ON w.id = l.word_id")
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
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

    def select_from_db(self, text, use_ssh):
        self.use_ssh = use_ssh
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
            }

            connection = psycopg2.connect(**params)
            cursor = connection.cursor()

            cursor.execute(text)
            self.tableResult = cursor.fetchall()

            connection.close()
        except:
            print("Connection Failed: " + text)

    def insert_into_db(self, text, use_ssh):
        self.use_ssh = use_ssh
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
            }

            connection = psycopg2.connect(**params)
            cursor = connection.cursor()

            cursor.execute(text)
            connection.commit()
            connection.close()
        except:
            print("Connection Failed: " + text)

    def get_indexes_id(self, use_ssh):
        self.use_ssh = use_ssh
        text = ("SELECT *\n"
                "FROM indexes")
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
            }

            connection = psycopg2.connect(**params)
            cursor = connection.cursor()

            cursor.execute(text)
            row = cursor.fetchone()
            while row is not None:
                self.indexes.update({row[1]: row[0]})
                row = cursor.fetchone()

            connection.close()

        except:
            print("Connection Failed: " + text)

    def get_settlement_by_index(self, use_ssh):
        self.use_ssh = use_ssh
        text = ("SELECT DISTINCT ic.index, ic.settlement\n"
                "FROM index_chains ic")
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
            }

            connection = psycopg2.connect(**params)
            cursor = connection.cursor()

            cursor.execute(text)
            row = cursor.fetchone()
            while row is not None:
                self.settlement_by_index.update({row[0]: row[1]})
                row = cursor.fetchone()

            connection.close()

        except:
            print("Connection Failed: " + text)

    def get_street_by_index(self, use_ssh):
        self.use_ssh = use_ssh
        text = ("SELECT DISTINCT ic.index, ic.street\n"
                "FROM index_chains ic")
        try:
            params = {
                'database': 'sac_db',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
            }

            connection = psycopg2.connect(**params)
            cursor = connection.cursor()

            cursor.execute(text)
            row = cursor.fetchone()
            while row is not None:
                if row[0] in self.streets_by_index.keys():
                    el = self.streets_by_index.get(row[0])
                    el.append(row[1])
                    self.streets_by_index.update({row[0]: el})
                else:
                    self.streets_by_index.update({row[0]: [row[1]]})
                row = cursor.fetchone()

            connection.close()

        except:
            print("Connection Failed: " + text)

    def __del__(self):
        self.tableResult = None
        self.adm_level1 = None
        self.adm_level2 = None
        self.settlement = None
        self.streets = None
        self.settlement_by_index = None
        self.streets_by_index = None
        self.indexes = None

    @staticmethod
    def get_text_query_street(words_array):
        text = ("SELECT * \n"
                "FROM ( \n")
        word_count = 1
        for word in words_array:
            if word_count != 1:
                text += "union \n"
            word_count += 1
            text += (("SELECT mp.id mapID, \n"
                      "st_astext(mp.centroid) centroid,\n"
                      "s.settlement_id, \n"
                      "n.name, \n"
                      "t.name, \n"
                      "similarity(word, '{0}') as similar\n"
                      "FROM streets AS s\n"
                      "  INNER JOIN links l ON s.id = l.object_id\n"
                      "  INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 2\n"
                      "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'uk'\n"
                      "  INNER JOIN types t ON t.id = s.type_id AND  t.lang ='uk'\n"
                      "  INNER JOIN words w ON w.id = l.word_id\n"
                      "where w.word % '{0}'\n").format(word))
        text += (") as t\n"
                 "  where t.similar > 0.1\n"
                 "ORDER BY t.similar")
        return text

    @staticmethod
    def get_text_query_setlements(words_array):
        text = ("SELECT * \n"
                "FROM ( \n")
        word_count = 1
        for word in words_array:
            if word_count != 1:
                text += "union \n"
            word_count += 1
            text += ("SELECT mp.id                  AS mapID,\n"
                     "    st_astext(mp.centroid) AS centroid,\n"
                     "    n.name,\n"
                     "    a.level2_id,\n"
                     "    a.level1_id,\n"
                     "    a.class_n,\n"
                     "    similarity(word, '{0}') AS similar\n"
                     "  FROM settlements a\n"
                     "    INNER JOIN links l ON a.id = l.object_id\n"
                     "    INNER JOIN map_objects mp ON mp.id = l.object_id AND mp.category_n = 3\n"
                     "    INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'ru'\n"
                     "    INNER JOIN words w ON w.id = l.word_id\n"
                     "  WHERE w.word % '{0}'\n".format(word))
        text += (") as t\n"
                 "  where t.similar > 0.1\n"
                 "ORDER BY t.similar DESC")
        return text

    @staticmethod
    def get_index_str(index, key_word_index):
        index_str_local = index.strip().lower()
        result_str = ""
        if len(index_str_local) > 5:
            for separator in key_word_index:
                index_str_local = index_str_local.replace(separator, "")
            index_str_local = index_str_local.strip()

        if len(index_str_local) == 5 and index_str_local.isdigit():
            result_str = index_str_local

        return result_str
