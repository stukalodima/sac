class MapObjectUtils:
    @staticmethod
    def get_adm_level(word_array, word_array_from_db, comment):
        result_array = dict()
        for word in word_array:
            true_search = word in word_array_from_db.keys()
            if true_search:
                row_list = word_array_from_db.get(word)
                for row in row_list:
                    result_array.update({row[2]: (row[3], row[4], row[5], row[6])})
        if len(result_array) == 0:
            print("{0} не определили".format(comment))
        return result_array

    @staticmethod
    def get_adm_level_similar(table_in, comment):
        table_out = dict()
        for row in table_in:
            table_out.update({row[0]: row})
        print("Получено {0} нечеткий поиск: {1}".format(comment, len(table_out)))
        print(table_out)
        return table_out

    @staticmethod
    def lev(s, t):
        if s == t:
            return 0
        elif len(s) == 0:
            return len(t)
        elif len(t) == 0:
            return len(s)
        v0 = [None] * (len(t) + 1)
        v1 = [None] * (len(t) + 1)
        for i in range(len(v0)):
            v0[i] = i
        for i in range(len(s)):
            v1[0] = i + 1
            for j in range(len(t)):
                cost = 0 if s[i] == t[j] else 1
                v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
            for j in range(len(v0)):
                v0[j] = v1[j]
        return v1[len(t)]

    @staticmethod
    def get_adm_level_lev(word_array, word_array_from_db, comment):
        result_array = dict()
        for word in word_array:
            distance = int(len(word) / 4) + 1
            for word_from_db in word_array_from_db.keys():
                if MapObjectUtils.lev(word, word_from_db) <= distance:
                    row_list = word_array_from_db.get(word_from_db)
                    for row in row_list:
                        result_array.update({row[2]: (row[3], row[4], row[5], row[6])})
        if len(result_array) == 0:
            print("{0} не определили".format(comment))
        return result_array

    @staticmethod
    def get_street_from_index_array(street_id_array, street):
        tmp_street = None
        if len(street) == 0 or len(street_id_array) == 0:
            return tmp_street
        for el_array in street_id_array:
            if el_array in street.keys():
                print(el_array)
                tmp_street = street.get(el_array)
        return tmp_street
