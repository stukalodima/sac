from MapParser import MapParser as parser

class MapObjectUtils:
    @staticmethod
    def get_adm_level(word_array, word_array_from_db, comment):
        result_array = dict()
        print("Пробуем найти ссылки слов на " + comment + ". Точный поиск")
        if len(word_array) > 1:
            print("у нас больше чем одно слово попробуем найти пересечение ссылок этих слов на объекты")
            word_id = -1
            object_array_by_word_id = dict()
            all_object_array = dict()
            for word in word_array:
                word_id += 1
                true_search = word in word_array_from_db.keys()
                set_id = set()
                if true_search:
                    row_list = word_array_from_db.get(word)
                    for row in row_list:
                        all_object_array.update({row[2]: (row[3], row[4], row[5], row[6], row[2], row[7])})
                        set_id.add(row[2])
                object_array_by_word_id.update({word_id: set_id})
            set_array = []
            result_set = object_array_by_word_id.get(word_id)
            while word_id >= 1:
                key = word_id - 1
                result_set = result_set.intersection(object_array_by_word_id.get(key))
                if len(result_set) > 0:
                    set_array.append(result_set)
                word_id -= 1
                result_set = object_array_by_word_id.get(word_id)
            if len(set_array):
                for set_el in set_array:
                    for set_value in set_el:
                        print("Нашли пересечения " + str(all_object_array.get(set_value)))
                        result_array.update({set_value: all_object_array.get(set_value)})
            else:
                print("HELL не нашли пересечения")
                result_array = all_object_array
        else:
            for word in word_array:
                true_search = word in word_array_from_db.keys()
                if true_search:
                    row_list = word_array_from_db.get(word)
                    for row in row_list:
                        result_array.update({row[2]: (row[3], row[4], row[5], row[6], row[2], row[7])})
        if len(result_array) == 0:
            print("{0} не определили".format(comment))
        print(result_array)
        return result_array

    @staticmethod
    def get_adm_level_similar(table_in, comment):
        table_out = dict()
        for row in table_in:
            table_out.update({row[0]: (row[1], row[3], row[2], row[4], row[0])})
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
                        result_array.update({row[2]: (row[3], row[4], row[5], row[6], row[2])})
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

    @staticmethod
    def get_street_type(new_address_str, type_of_adm_level):
        key_word = set()
        separators = set()
        separators.add(" ")
        word_array = parser.get_word_array(new_address_str, key_word, separators, False)
        result_type = None
        for word in word_array:
            for type_key, type_value in type_of_adm_level.items():
                if word in type_value:
                    result_type = type_key
        return result_type

    @staticmethod
    def filter_street_by_type(streets, street_type):
        filtered_streets = dict()
        for street_key, street_value in streets.items():
            if street_value[3] == street_type:
                filtered_streets.update({street_key: street_value})

        return filtered_streets
