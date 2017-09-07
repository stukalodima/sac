class DictUtils:
    @staticmethod
    def filter_dict(filtered_dict, owner, comment):
        if len(filtered_dict) > 0:
            list_keys_for_delete = []
            for key, value in filtered_dict.items():
                if value[2] not in owner.keys():
                    list_keys_for_delete.append(key)
            if len(list_keys_for_delete) > 0:
                for key in list_keys_for_delete:
                    filtered_dict.pop(key)
        print(comment + str(filtered_dict))
        return filtered_dict

    @staticmethod
    def filter_street_dict(filtered_dict, type_el, comment):
        if len(filtered_dict) > 0:
            list_keys_for_delete = []
            for key, value in filtered_dict.items():
                if value[4] != type_el:
                    list_keys_for_delete.append(key)
            if len(list_keys_for_delete) > 0:
                for key in list_keys_for_delete:
                    filtered_dict.pop(key)
        if len(filtered_dict) > 1:
            max_similar = 0.0
            key_max_similar = 0
            keys = list()
            for key, value in filtered_dict.items():
                if value[5] > max_similar:
                    key_max_similar = key
                    max_similar = value[5]
                keys.append(key)
            for key in keys:
                if key != key_max_similar:
                    filtered_dict.pop(key)
        print(comment + str(filtered_dict))
        return filtered_dict

    @staticmethod
    def get_key_isnull_from_dict(dict_for_search):
        is_key = 'ISNULL'
        for key in dict_for_search.keys():
            is_key = " = " + str(key)
        return is_key

    @staticmethod
    def get_key_from_dict(dict_for_search):
        is_key = 'null'
        for key in dict_for_search.keys():
            is_key = str(key)
        return is_key
