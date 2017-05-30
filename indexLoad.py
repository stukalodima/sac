from ExcelReader import ExcelReader
from SSHConnector import SSHConnector as Connector

separators = (",", ".", "-", "(", ")", "/", "\\")
key_word = dict()


def get_array_words(string, this_separator):
    words_array = string.split(this_separator)
    result = []
    for word in words_array:
        is_with_out_separator = True
        check_word = word.strip()
        for sep in separators:
            if check_word.find(sep) != -1:
                is_with_out_separator = False
                tpm_result = get_array_words(check_word, sep)
                for tmp_word in tpm_result:
                    if tmp_word != "" and tmp_word not in result and tmp_word not in key_word:
                        result.append(tmp_word)
        if is_with_out_separator and check_word not in result and check_word not in key_word:
            result.append(check_word)
    return result


def get_word_array(first_string, second_string):
    print("Получаем слова по первой колонке...")
    result_array = get_array_words(first_string.lower(), " ")
    if len(result_array) == 0:
        print("В первой колонке слов нет!!!")
        print("Получаем слова по второй колонке...")
        result_array = get_array_words(second_string.lower(), " ")
        print(second_string)
    else:
        print(first_string)
    return result_array


def filter_dict(filtered_dict, owner, comment):
    if len(filtered_dict) > 1:
        list_keys_for_delete = []
        for key, value in filtered_dict.items():
            if value[2] not in owner.keys():
                list_keys_for_delete.append(key)
        if len(list_keys_for_delete) > 0:
            for key in list_keys_for_delete:
                filtered_dict.pop(key)
    print(comment + str(filtered_dict))
    return filtered_dict


def get_adm_level(word_array, word_array_from_db, comment):
    result_array = dict()
    for word in word_array:
        true_search = word in word_array_from_db.keys()
        if true_search:
            row_list = word_array_from_db.get(word)
            for row in row_list:
                result_array.update({row[2]: (row[3], row[4], row[5])})
    if len(result_array) != 0:
        print("Получили {0}:".format(comment))
        for key, value in result_array.items():
            print(str(key) + " | " + str(value))
    else:
        print("{0} не определили".format(comment))
    print("\n")
    return result_array


def get_key_from_dict(dict_for_search):
    is_key = 'null'
    for key in dict_for_search.keys():
        is_key = key
    return is_key

index_file = ExcelReader("indexes1.xlsx")
index_table = index_file.table

data_base = Connector(True)
print("Читаем таблицу областей...")
data_base.get_adm_level1_word(True)
print("Закончили читать таблицу областей...")
print("Читаем таблицу районов...")
data_base.get_adm_level2_word(True)
print("Закончили читать таблицу районов...")
print("Читаем таблицу городов...")
data_base.get_settlement_word(True)
print("Закончили читать таблицу городов...")
print("Читаем таблицу улиц...")
data_base.get_streets_word(True)
print("Закончили читать таблицу улиц...")

start_row = 25790
row_count = 0
for line in index_table:
    row_count += 1
    if row_count < start_row:
        continue
    if row_count > start_row + 20:
        break
    print(line)
    print("\n")
    words_array_obl = get_word_array(line[0], "")
    obl = get_adm_level(words_array_obl, data_base.adm_level1, "Области")
    words_array_rn = get_word_array(line[1], "")
    rn = get_adm_level(words_array_rn, data_base.adm_level2, "районы")
    words_array_city = get_word_array(line[2], "")
    city = get_adm_level(words_array_city, data_base.settlement, "Города")
    words_array_street = get_word_array(line[5], "")
    street = get_adm_level(words_array_street, data_base.streets, "Улицы")
    index = str(line[3])
    while len(index) < 5:
        index = "0" + index
    print(index)
    rn = filter_dict(rn, obl, "r-n: ")
    city = filter_dict(city, rn, "city: ")
    street = filter_dict(street, city, "street: ")
    if len(street) == 0:
        continue

    data_base.select_from_db("SELECT * FROM indexes WHERE index = '%(index)s'" % {"index": index}, True)
    if len(data_base.tableResult) == 0:
        data_base.insert_into_db("INSERT INTO indexes (index) VALUES ('%(index)s')" % {"index": index}, True)
        data_base.select_from_db("SELECT * FROM indexes WHERE index = '%(index)s'" % {"index": index}, True)

    if len(data_base.tableResult) == 0:
        continue
    index_id = data_base.tableResult[0][0]

    parameters = {"adm_level1": get_key_from_dict(obl), "adm_level2": get_key_from_dict(rn),
                  "settlement": get_key_from_dict(city), "street": get_key_from_dict(street), "index": index_id}
    data_base.insert_into_db("INSERT INTO index_chains (adm_level1, adm_level2, settlement, street, index) \n"
                             "VALUES (%(adm_level1)s, %(adm_level2)s, %(settlement)s, %(street)s, %(index)s)\n"
                             "" % parameters, True)
