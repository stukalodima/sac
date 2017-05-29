from ExcelReader import ExcelReader as ExcelReader
from AddressObject import AddressLine as AddressLine
from SSHConnector import SSHConnector as Connector

separators = (",", ".", "-", "(", ")", "/", "\\")
key_word = {"building", "street", "apt", "str", "app", "ap", "yl", "ul", "fl", "kv", "highway", "bul", "apartment",
            "avenue", "prospekt", "obl", "ave", "room", "region", "district", "pgt", "область", "rayon", "selo"}

words_table = []


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


def get_word_id_string(string, find_alpha, find_number, comment):
    words_array = get_array_words(string, " ")
    word_id_array = ""
    print("!!!" + comment + "!!!")
    for word in words_array:
        find = False
        if word in key_word or (len(word) == 1 and word.isalpha()) or len(word) == 0:
            continue
        if word.isalpha() and find_number:
            continue
        if word.isdigit() and find_alpha:
            continue
        count_a = 0
        count_n = 0
        for alp in word:
            if alp.isalpha():
                count_a += 1
            if alp.isdigit():
                count_n += 1
        if count_n > 0 and (count_a == 1 or count_a == 2):
            continue
        for word_row in words_table:
            if word == word_row[1]:
                print(word_row[1] + " true find")
                find = True
                word_id_array += (str(word_row[0]) + ",")
            if find:
                break
        if not find:
            for word_row in words_table:
                if distance(word, word_row[1]) <= (int(len(word) / 4) + 1):
                    print(word_row[1])
                    word_id_array += (str(word_row[0]) + ",")
    word_id_array = word_id_array[:len(word_id_array) - 1]
    return word_id_array


def distance(a, b):
    n, m = len(a), len(b)
    if n > m:
        a, b = b, a
        n, m = m, n

    current_row = range(n + 1)
    for i in range(1, m + 1):
        previous_row, current_row = current_row, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete, change = previous_row[j] + 1, current_row[j - 1] + 1, previous_row[j - 1]
            if a[j - 1] != b[i - 1]:
                change += 1
            current_row[j] = min(add, delete, change)

    return current_row[n]


def get_word_array(first_string, second_string):
    print("Получаем слова по первой колонке...")
    result_array = get_array_words(first_string.lower(), " ")
    if len(result_array) == 0:
        print("В первой колонке слов нет!!!")
        print("Получаем слова по второй колонке...")
        result_array = get_array_words(second_string.lower())
        print(second_string)
    else:
        print(first_string)
    return result_array


def get_adm_level(word_array, word_array_from_db, comment):
    result_array = dict()
    for word in word_array:
        true_search = word in word_array_from_db.keys()
        if true_search:
            row_list = word_array_from_db.get(word)
            for row in row_list:
                result_array.update({row[2]: (row[3], row[4])})
    if len(result_array) == 0:
        for word in word_array:
            mistake_count = int(len(word) / 4)
            if mistake_count == 0:
                continue
            for key in word_array_from_db.keys():
                if distance(key, word) <= mistake_count:
                    row_list = word_array_from_db.get(key)
                    for row in row_list:
                        result_array.update({row[2]: (row[3], row[4])})
    if len(result_array) != 0:
        print("Получили {0}:".format(comment))
        for key, value in result_array.items():
            print(str(key) + " | " + str(value))
    else:
        print("{0} не определили".format(comment))
    print("\n")
    return result_array


print("Читаем файл адресов")
reader = ExcelReader('inputFile.xlsx')
table = reader.table
address_table = []
for line in table:
    address = AddressLine(*line)
    address_table.append(address)
print("Закончили читать файл адресов")

data_base = Connector(True)
# print("Читаем таблицу областей...")
# data_base.get_adm_level1_word(True)
# print("Закончили читать таблицу областей...")
# print("Читаем таблицу районов...")
# data_base.get_adm_level2_word(True)
# print("Закончили читать таблицу районов...")
print("Читаем таблицу городов...")
data_base.get_settlement_word(True)
print("Закончили читать таблицу городов...")
# print("Читаем таблицу улиц...")
# data_base.get_streets_word(True)
# print("Закончили читать таблицу улиц...")
# print("Читаем таблицу привязок адм. единиц...")
# data_base.get_chains_adm_level(True)
# print("Закончили читать таблицу привязок адм. единиц...")

row_count = 0
start_row = 2390
end_row = 2410
print("Побежали по строкам")
print("\n")
for line in address_table:
    row_count += 1
    if row_count < start_row:
        continue
    if row_count > end_row:
        break
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("Обрабатываем строку №" + str(row_count))
    if len(line.city) == 0 and len(line.address) == 0:
        print("Обе колонки адреса пусты... искать нечего")
        continue
    print("Формируем слова для поиска адм. одиниц\n")
    words_array_city = get_word_array(line.city, line.address)
    # print("Формируем слова для поиска улиц\n")
    # words_array_street = get_word_array(line.address, line.city)
    # print("Пробуем определить область")
    # adm_level1 = get_adm_level(words_array_city, data_base.adm_level1, "Области")
    # print("Пробуем определить районы")
    # adm_level2 = get_adm_level(words_array_city, data_base.adm_level2, "Районы")
    print("Пробуем определить города")
    settlement = get_adm_level(words_array_city, data_base.settlement, "Города")
    # print("Пробуем определить улицу")
    # street = get_adm_level(words_array_street, data_base.streets, "Улицы")
    # print("Ищем пересечения по адм. единицам...")
    # for city in settlement.keys():
    #     summ_find = 0;
    #     row_chains = data_base.chains_adm_level.get(city)
    #     if row_chains is not None:
    #         if row_chains[]
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
