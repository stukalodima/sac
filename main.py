from ExcelReader import ExcelReader as ExcelReader
from AddressObject import AddressLine as AddressLine
from SSHConnector import SSHConnector as Connector

separators = (",", ".", "-", "(", ")", "/", "\\")
key_word = {"building", "street", "apt", "str", "app", "ap", "yl", "ul", "fl", "kv", "highway", "bul", "apartment",
            "avenue", "prospekt", "obl", "ave", "room", "region", "district", "pgt"}

query_city_id = ("SELECT mp.id AS city\n"
                 "FROM words AS w\n"
                 "  INNER JOIN links l ON l.word_id = w.id\n"
                 "  INNER JOIN map_objects mp ON mp.id = l.object_id\n"
                 "WHERE w.id IN ({0})\n"
                 "      AND mp.category_n = 3")

query_street_id = ("SELECT mp.id AS street_id\n"
                   "FROM words AS w\n"
                   "  INNER JOIN links l ON l.word_id = w.id\n"
                   "  INNER JOIN map_objects mp ON mp.id = l.object_id\n"
                   "WHERE w.id IN ({0})\n"
                   "      AND mp.category_n = 2")

query_address_id = ("SELECT a.id AS address_id, mp.id AS map_id, ST_AsText(mp.centroid) AS point, n.name\n"
                    "FROM address a\n"
                    "  INNER JOIN map_objects mp ON mp.id = a.id\n"
                    "  INNER JOIN names n ON n.name_id = mp.name_id AND n.lang = 'ru'\n"
                    "WHERE a.settlement_id IN ({0})\n"
                    "     AND a.street_id IN ({1})")


def get_array_words(string, this_separator):
    words_array = string.split(this_separator)
    result = []
    for word in words_array:
        is_with_out_separator = True
        for sep in separators:
            if word.find(sep) != -1:
                is_with_out_separator = False
                tpm_result = get_array_words(word, sep)
                for tmp_word in tpm_result:
                    if tmp_word.strip() != "" and tmp_word.strip() not in result:
                        result.append(tmp_word.strip())
        if is_with_out_separator and word.lower() not in result:
            result.append(word.lower())
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


def get_word_id_aray(string, find_alpha, find_number, comment):
    words_array = get_array_words(string, " ")
    word_id_array = []
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
                word_id_array.append(word_row[0])
            if find:
                break
        if not find:
            for word_row in words_table:
                if distance(word, word_row[1]) <= (int(len(word) / 4) + 1):
                    print(word_row[1])
                    word_id_array.append(word_row[0])
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


def get_wordID_linkID_array(array_wordID_linkID, array_wordID):
    for wordID in array_wordID:
        for linkID_row in links_table:
            if wordID == linkID_row[0]:
                array_wordID_linkID.append()


reader = ExcelReader('inputFile.xlsx')
table = reader.table
address_table = []
for line in table:
    address = AddressLine(*line)
    address_table.append(address)

databaseConnector = Connector("SELECT * FROM words")
words_table = databaseConnector.tableResult
del databaseConnector
databaseConnector = Connector("SELECT * \n"
                              "FROM links l")
links_table = databaseConnector.tableResult
del databaseConnector
exit(0)
row_count = 0
start_row = 34
end_row = 34
for line in address_table:
    row_count += 1
    if row_count < start_row:
        continue
    if row_count > end_row:
        break
    print(line)

    # /////////////////////////////////
    # array_wordID = get_word_id_aray()
    # array_wordID_linkID = []
    # get_wordID_linkID_array(array_wordID_linkID, array_wordID)
    # /////////////////////////////////

    # f = open('/home/dima/result.txt', 'a')
    # f.write(line.city + " " + line.address)
    # do with city
    word_id_array = get_word_id_string(line.city, True, False, "Search city")
    print("city word id:" + word_id_array)
    is_address_parse = False
    if len(word_id_array) == 0:
        word_id_array = get_word_id_string(line.address, True, False, "Search city by address column")
        print("city word id:" + word_id_array)
        is_address_parse = True
    if word_id_array == "":
        print("CITY NOT FIND")
        continue
    city_obj_table = Connector(query_city_id.format(word_id_array)).tableResult
    city_obj_id = ""
    for el in city_obj_table:
        city_obj_id += (str(el[0]) + ",")
    city_obj_id = city_obj_id[:len(city_obj_id) - 1]
    print("city object ID:" + city_obj_id)

    # do with street
    if not is_address_parse:
        word_id_array = get_word_id_string(line.address, True, False, "Search street")
    else:
        print("Street was parse with city")
    print("street word id:" + word_id_array)
    if word_id_array == "":
        print("STREET NOT FIND")
        continue
    street_obj_table = Connector(query_street_id.format(word_id_array)).tableResult
    street_obj_id = ""
    for el in street_obj_table:
        street_obj_id += (str(el[0]) + ",")
    street_obj_id = street_obj_id[:len(street_obj_id) - 1]
    print("street object ID:" + street_obj_id)

    # do with apartment
    # word_id_array = get_word_id_string(line.address, False, True, "Search apartment")
    # print("apartment word id:" + word_id_array)
    # app_obj_table = Connector(query_app_id.format(word_id_array)).tableResult
    # app_obj_id = set()
    # for el in app_obj_table:
    #     app_obj_id.add(el[0])
    # print(app_obj_id)

    address_obj_table = Connector(query_address_id.format(city_obj_id, street_obj_id)).tableResult
    address_obj_id = set()
    apartment = get_array_words(line.address, " ")
    apartment_key = ""
    for ap in apartment:
        count_a = 0
        count_n = 0
        for alp in ap:
            if alp.isalpha():
                count_a += 1
            if alp.isdigit():
                count_n += 1
        if count_n > 0 and (count_a == 1 or count_a == 2):
            for alp in ap:
                if alp.isdigit():
                    apartment_key += alp
                if alp.isalpha():
                    apartment_key += (alp.lower())
            break
        if ap.isdigit():
            apartment_key = ap
            break
    if apartment_key == "":
        print("APARTMENT NOT IN LINE")
        continue
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    # f.write(" | ")
    find_apartment = False
    print("apartment " + apartment_key)
    for el in address_obj_table:
        print("address:")
        print(el)
        if el[3] == apartment_key:
            print(el[2])
            find_apartment = True
            break
        else:
            print("Not find")
    if not find_apartment:
        for el in address_obj_table:
            if distance(el[3], apartment_key) <= 1:
                print(el[2])
                # f.write(el[2])
    # f.write("\n")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    # f.close()
    # address_obj_id.add(el[0])
    # print("Address ID:" + str(address_obj_id))
    # res_set = address_obj_id.intersection(app_obj_id)
    # print("Intersection address and apartment:" + str(res_set))
    #
    # # do final query
    # address_obj_id_str = ""
    # for el_address in res_set:
    #     address_obj_id_str += (str(el[0]) + ",")
    #     address_obj_id_str = address_obj_id_str[:len(address_obj_id_str) - 1]
    #
    # line_result = Connector(query_final_data.format(address_obj_id_str)).tableResult
    # if len(line_result) == 0:
    #     print("Not found")
    # res_count = 0
    # for el in line_result:
    #     print("RESULT " + str(res_count) + ":" + el[1])

    # if row_count == 20:
    #     break
