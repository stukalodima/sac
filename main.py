from ExcelReader import ExcelReader as ExcelReader
from AddressObject import AddressLine as AddressLine
from SSHConnector import SSHConnector as Connector
from geopy.geocoders import Nominatim
from Request import get_json_result

# geolocator = Nominatim(country_bias='Украина')

separators = (" ", ",", ".", "-", "(", ")")
key_word = ["building", "street", "apt"]

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

query_app_id = ("SELECT mp.id AS ap_id\n"
                "FROM words AS w\n"
                "  INNER JOIN links l ON l.word_id = w.id\n"
                "  INNER JOIN map_objects mp ON mp.id = l.object_id\n"
                "WHERE w.id IN ({0})\n"
                "      AND mp.category_n = 1")

query_address_id = ("SELECT a.id\n"
                    "                FROM address a\n"
                    "                WHERE a.settlement_id IN ({0})\n"
                    "                      AND a.street_id IN ({1})")


def get_array_words(string, separator_id):
    result = string.split(separators[separator_id])
    return result


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


reader = ExcelReader('inputFile.xlsx')
table = reader.table
address_table = []
for line in table:
    address = AddressLine(*line)
    address_table.append(address)

databaseConnector = Connector("SELECT * FROM words")
words_table = databaseConnector.tableResult

row_count = 0
for line in address_table:
    row_count += 1
    if row_count < 3:
        continue
    address_str = ""
    # do with city
    word_id_array = ""
    res = (get_array_words(line.city, 0))
    print("!!!" + line.city + "!!!")
    for el in res:
        word = el.lower()
        find = False
        if word in key_word:
            continue
        for word_row in words_table:
            if word == word_row[1]:
                find = True
                word_id_array += (str(word_row[0]) + ",")
            if find:
                break
    word_id_array = word_id_array[:len(word_id_array) - 1]
    print("city word id:" + word_id_array)
    city_obj_table = Connector(query_city_id.format(word_id_array)).tableResult
    city_obj_id = ""
    for el in city_obj_table:
        city_obj_id += (str(el[0]) + ",")
        print("city:" + str(el[0]))
    city_obj_id = city_obj_id[:len(city_obj_id) - 1]

    # do with street
    word_id_array = ""
    tmp_res = (get_array_words(line.address, 1))
    res = []
    for tmp_word in tmp_res:
        iter_res = get_array_words(tmp_word, 0)
        for el_iter in iter_res:
            res.append(el_iter)
    print("!!!" + line.address + "!!!")
    for el in res:
        word = el.lower()
        find = False
        print("Is:" + word)
        for word_row in words_table:
            if word == word_row[1]:
                print("find:" + word)
                find = True
                word_id_array += (str(word_row[0]) + ",")
            if find:
                break
    word_id_array = word_id_array[:len(word_id_array) - 1]
    print("str word id:" + word_id_array)
    street_obj_table = Connector(query_street_id.format(word_id_array)).tableResult
    street_obj_id = ""
    for el in street_obj_table:
        street_obj_id += (str(el[0]) + ",")
        print("street:" + str(el[0]))
    street_obj_id += street_obj_id[:len(street_obj_id) - 1]
    print("----")
    # do with app
    print("app word id:" + word_id_array)
    app_obj_table = Connector(query_app_id.format(word_id_array)).tableResult
    app_obj_id = ""
    for el in app_obj_table:
        app_obj_id += (str(el[0]) + ",")
        print("app:" + str(el[0]))
    app_obj_id += app_obj_id[:len(app_obj_id) - 1]

    addr_obj_table = Connector(query_address_id.format(city_obj_id,street_obj_id)).tableResult
    addr_obj_id = ""
    for el in addr_obj_table:
        addr_obj_id += (str(el[0]) + ",")
        print("addr id:" + str(el[0]))



    if row_count == 3:
        break





















# rowcount = 0
# count_ok = 0
# for line in address_table:
#     rowcount += 1
#     address_str = ""
#
#     if line.city is not None:
#         template_city = city_format(line.city)
#     else:
#         template_city = ""
#
#     if line.address is not None:
#         template_street = street_format(line.address)
#     else:
#         template_street = ""
#     query = {"city": template_city, "street": template_street}
#     print(line)
#     print("Row:" + str(rowcount))
#     try:
#         location = geolocator.geocode(query, False)
#     except:
#         print("Not connect to DB")
#     if location is None:
#         print("Empty result")
#     else:
#         if len(location) == 1:
#             print(location[0].address)
#             print((location[0].latitude, location[0].longitude))
#         else:
#             print("Find " + str(len(location)) + " result")
#             for el in location:
#                 print(el)
#                 print((el.latitude, el.longitude))
#     print("")









# 169309 wordid




































# rowcount = 0
# count_ok = 0
# for line in address_table:
#     rowcount += 1
#     address_str = ""
#     if line.city is not None:
#         template_city = city_format(line.city)
#     else:
#         template_city = ""
#     if line.address is not None:
#         template_street = street_format(line.address)
#     else:
#         template_street = ""
#     if template_street.find(template_city) >= 0:
#         address_str = template_street
#     elif template_city != "" and template_street != "":
#         address_str = template_city + " " + template_street
#     elif template_street == "":
#         address_str = template_city
#     else:
#         address_str = template_street
#
#     if address_str == "":
#         print("blank address row #" + rowcount)
#         continue
#     address_str.strip()
#     print(address_str)
#     result = get_json_result(address_str)
#     print("line #" + str(rowcount) + " " + result['status'])
#     if result['status'] == "OK":
#         count_ok += 1
#         print("OK " + str(count_ok))
#
#     if count_ok == 20:
#         break
#
# print(count_ok)
