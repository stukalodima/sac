from ExcelReader import ExcelReader as ExcelReader
from AddressObject import AddressLine as AddressLine
from Address import Address as Address
from PostgresConnector import PostgresConnector as Connector
from MapParser import MapParser as MapParser
from MapObjectUtils import MapObjectUtils as MapObjectUtils
from DictUtils import DictUtils as DictUtils

separators = (",", ".", "-", "(", ")", "/", "|", ":", ";")
key_word = ("building", "street", "apt", "str", "app", "ap", "yl", "ul", "fl", "kv", "highway", "bul", "apartment",
            "avenue", "prospekt", "obl", "ave", "room", "region", "district", "pgt", "область", "rayon", "selo", "city",
            "flat", "prov", "", "kvartira", "pereulok")
key_word_index = ("ukrpost", "ukraine", "ua", "-", "ukrposhta", "thezipc", "(", ")", "new", "mail", "code", "index",
                  "ind", "indeks", "postcode", "postal", "ukrain", "ukraina", "zip", ".", ",")

key_word_replace = ("(до 30 кг на одне місце)", "(до 30 кг)", "до 30 кг на одне місце", "до 30 кг", "Відділення №1:",
                    "Відділення №2:", "Відділення №3:", "Відділення №4:", "Відділення №5:", "Відділення №6:",
                    "Відділення №1",
                    "Відділення №2", "Відділення №3", "Відділення №4", "Відділення №5", "Відділення №6"
                    )

type_of_adm_level = {"вул.": ["str", "str.", "street", "yl", "ul", "вул.", "вул", "вулиця", "st.", "st"],
                     "пров.": ["пров", "пров.", "провулок", "prov", "pereulok"],
                     "просп.": ["ave.", "ave", "prospekt", "avenue", "просп", "проспект"],
                     "бульв.": ["bul.", "bul", "бульв", "бульв.", "бульвар"],
                     "шосе": ["highway", "шосе", "shosse"]
                     }

replace_char = ("&quot;", '"', "№")

words_table = []

print("Читаем файл адресов...")
reader = ExcelReader('inputFile.xlsx')
table = reader.table
reader = None
address_table = []
for line in table:
    address = AddressLine(*line)
    address_table.append(address)
table = None
print("Закончили читать файл адресов...")

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
print("Читаем таблицу улиц...")
data_base.get_streets_word(True)
print("Закончили читать таблицу улиц...")
print("Читаем таблицу индексов...")
data_base.get_indexes_id(True)
print("Закончили читать таблицу индексов...")
print("Читаем таблицу соответствия индексов и городов...")
data_base.get_settlement_by_index(True)
print("Закончили читать таблицу соответствия индексов и городов...")
print("Читаем таблицу соответствия индексов и улиц...")
data_base.get_street_by_index(True)
print("Закончили читать таблицу соответствия индексов и улиц...")

row_count = 0
start_row = 0
end_row = 19555
find_city = 0
find_street = 0
find_index = 0
print("Побежали по строкам")
print("\n")

result_array = []
for line in address_table:
    row_count += 1
    if row_count < start_row:
        continue
    if row_count > end_row:
        break

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    print("Обрабатываем строку №" + str(row_count))
    if len(line.city) == 0 and len(line.address) == 0 and len(line.index) == 0:
        print("Обе колонки адреса пусты... искать нечего")
        continue
    print(line)
    address = Address(line)
    print("Ищем индекс " + line.index)
    index_str = Connector.get_index_str(line.index, key_word_index)
    print(index_str)
    index_id = data_base.indexes.get(index_str)
    if index_id is not None:
        have_index = True
        find_index += 1
        print("Нашли индекс в базе")
    else:
        have_index = False
        print("Не нашли индекс в базе")

    our_city = None
    our_street = None
    settlement = dict()
    settlement_similar = dict()
    settlement_lev = dict()
    street = dict()
    street_similar = dict()
    street_lev = dict()
    new_address_str = line.address
    column_name_city = [line.city, new_address_str]
    for replace_word in key_word_replace:
        new_address_str = new_address_str.replace(replace_word, "")
    street_type = MapObjectUtils.get_street_type(new_address_str, type_of_adm_level)
    do_with_out_index = True
    if have_index:
        do_with_out_index = False
        street_id_array = []
        print("Формируем слова для поиска улиц")
        tmp_address = new_address_str
        new_address_str = new_address_str.replace(line.city, "")
        words_array_street = MapParser.get_word_array(new_address_str, key_word, separators, False)
        if len(words_array_street) == 0:
            new_address_str = tmp_address
            words_array_street = MapParser.get_word_array(new_address_str, key_word, separators, False)
        print("\nПробуем определить улицу")
        street = MapObjectUtils.get_adm_level(words_array_street, data_base.streets, "Улицы")
        street_id_array = data_base.streets_by_index.get(index_id)
        if len(street) > 0:
            if street_type is not None:
                street = MapObjectUtils.filter_street_by_type(street, street_type)
            if len(street) > 0:
                our_street = MapObjectUtils.get_street_from_index_array(street_id_array, street)
                if our_street is not None:
                    print(our_street)
        if our_street is None:
            print("Пробуем нечеткий поиск")
            text = Connector.get_text_query_street(words_array_street)
            data_base.select_from_db(text, False)
            street_similar = MapObjectUtils.get_adm_level_similar(data_base.tableResult, "Улицы")
            if len(street_similar) > 0:
                if street_type is not None:
                    street_similar = MapObjectUtils.filter_street_by_type(street_similar, street_type)
                if len(street_similar) > 0:
                    our_street = MapObjectUtils.get_street_from_index_array(street_id_array, street_similar)
                    if our_street is not None:
                        print(our_street)
            if our_street is None:
                print("Пробуем нечеткий поиск растояния Левенштейна")
                street_lev = MapObjectUtils.get_adm_level_lev(words_array_street, data_base.streets,
                                                              "Улицы по Левенштейну")
                if street_type is not None:
                    street_lev = MapObjectUtils.filter_street_by_type(street_lev, street_type)
                if len(street_lev) > 0:
                    our_street = MapObjectUtils.get_street_from_index_array(street_id_array, street_lev)
                    if our_street is not None:
                        print(our_street)

        if our_street is None:
            count_way = 0
            settlement_id = 0
            while count_way < 2:
                city_column_value = column_name_city[count_way]
                count_way += 1
                if our_city is not None:
                    break
                print("Формируем слова для поиска города")
                words_array_city = MapParser.get_word_array(city_column_value, key_word, separators, False)
                print("\nПробуем определить города")
                settlement = MapObjectUtils.get_adm_level(words_array_city, data_base.settlement, "Города")
                if len(settlement) > 0:
                    settlement_id = data_base.settlement_by_index.get(index_id)
                    our_city = {settlement_id: settlement.get(settlement_id)}
                    if our_city is not None:
                        print(our_city)
                if our_city is None:
                    print("Пробуем нечеткий поиск")
                    text = Connector.get_text_query_setlements(words_array_city)
                    data_base.select_from_db(text, False)
                    settlement_similar = MapObjectUtils.get_adm_level_similar(data_base.tableResult, "Города")
                    if our_city is None:
                        settlement_id = data_base.settlement_by_index.get(index_id)
                        our_city = {settlement_id: settlement_similar.get(settlement_id)}
                        if our_city is not None:
                            print(our_city)
                    if our_city is None:
                        print("Пробуем нечеткий поиск растояния Левенштейна")
                        settlement_lev = MapObjectUtils.get_adm_level_lev(words_array_city, data_base.settlement,
                                                                          "города по Левенштейну")
                        settlement_id = data_base.settlement_by_index.get(index_id)
                        our_city = {settlement_id: settlement_lev.get(settlement_id)}
                        if our_city is not None:
                            print(our_city)
            if our_city is not None:
                if len(street) > 0:
                    streets_by_city = DictUtils.filter_dict(street, our_city, "Ulici")
                    if len(streets_by_city) == 1:
                        our_street = streets_by_city
                        print(our_street)
                    else:
                        print(streets_by_city)
                        our_street = None
                elif len(street_similar) > 0:
                    streets_by_city = DictUtils.filter_dict(street_similar, our_city, "Ulici similar")
                    if len(streets_by_city) == 1:
                        our_street = streets_by_city
                        print(our_street)
                    else:
                        print(streets_by_city)
                elif len(street_lev) > 0:
                    streets_by_city = DictUtils.filter_dict(street_lev, our_city, "Ulici lev")
                    if len(streets_by_city) == 1:
                        our_street = streets_by_city
                        print(our_street)
                    else:
                        print(streets_by_city)
        if our_street is None and our_city is None:
            do_with_out_index = True
        elif our_city is not None:
            if len(our_city) > 1 or len(our_city) == 0:
                do_with_out_index = True
            else:
                for el in our_city.values():
                    if el is not None and el[5] != 1:
                        do_with_out_index = True

    if do_with_out_index:
        count_way = 0
        settlement_id = 0
        while count_way < 2:
            city_column_value = column_name_city[count_way]
            count_way += 1
            if (settlement is not None and len(settlement) > 0) \
                    or (settlement_similar is not None and len(settlement_similar) > 0) \
                    or (settlement_lev is not None and len(settlement_lev) > 0):
                break
            print("Формируем слова для поиска города")
            words_array_city = MapParser.get_word_array(city_column_value, key_word, separators, False)
            print("\nПробуем определить города")
            if settlement is None or len(settlement) == 0:
                settlement = MapObjectUtils.get_adm_level(words_array_city, data_base.settlement, "Города")
            if len(settlement) == 0:
                print("Пробуем нечеткий поиск")
                text = Connector.get_text_query_setlements(words_array_city)
                data_base.select_from_db(text, False)
                settlement_similar = MapObjectUtils.get_adm_level_similar(data_base.tableResult, "Города")
                if len(settlement_similar) == 0:
                    print("Пробуем нечеткий поиск растояния Левенштейна")
                    settlement_lev = MapObjectUtils.get_adm_level_lev(words_array_city, data_base.settlement,
                                                                      "города по Левенштейну")

        street_id_array = []
        print("Формируем слова для поиска улиц")
        tmp_address = new_address_str
        new_address_str = new_address_str.replace(line.city, "")
        words_array_street = MapParser.get_word_array(new_address_str, key_word, separators, False)
        if len(words_array_street) == 0:
            new_address_str = tmp_address
            words_array_street = MapParser.get_word_array(new_address_str, key_word, separators, False)
        print("\nПробуем определить улицу")
        street = MapObjectUtils.get_adm_level(words_array_street, data_base.streets, "Улицы")
        if len(street) == 0:
            print("Пробуем нечеткий поиск")
            text = Connector.get_text_query_street(words_array_street)
            data_base.select_from_db(text, False)
            street_similar = MapObjectUtils.get_adm_level_similar(data_base.tableResult, "Улицы")
            if len(street_similar) == 0:
                print("Пробуем нечеткий поиск растояния Левенштейна")
                street_lev = MapObjectUtils.get_adm_level_lev(words_array_street, data_base.streets,
                                                              "Улицы по Левенштейну")

        if street is not None and len(street) > 0:
            if len(settlement) > 0:
                streets_by_city = DictUtils.filter_dict(street, settlement, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
            elif len(settlement_similar) > 0:
                streets_by_city = DictUtils.filter_dict(street, settlement_similar, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
            elif len(settlement_lev) > 0:
                streets_by_city = DictUtils.filter_dict(street, settlement_lev, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
        elif street_similar is not None and len(street_similar) > 0:
            if len(settlement) > 0:
                streets_by_city = DictUtils.filter_dict(street_similar, settlement, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
            elif len(settlement_similar) > 0:
                streets_by_city = DictUtils.filter_dict(street_similar, settlement_similar, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
            elif len(settlement_lev) > 0:
                streets_by_city = DictUtils.filter_dict(street_similar, settlement_lev, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
        elif street_lev is not None and len(street_lev) > 0:
            if len(settlement) > 0:
                streets_by_city = DictUtils.filter_dict(street_lev, settlement, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
            elif len(settlement_similar) > 0:
                streets_by_city = DictUtils.filter_dict(street_lev, settlement_similar, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
            elif len(settlement_lev) > 0:
                streets_by_city = DictUtils.filter_dict(street_lev, settlement_lev, "Ulici")
                if len(streets_by_city) > 0:
                    our_street = streets_by_city
    if our_street is not None:
        if type(our_street) == dict:
            if len(our_street) > 1:
                if street_type is not None:
                    our_street = MapObjectUtils.filter_street_by_type(our_street, street_type)
            if len(our_street) != 1:
                our_street = None
            else:
                tmp = ""
                for el in our_street.keys():
                    tmp = el
                our_street = our_street.get(tmp)
    if len(settlement) == 1:
        our_city = settlement
    else:
        print(settlement)
    print("Формируем слова для поиска номера дома")
    words_array_kv = MapParser.get_word_array(new_address_str, key_word, separators, True)

    if our_street is not None:
        text = Connector.get_text_apartment(our_street[2], our_street[4])
        data_base.select_from_db(text, False)
        find_kv = False
        kv = None
        if len(data_base.tableResult) > 0 and len(words_array_kv) > 0:
            for el in data_base.tableResult:
                if el[1] == words_array_kv[0]:
                    find_kv = True
                    kv = el
        if not find_kv and new_address_str.find("/") > -1 and len(words_array_kv) > 1:
            if len(data_base.tableResult) > 0:
                for el in data_base.tableResult:
                    if el[1] == words_array_kv[0] + "/" + words_array_kv[1]:
                        find_kv = True
                        kv = el
    if our_street is not None and kv is not None:
        text = Connector.get_text_city_name(our_street[2])
        data_base.select_from_db(text, False)
        if len(data_base.tableResult) > 0:
            city_name = data_base.tableResult[0][0]
        find_city += 1
        find_street += 1
        line.find_address = city_name + " " + str(our_street[3]) + " " + str(our_street[1]) + " " + str(kv[1])
        line.centroid = str(kv[0])
    elif our_street is not None:
        text = Connector.get_text_city_name(our_street[2])
        data_base.select_from_db(text, False)
        if len(data_base.tableResult) > 0:
            city_name = data_base.tableResult[0][0]
        find_city += 1
        find_street += 1
        num_kv = ""
        if len(words_array_kv) > 0:
            num_kv = words_array_kv[0]
        line.find_address = city_name + " " + str(our_street[3]) + " " + str(our_street[1]) + " " + str(num_kv)
        line.centroid = str(our_street[0])
    elif our_city is not None:
        if len(our_city) == 1:
            for el in our_city.values():
                if el is not None and el[5] == 1:
                    line.find_address = el[1]
                    line.centroid = str(el[0])
        else:
            print(our_city)
    print("Город:")
    print(our_city)
    print("Улица:")
    print(our_street)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

ExcelReader.save_to_file('outFile.xlsx', address_table)
