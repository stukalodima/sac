from ExcelReader import ExcelReader
from PostgresConnector import PostgresConnector as Connector
from Parser import Parser as Parser
from DictUtils import DictUtils as DictUtils
from MapObjectUtils import MapObjectUtils as MapObjectUtils

separators = (",", ".", "-", "(", ")", "/", "\\")

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

ind = 0
while ind < 5:
    filename = "indexes" + ("", "1", "2", "3", "4")[ind] + ".xlsx"
    print("\n")
    print("Читаем " + filename + "...")
    index_file = ExcelReader(filename)
    print("Закончили читать " + filename + "...")
    print("\n")
    index_table = index_file.table
    ind += 1

    row_count = 0
    start_row = 25857
    for line in index_table:
        row_count += 1
        # if row_count < start_row:
        #     continue
        # if row_count > start_row + 20:
        #     break
        print(line)
        # Получаем слова для поиска адм. единиц
        words_array_obl = Parser.get_word_array(line[0], separators, "")
        words_array_rn = Parser.get_word_array(line[1], separators, "")
        words_array_city = Parser.get_word_array(line[2], separators, "")
        words_array_street = Parser.get_word_array(line[5], separators, "")

        # Получаем адм. единицы
        obl = MapObjectUtils.get_adm_level(words_array_obl, data_base.adm_level1, "Области")
        rn = MapObjectUtils.get_adm_level(words_array_rn, data_base.adm_level2, "районы")
        city = MapObjectUtils.get_adm_level(words_array_city, data_base.settlement, "Города")

        # Название улиц может состоять из нескольких слов
        # Нужно получить все ссылки и найти пересечения
        maps = []
        streets = dict()
        for word in words_array_street:
            text = get_text_query_street
            data_base.select_from_db(text, False)
            tmp_dict = dict()
            tmp_dict = MapObjectUtils.get_adm_level_similar(data_base.tableResult, tmp_dict, "Улицы")
            streets.update(tmp_dict)
            tmp_set = set()
            tmp_set.update(tmp_dict.keys())
            maps.append(tmp_set)

        # По каждому слову делаем множиства ссылок и ищем пересечение множеств
        result_set = set()
        result_set.update(maps[0])
        for el_maps in maps:
            result_set.intersection_update(el_maps)
        street = dict()
        for el_set in result_set:
            street.update({el_set: streets.get(el_set)})

        # Получаем индекс и если он меньше 5 символов добавляем лидирующие нули
        index = str(line[3])
        while len(index) < 5:
            index = "0" + index

        # Фильтруем все админ единици
        print("obl: " + str(obl))
        rn = DictUtils.filter_dict(rn, obl, "r-n: ")
        if len(rn) == 0:
            for key, value in obl.items():
                if value[2] in city.keys():
                    city = {value[2]: city.get(value[2])}
        if len(city) > 1:
            city = DictUtils.filter_dict(city, rn, "city: ")
        else:
            print("city: " + str(city))
        street = DictUtils.filter_dict(street, city, "street_all: ")
        street = DictUtils.filter_street_dict(street, line[4], "street: ")
        # Если нет ни улици ни города то эта запись нам бесполезна
        if len(street) == 0 and len(city) == 0:
            continue

        # Ищем ID индекса в базе
        data_base.select_from_db("SELECT * FROM indexes WHERE index = '%(index)s'" % {"index": index}, True)
        # Если его нет записываем
        if len(data_base.tableResult) == 0:
            data_base.insert_into_db("INSERT INTO indexes (index) VALUES ('%(index)s')" % {"index": index}, True)
            data_base.select_from_db("SELECT * FROM indexes WHERE index = '%(index)s'" % {"index": index}, True)

        index_id = data_base.tableResult[0][0]

        # Ищем запись об адресе в базе
        parameters = {"adm_level1": DictUtils.get_key_isnull_from_dict(obl),
                      "adm_level2": DictUtils.get_key_isnull_from_dict(rn),
                      "settlement": DictUtils.get_key_isnull_from_dict(city),
                      "street": DictUtils.get_key_isnull_from_dict(street),
                      "index": index_id}

        data_base.select_from_db(("SELECT *\n"
                                  "FROM index_chains ic\n"
                                  "WHERE ic.index = %(index)s\n"
                                  "      AND ic.street %(street)s\n"
                                  "      AND ic.settlement %(settlement)s\n"
                                  "      AND ic.adm_level2 %(adm_level2)s\n"
                                  "      AND ic.adm_level1 %(adm_level1)s" % parameters), True)
        # Если она есть то переходим к следующей строке
        if len(data_base.tableResult) != 0:
            continue

        # Записываем в базу записи об адресе по индексу
        parameters = {"adm_level1": DictUtils.get_key_from_dict(obl), "adm_level2": DictUtils.get_key_from_dict(rn),
                      "settlement": DictUtils.get_key_from_dict(city), "street": DictUtils.get_key_from_dict(street),
                      "index": index_id}
        data_base.insert_into_db("INSERT INTO index_chains (adm_level1, adm_level2, settlement, street, index) \n"
                                 "VALUES (%(adm_level1)s, %(adm_level2)s, %(settlement)s, %(street)s, %(index)s)\n"
                                 "" % parameters, True)

        print(str(index) + " inserted!!!")
