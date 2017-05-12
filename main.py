from ExcelReader import ExcelReader as ExcelReader
from AddressObject import AddressLine as AddressLine
from SSHConnector import SSHConnector as Connector
from Request import get_json_result


def city_format(city):
    city_str = city.strip()
    if city_str.isdigit():
        return ""
    first_is_digit = True
    index = 0
    for alpha in city_str:
        if not alpha.isalpha():
            index += 1
        else:
            first_is_digit = False

        if not first_is_digit:
            break
    city_str = city_str[index::]

    city_str = city_str.replace("m.", "")

    return city_str.strip()


def street_format(street):
    street_str = street.strip()
    street_str = street_str.replace("str.", "")
    street_str = street_str.replace("str ", "")

    index_app = street_str.find("app")

    return street_str


reader = ExcelReader('inputFile.xlsx')
table = reader.table
address_table = []
for line in table:
    address = AddressLine(*line)
    address_table.append(address)

# print(len(table))

# databaseConnector = Connector("Select * from words")

# words = {}
#
# for row in databaseConnector.tableResult:
#     words[row[1]] = row[0]

rowcount = 0
count_ok = 0
for line in address_table:
    rowcount += 1
    address_str = ""
    if line.city is not None:
        template_city = city_format(line.city)
    else:
        template_city = ""
    if line.address is not None:
        template_street = street_format(line.address)
    else:
        template_street = ""
    if template_street.find(template_city) >= 0:
        address_str = template_street
    elif template_city != "" and template_street != "":
        address_str = template_city + " " + template_street
    elif template_street == "":
        address_str = template_city
    else:
        address_str = template_street

    if address_str == "":
        print("blank address row #" + rowcount)
        continue
    address_str.strip()
    print(address_str)
    result = get_json_result(address_str)
    print("line #" + str(rowcount) + " " + result['status'])
    if result['status'] == "OK":
        count_ok += 1
        print("OK " + str(count_ok))

    # if count_ok == 10:
    #     break

print(count_ok)
