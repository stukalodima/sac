from ExcelReader import ExcelReader as ExcelReader
from AddressObject import AddressLine as AddressLine

reader = ExcelReader('inputFile.xlsx')
table = reader.table
for line in table:
    address = AddressLine(*line)
    print(address)

print(len(table))