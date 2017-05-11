from openpyxl import load_workbook as work_book


class ExcelReader:
    def __init__(self, file_name):
        self.fileName = file_name
        self.table = []
        wb = work_book(self.fileName, True)
        for sheet in wb.worksheets:
            for row in sheet.rows:
                values = []
                for cell in row:
                    values.append(cell.value)
                self.table.append(values)
