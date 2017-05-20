from openpyxl import load_workbook as work_book


class ExcelReader:
    def __init__(self, file_name):
        self.fileName = file_name
        self.table = []
        wb = work_book(self.fileName, True)
        head = True
        for sheet in wb.worksheets:
            for row in sheet.rows:
                if head:
                    head = False
                    continue
                values = []
                for cell in row:
                    if cell.value is not None:
                        values.append(cell.value)
                    else:
                        values.append("")
                self.table.append(values)
