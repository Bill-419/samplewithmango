import sys
import pandas as pd
from PySide6.QtWidgets import QTableWidgetItem, QFileDialog, QMessageBox
from PySide6.QtCore import Qt

class TableHandler:
    def __init__(self, table_widget, db_handler):
        self.table_widget = table_widget
        self.db_handler = db_handler

    def save_data(self):
        table = self.table_widget.get_table()
        data = []
        for row in range(table.rowCount()):
            row_data = {}
            for col in range(table.columnCount()):
                item = table.item(row, col)
                if item:
                    row_data[str(col)] = {
                        'text': item.text(),
                        'alignment': item.textAlignment(),
                        'row_height': table.rowHeight(row),
                        'column_width': table.columnWidth(col)
                    }
                else:
                    row_data[str(col)] = {
                        'text': '',
                        'alignment': int(Qt.AlignLeft | Qt.AlignVCenter),
                        'row_height': table.rowHeight(row),
                        'column_width': table.columnWidth(col)
                    }
            data.append(row_data)

        self.db_handler.save_table(data)
        QMessageBox.information(self.table_widget, "保存成功", "表格数据已保存到数据库")

    def refresh_data(self):
        table = self.table_widget.get_table()
        if table.rowCount() > 0:
            table.removeRow(table.rowCount() - 1)
        self.load_table_data()

        table = self.table_widget.get_table()
        if table.rowCount() > 0:
            table.removeRow(table.rowCount() - 2)
        print("数据已刷新")

    def export_to_excel(self):
        table = self.table_widget.get_table()
        path, _ = QFileDialog.getSaveFileName(self.table_widget, "导出为Excel", "", "Excel Files (*.xlsx)")
        if path:
            data = []
            for row in range(table.rowCount()):
                row_data = []
                for col in range(table.columnCount()):
                    item = table.item(row, col)
                    row_data.append(item.text() if item else '')
                data.append(row_data)
            df = pd.DataFrame(data)
            df.to_excel(path, index=False, header=False)
            QMessageBox.information(self.table_widget, "导出成功", f"表格已成功导出到 {path}")

    def load_table_data(self):
        response = self.db_handler.get_table()
        if response["status"] == "success":
            data = response["data"]
            self.populate_table(data)
        else:
            self.populate_table_with_default_data()

    def populate_table(self, data):
        table = self.table_widget.get_table()
        table.clearContents()

        if not isinstance(data, list) or not all(isinstance(row, dict) for row in data):
            raise ValueError("Data must be a list of dictionaries")

        row_count = len(data)
        col_count = max(len(row) for row in data) if row_count > 0 else 0
        table.setRowCount(row_count)
        table.setColumnCount(col_count)

        for row_idx, row_data in enumerate(data):
            if not isinstance(row_data, dict):
                raise ValueError(f"Row data at index {row_idx} is not a dictionary: {row_data}")

            for col_idx_str, cell_data in row_data.items():
                try:
                    col_idx = int(col_idx_str)
                except ValueError:
                    continue

                if isinstance(cell_data, dict):
                    item = QTableWidgetItem(cell_data.get('text', ''))
                    item.setTextAlignment(cell_data.get('alignment', int(Qt.AlignLeft | Qt.AlignVCenter)))
                    table.setItem(row_idx, col_idx, item)
                    table.setRowHeight(row_idx, cell_data.get('row_height', table.rowHeight(row_idx)))
                    table.setColumnWidth(col_idx, cell_data.get('column_width', table.columnWidth(col_idx)))
                else:
                    item = QTableWidgetItem(cell_data)
                    table.setItem(row_idx, col_idx, item)

    def populate_table_with_default_data(self):
        table = self.table_widget.get_table()
        table.setRowCount(2)
        table.setColumnCount(11)
        for row in range(2):
            for col in range(11):
                item = QTableWidgetItem(f'({row}, {col})')
                table.setItem(row, col, item)
