from PySide6.QtWidgets import QMainWindow,QInputDialog, QVBoxLayout, QWidget, QTableWidget, QTableWidgetItem, QApplication,QColorDialog
from PySide6.QtGui import QColor
from PySide6.QtCore import Qt
from function.menu_base import MenuOperations
class TableWidget(QWidget):
    def __init__(self, table=None):
        super().__init__()

        if table is None:
            self.table = QTableWidget(6, 6)
            self.items = []  # 存储所有的QTableWidgetItem对象
            for row in range(6):
                for col in range(6):
                    item = QTableWidgetItem(f"Item {row + 1},{col + 1}")
                    item.setBackground(QColor("#ffffff"))  # 设置默认背景颜色为白色
                    item.setForeground(QColor("#000000"))  # 设置默认字体颜色为黑色
                    self.table.setItem(row, col, item)
                    self.items.append(item)  # 将item添加到列表中
        else:
            self.table = table

        self.menu_operations = MenuOperations(self.table)
        self.table.cellDoubleClicked.connect(self.menu_operations.on_cell_double_clicked)

        layout = QVBoxLayout(self)
        layout.addWidget(self.table)

        # 创建上下文菜单
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.menu_operations.open_menu)

    def get_table(self):
        return self.table

    @staticmethod
    def create_table_widget():
        return TableWidget()
