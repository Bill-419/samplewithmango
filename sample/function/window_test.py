# main_window.py
import sys
from PySide6.QtWidgets import QMainWindow, QVBoxLayout, QWidget, QPushButton, QHBoxLayout, QApplication,QColorDialog,QInputDialog
from server.database import MongoDBHandler
from function.table import TableWidget
from table_handler import TableHandler  # Assuming TableHandler is defined in table_handler.py


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Table Handler Example")

        self.table_widget = TableWidget()  # Assuming TableWidget is implemented in function.table

        # Provide the necessary arguments for MongoDBHandler
        server_url = "http://localhost:5000"
        uri = "mongodb://localhost:27017/"
        db_name = "test_db"
        collection_name = "test_collection"
        self.db_handler = MongoDBHandler(uri, db_name, collection_name)

        self.table_handler = TableHandler(self.table_widget, self.db_handler)

        layout = QVBoxLayout()
        layout.addWidget(self.table_widget)

        button_layout = QHBoxLayout()

        save_button = QPushButton("Save Data")
        save_button.clicked.connect(self.table_handler.save_data)
        button_layout.addWidget(save_button)

        refresh_button = QPushButton("Refresh Data")
        refresh_button.clicked.connect(self.table_handler.refresh_data)
        button_layout.addWidget(refresh_button)

        export_button = QPushButton("Export to Excel")
        export_button.clicked.connect(self.table_handler.export_to_excel)
        button_layout.addWidget(export_button)

        layout.addLayout(button_layout)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self.table_handler.load_table_data()


if __name__ == "__main__":
    app = QApplication(sys.argv)

    window = MainWindow()
    window.show()

    sys.exit(app.exec())
