import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QCheckBox, QLineEdit, QInputDialog


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Database Creation Tool")
        self.initUI()

    def initUI(self):
        main_layout = QVBoxLayout()

        # Metrics Box
        metrics_box = QVBoxLayout()
        metrics_label = QLabel("Metrics:")
        metrics_box.addWidget(metrics_label)

        self.metrics_layout = QVBoxLayout()
        metrics_box.addLayout(self.metrics_layout)

        add_metric_button = QPushButton("Add Metric")
        add_metric_button.clicked.connect(lambda: self.add_item("Metric"))
        metrics_box.addWidget(add_metric_button)

        main_layout.addLayout(metrics_box)

        # Dimensions Box
        dimensions_box = QVBoxLayout()
        dimensions_label = QLabel("Dimensions:")
        dimensions_box.addWidget(dimensions_label)

        self.dimensions_layout = QVBoxLayout()
        dimensions_box.addLayout(self.dimensions_layout)

        add_dimension_button = QPushButton("Add Dimension")
        add_dimension_button.clicked.connect(lambda: self.add_item("Dimension"))
        dimensions_box.addWidget(add_dimension_button)

        main_layout.addLayout(dimensions_box)

        # Create Database Button
        create_db_button = QPushButton("Create Database")
        create_db_button.clicked.connect(self.create_database)
        main_layout.addWidget(create_db_button)

        self.setLayout(main_layout)

    def add_item(self, name):
        item_name, ok = QInputDialog.getText(self, f"Add {name}", f"Enter {name} name:")
        if ok and item_name:
            checkbox = QCheckBox(item_name)
            if name == "Metric":
                self.metrics_layout.addWidget(checkbox)
            else:
                self.dimensions_layout.addWidget(checkbox)

    def create_database(self):
        selected_metrics = [self.metrics_layout.itemAt(i).widget().text() for i in range(self.metrics_layout.count())]
        selected_dimensions = [self.dimensions_layout.itemAt(i).widget().text() for i in range(self.dimensions_layout.count())]

        print("Selected Metrics:", selected_metrics)
        print("Selected Dimensions:", selected_dimensions)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
