"""EPH GeoParquet Loader – main plugin class (toolbar button + action)."""

from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon
from .dialog import EphLoaderDialog


class EphLoaderPlugin:
    def __init__(self, iface):
        self.iface = iface
        self.action = None

    def initGui(self):
        self.action = QAction(
            QIcon(),
            "EPH GeoParquet Loader",
            self.iface.mainWindow(),
        )
        self.action.triggered.connect(self.run)
        self.iface.addToolBarIcon(self.action)
        self.iface.addPluginToMenu("EPH Loader", self.action)

    def unload(self):
        self.iface.removePluginMenu("EPH Loader", self.action)
        self.iface.removeToolBarIcon(self.action)

    def run(self):
        dlg = EphLoaderDialog(self.iface)
        dlg.exec_()
