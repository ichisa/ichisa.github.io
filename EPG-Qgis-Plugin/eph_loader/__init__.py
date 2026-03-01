"""EPH GeoParquet Loader – QGIS plugin entry point."""


def classFactory(iface):
    from .plugin import EphLoaderPlugin
    return EphLoaderPlugin(iface)
