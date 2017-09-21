from kanner import settings

class Resource(object):

    def __init__(self, metadata, dest_path):
        class_name = settings.custom_harvester.get(metadata["id"], "DefaultResource")
        module = __import__("kanner.resource_harvesters")
        _class = getattr(module, class_name)
        self.harvester = _class(metadata, dest_path)


    def harvest():
        self.harvester.harvest()
