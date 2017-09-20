import json
from os import path
from kanner.resource_harvesters import resource_harvester

class PackageHarvester(object):

    DAY_IN_SECONDS = 86400

    def __init__(self, package_metadata, dest_path, force_harvest=false):
        self.metadata = JSON.loads(package_metadata)
        self.id = self.metadata['id']
        self.name = self.metadata['title']
        self.metadata_path = path.join(dest_path, safe_package_name())
        self.update_frequency = guess_update_frequency()
        self.force_harvest = force_harvest



    def harvest(self):
        if harvest_is_required():
            ensure_package_directory_exists()
            save_package_metadata()
            for resource in self.metadata['resources']:
                Resource(resource, dest_path).harvest()





    def harvest_is_required(self):
        #default to not harvesting
        is_required = False

        if self.force_harvest:
            print("Harvest of {0.name} forced by constructor flag".format(self))
            return True
        if path.exists(package_metadata_file_path()):
            is_required = is_within_frequency_limit(package_metadata_file_path())
            if is_required:
                print("Harvest of {0.name} triggered by Update Frequency check".format(self))
        else:
            is_required = True
            print("Harvest of {0.name} triggered because file does not yet exist".format(self))

        return is_required



    def ensure_package_directory_exists(self):
        if not path.exists(self.metadata_path):
          mkdir(package_path)
          print("INFO: Creating directory for: " + self.name)

    def save_package_metadata(self):
        with open(package_metadata_file_path()) as package_meta:
          package_meta.write(self.metadata)
        print("Metadata file for {0.name} created".format(self))


    def safe_package_name(self):
        filename_safe_string(self.name)


    def filename_safe_string(value):
        value = re.sub('[^\w\s-]', '', value).strip().lower()
        value = re.sub('[-\s]+', '-', value)
        return value

    def package_metadata_file_path(self):
        path.join(self.metadata_path, safe_package_name + "ckan-metadata.json")

    def is_within_frequency_limit(self, filepath):
            now = time.time()
            file_mod_time = path.getmtime(filepath)




    def guess_update_frequency(self):
        #default to quarterly
        freq = DAY_IN_SECONDS * 91
        desc = self.metadata['description'].lower()

        if "update frequency" in desc:
            if "daily" in desc:
                freq = DAY_IN_SECONDS
            if "weekly" in desc:
                freq = DAY_IN_SECONDS * 7
            if "monthly" in desc:
                freq = DAY_IN_SECONDS * 31

        return freq


#
