from requests import exceptions, get
import unicodedata
import fileinput
import json
import re
import hashlib
import datetime, time
from os import mkdir, getcwd, path

DAY_IN_SECONDS = 86400

def filename_safe_string(value):
  value = re.sub('[^\w\s-]', '', value).strip().lower()
  value = re.sub('[-\s]+', '-', value)
  return value



def filename_from_headers(headers):
    cd = headers.get("Content-Disposition", "")
    if cd and "filename=" in cd:
        return cd.split("filename=")[1]
    else:
        return ''

def resource_filesafe_name(resource):
    return filename_safe_string(resource['name']) + "-"  + resource['format'].lower().strip()


def write_to_log(string, filename="download.log"):
    with open(path.join('dats', filename), "a+") as log:
      log.write(str(datetime.datetime.now()) + ": " + string + "\n")

def older_than_one_day(path):
  now = time.time()
  file_mod_time = path.getmtime(path)
  return (now - file_mod_time) > DAY_IN_SECONDS

def should_create_file(filepath, resource={}):
  if path.exists(filepath):
    return older_than_update_frequency(filepath, resource)
  else:
    return True

def older_than_update_frequency(filepath, resource):
  frequency_lookup = {
    "Update Frequency: Daily": DAY_IN_SECONDS,
    "Update Frequency:  Daily": DAY_IN_SECONDS,
    "Update Frequency: Weekly": (DAY_IN_SECONDS * 7),
    "Update Frequency: Monthly": (DAY_IN_SECONDS * 31),
    "Update frequency: Monthly": (DAY_IN_SECONDS * 31),
    }
  freq = frequency_lookup.get(resource.get('description','').strip(), False)
  if freq:
    now = time.time()
    file_mod_time = path.getmtime(filepath)
    return (now - file_mod_time) > freq
  else:
    return False

def is_expected_filetype(resource, request):
  format_lookup = {
      'csv': ('text/csv',),
      'shp': ('application/zip', 'application/octet-stream'),
      'kml': ('application/vnd.google-earth.kml+xml',),
      'geojson': ('application/vnd.geo+json', 'application/json'),
      'html': ('text/html',)
      }

  described_format   = resource['format'].lower()
  format_from_header = request.headers['Content-Type']
  return any(format in format_from_header for format in format_lookup.get(described_format,()))

def curated_urls(dataset):
  if dataset.get('use_odp_url', '').lower() == "no":
      urls = dataset['correct_url'].split(";;")
  else:
      urls = [dataset.get('dataset_url', '')]
  return urls


# Create a data structure for datasets listed in Google Spreadsheet

curated_datasets = {}
with open("curated_datasets.jsonlines") as curated:
    for line in curated.readlines():
        dataset = json.loads(line)
        if dataset['create_dat_collection'].lower() == "yes":
            if not curated_datasets.get(dataset['package_id'], False):
                curated_datasets[dataset['package_id']] = {}
            curated_datasets[dataset['package_id']][dataset['dataset_id']] = dataset
    print "Done reading curated datasets"

print curated_datasets

for line in fileinput.input():
  package = json.loads(line)

  if not curated_datasets.get(package['id'], False):
      print "Skipping " + package['title']
      continue

  package_name = filename_safe_string(package['title'])
  package_dir = path.join(getcwd(), 'dats', package_name )

  if not path.exists(package_dir):
    mkdir(package_dir)
    write_to_log("INFO: Creating dat directory for: " + package['title'])

  package_meta_file_path = path.join(package_dir, package['id'] + "-ckan-metadata.json")

  if should_create_file(package_meta_file_path):
    with open(package_meta_file_path, "w") as package_meta:
      package_meta.write(line)
    write_to_log("INFO : Creating metadata dump for package " + package_name)

  for resource in package['resources']:
      resource_name = resource_filesafe_name(resource)
      resource_dir = path.join(package_dir, resource_name )
      resource_urls = curated_urls(curated_datasets.get(package['id'], {}).get(resource['id'], {}))
      if not path.exists(resource_dir):
        mkdir(resource_dir)
        write_to_log("INFO: Creating resource directory for: " + resource_name)

      url = resource_urls[0]

      try:
        request = get(url)
        resource_file_path = path.join(resource_dir, str(resource['id']))
        if True:
        #if should_create_file(resource_file_path, resource):
            write_to_log("INFO : Saving a new copy of the dataset for " + resource_name)
            with open(resource_file_path, "w") as resource_file:
                resource_file.write(request.content)
            with open(path.join(resource_dir, resource_name + "-download-manifest"), "w") as manifest:

                manifest.write(json.dumps({
                    'content-sha256': hashlib.sha256(request.content).hexdigest(),
                    'download-at' : str(datetime.datetime.now),
                    'headers': dict(request.headers),
                    'original_url': url,
                    'filename': filename_from_headers(resource),
                }))

      except exceptions.RequestException as e:
        with open("download.log", "w+") as log:
            log.write("ERROR : Failed download for " + resource_name + " at url " + resource['url'] + "\n")



      resource_metadata_path = path.join(package_dir, resource_name + "-metadata.json")
      if should_create_file(resource_metadata_path, resource):
          with open(resource_metadata_path, "w") as resource_metadata_file:
              resource_metadata_file.write(json.dumps(resource))
