import os
import sys

target_path = sys.argv[1]
source_path = "../notebooks" + sys.argv[1]

print("Importing notebooks from " + source_path + " to " + target_path)
command = "databricks workspace import_dir {0} {1} -o".format(
    source_path, target_path)
print(command)
os.system(command)
