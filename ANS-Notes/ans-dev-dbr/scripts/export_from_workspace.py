import os
import sys

if len(sys.argv) > 1:
    folder_path = sys.argv[1]
else:
    folder_path = ''

git_path = os.path.realpath('..')
source_path = "/notebooks" + folder_path.replace('\\', '/')
target_path = git_path + '\\notebooks\\notebooks' + \
    folder_path.replace('/', '\\')


def export_workspace_files(source_path, folder_path):

    print("Exporting notebooks from " + source_path + " to " + target_path)
    command = 'databricks workspace export_dir {0} "{1}" -o'.format(
        source_path, target_path)
    print(command)
    os.system(command)


def convert_to_crlf(target_path):
    print("Converting from LF to CRLF")

    WINDOWS_LINE_ENDING = b'\r\n'
    UNIX_LINE_ENDING = b'\n'

    for root, dirs, files in os.walk(target_path):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)

                with open(file_path, 'rb') as open_file:
                    content = open_file.read()

                content = content.replace(
                    WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

                content = content.replace(
                    UNIX_LINE_ENDING, WINDOWS_LINE_ENDING)

                with open(file_path, 'wb') as open_file:
                    open_file.write(content)


export_workspace_files(source_path, folder_path)
convert_to_crlf(target_path)
print("Done")
