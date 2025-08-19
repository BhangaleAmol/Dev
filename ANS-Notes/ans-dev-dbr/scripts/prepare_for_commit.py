import os

git_path = os.path.realpath('..')
target_path = git_path + '\\notebooks\\notebooks'


def setup_git_repo(git_path):
    print("GIT hard reset")
    os.chdir(git_path)
    os.system('git config core.autocrlf true')
    os.system('git fetch origin')
    os.system('git reset --hard origin/master')
    os.system('git clean -f -d')


def export_workspace_files(target_path):
    print("Exporting notebooks to " + target_path)
    export_cmd = 'databricks workspace export_dir /notebooks "' + target_path + '" -o'
    os.system(export_cmd)


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


setup_git_repo(git_path)
export_workspace_files(target_path)
convert_to_crlf(target_path)
