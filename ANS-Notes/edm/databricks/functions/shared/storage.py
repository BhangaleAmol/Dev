from databricks.sdk.runtime import dbutils


def file_exists(file_path: str) -> bool:
    try:
        dbutils.fs.ls(file_path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise


def list_files(folder_path: str):
    for x in dbutils.fs.ls(folder_path):
        if x.path[-1] != '/':
            yield x
        else:
            for y in list_files(x.path):
                yield y
