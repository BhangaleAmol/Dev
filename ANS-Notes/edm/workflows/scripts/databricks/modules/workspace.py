import requests
import json
import sys


def export_file(file_path, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/workspace/export"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {dbr_token}"
    }
    data = {
        "path": file_path,
        "format": "AUTO",
        "direct_download": "true"
    }

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.get(
        request_url, data=json.dumps(data), headers=headers)
    response.raise_for_status()

    return response.text


def list_files(folder_path, workspace_url, dbr_token):

    request_url = f"{workspace_url}/api/2.0/workspace/list"
    data = {'path': folder_path}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.get(
        request_url, data=json.dumps(data), auth=("token", dbr_token))

    response_dict = json.loads(response.text)
    return response_dict
