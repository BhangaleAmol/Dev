import requests
import json
import sys


def create_repository(repository_path, git_url, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/repos"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {dbr_token}"
    }
    data = {
        "url": git_url,
        "provider": "azureDevOpsServices",
        "path": repository_path
    }

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.post(
        request_url, data=json.dumps(data), headers=headers)
    response.raise_for_status()

    response_dict = json.loads(response.text)
    return response_dict


def get_repository_id(repository_path, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/repos"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {dbr_token}"
    }
    data = {"path_prefix": repository_path}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.get(
        request_url, data=json.dumps(data), headers=headers)
    response.raise_for_status()

    repository_id = None
    response_dict = json.loads(response.text)
    if 'repos' in response_dict:
        repository_id = response_dict['repos'][0]['id']

    return repository_id


def remove_repository(repository_id, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/repos/{repository_id}"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {dbr_token}"
    }
    data = {}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.delete(
        request_url, data=json.dumps(data), headers=headers)
    response.raise_for_status()
    return response.text


def update_repository(repository_id, git_branch, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/repos/{repository_id}"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {dbr_token}"
    }
    data = {"branch": git_branch}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.patch(
        request_url, data=json.dumps(data), headers=headers)
    response.raise_for_status()
    return response.text
