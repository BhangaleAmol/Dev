import requests
import json
import time
import sys


def create_job(job_name, notebook_path, workspace_url, dbr_token, cluster_id):
    request_url = f"{workspace_url}/api/2.0/jobs/create"
    data = {'name': job_name, 'existing_cluster_id': cluster_id,
            'notebook_task': {'notebook_path': notebook_path}}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.post(request_url, data=json.dumps(
        data), auth=("token", dbr_token))
    response_dict = json.loads(response.text)
    return response_dict['job_id']


def delete_job(job_id, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/jobs/delete"
    data = {'job_id': job_id}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.post(request_url, data=json.dumps(
        data), auth=("token", dbr_token))
    response_dict = json.loads(response.text)
    return response_dict


def get_job(job_name, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/jobs/list"
    data = {}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.get(request_url, data=json.dumps(
        data), auth=("token", dbr_token))
    response_dict = json.loads(response.text)

    if not response_dict:
        return False

    jobs = [j['job_id']
            for j in response_dict['jobs'] if j['settings']['name'] == job_name]

    if len(jobs) == 0:
        return False

    return jobs[0]


def get_run_status(run_id, workspace_url, dbr_token):

    waiting = True
    while waiting:
        time.sleep(30)

        request_url = f'{workspace_url}/api/2.1/jobs/runs/get?run_id={run_id}'
        response = requests.get(request_url, auth=("token", dbr_token))
        response.raise_for_status()
        response_dict = json.loads(response.text)

        current_state = response_dict['state']['life_cycle_state']
        result_state = ''

        if current_state in ['TERMINATED']:
            result_state = response_dict['state']['result_state']
            return (result_state in ['SUCCESS'])

        if current_state in ['INTERNAL_ERROR', 'SKIPPED']:
            return False


def run_job(job_id, workspace_url, dbr_token):
    request_url = f"{workspace_url}/api/2.0/jobs/run-now"
    data = {'job_id': job_id}

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.post(request_url, data=json.dumps(
        data), auth=("token", dbr_token))
    response_dict = json.loads(response.text)
    return response_dict['run_id']


def run_job_from_branch(run_name, notebook_path, notebook_params, git_url, git_branch, workspace_url, dbr_token, cluster_id):
    request_url = f"{workspace_url}/api/2.1/jobs/runs/submit"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {dbr_token}"
    }

    data = {
        "run_name": f"{run_name}_for_{git_branch}_branch",
        "git_source": {
            "git_url": f"{git_url}",
            "git_provider": "azureDevOpsServices",
            "git_branch": f"{git_branch}"
        },

        "tasks": [
            {
                "task_key": f"{run_name}",
                "notebook_task": {
                    "notebook_path": f"{notebook_path}",
                    "base_parameters": notebook_params
                },
                "existing_cluster_id": f"{cluster_id}"
            }
        ]
    }

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.post(
        request_url, data=json.dumps(data), headers=headers)
    response.raise_for_status()
    response_dict = json.loads(response.text)
    return response_dict['run_id']


def run_job_from_commit(run_name, notebook_path, notebook_params, git_url, git_commit, workspace_url, dbr_token, cluster_id):
    request_url = f"{workspace_url}/api/2.1/jobs/runs/submit"
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {dbr_token}"
    }

    git_commit_short = git_commit[0:8]
    run_name = f"{run_name}_for_{git_commit_short}_commit"

    data = {
        "run_name": run_name,
        "git_source": {
            "git_url": f"{git_url}",
            "git_provider": "azureDevOpsServices",
            "git_commit": f"{git_commit}"
        },
        "tasks": [
            {
                "task_key": f"{run_name}",
                "notebook_task": {
                    "notebook_path": f"{notebook_path}",
                    "base_parameters": notebook_params
                },
                "existing_cluster_id": f"{cluster_id}"
            }
        ]
    }
    response = requests.post(
        request_url, data=json.dumps(data), headers=headers)

    print(request_url)
    print(data)
    sys.stdout.flush()

    response.raise_for_status()
    response_dict = json.loads(response.text)
    return response_dict['run_id']


def run_job_from_workspace(run_name, notebook_path, notebook_params, workspace_url, dbr_token, cluster_id):
    request_url = f"{workspace_url}/api/2.0/jobs/runs/submit"

    data = {
        'run_name': run_name,
        'existing_cluster_id': cluster_id,
        'notebook_task': {
            "notebook_path": f"{notebook_path}",
            "base_parameters": notebook_params
        }
    }

    print(request_url)
    print(data)
    sys.stdout.flush()

    response = requests.post(
        request_url,
        data=json.dumps(data),
        auth=("token", dbr_token))

    response.raise_for_status()
    response_dict = json.loads(response.text)
    return response_dict['run_id']
