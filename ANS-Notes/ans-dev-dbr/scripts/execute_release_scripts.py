# execute_release_scripts.py
#!/usr/bin/python3
import json
import requests
import os
import sys
import getopt
import time
import re

from azure.cosmosdb.table.tableservice import TableService


def get_release_scripts(shard, token, workspace_path):

    url = shard + '/api/2.0/workspace/list'
    values = {'path': workspace_path}
    resp = requests.get(
        url, data=json.dumps(values), auth=("token", token))

    respdict = json.loads(resp.text)
    result = [(object['path'].split('/')[-1], object['path'])
              for object in respdict['objects']]

    return result

def create_release_job(shard, token, cluster_id, job_name, path):
    url = shard + '/api/2.0/jobs/create'
    values = {'name': job_name, 'existing_cluster_id': cluster_id,
              'notebook_task': {'notebook_path': path}}

    resp = requests.post(url, data=json.dumps(
        values), auth=("token", token))
    respdict = json.loads(resp.text)

    return respdict['job_id']

def get_release_job(shard, token, name):
    url = shard + '/api/2.0/jobs/list'
    values = {}

    resp = requests.get(url, data=json.dumps(
        values), auth=("token", token))
    respdict = json.loads(resp.text)

    if not respdict:
        return False 

    jobs = [j['job_id'] for j in respdict['jobs'] if j['settings']['name'] == name]

    if len(jobs) == 0:
        return False

    return jobs[0]

def run_release_job(shard, token, job_id):
    url = shard + '/api/2.0/jobs/run-now'
    values = {'job_id': job_id}

    resp = requests.post(url, data=json.dumps(
        values), auth=("token", token))
    respdict = json.loads(resp.text)

    return respdict['run_id']

def delete_release_job(shard, token, job_id):
    url = shard + '/api/2.0/jobs/delete'
    values = {'job_id': job_id}

    resp = requests.post(url, data=json.dumps(
        values), auth=("token", token))
    respdict = json.loads(resp.text)

    return respdict

def run_release_script(shard, token, cluster_id, run_name, path):

    url = shard + '/api/2.0/jobs/runs/submit'
    values = {'run_name': run_name, 'existing_cluster_id': cluster_id,
              'notebook_task': {'notebook_path': path}}

    resp = requests.post(url, data=json.dumps(
        values), auth=("token", token))
    respdict = json.loads(resp.text)

    return respdict['run_id']


def get_run_status(shard, token, run_id):

    waiting = True
    while waiting:
        time.sleep(30)

        url = shard + '/api/2.0/jobs/runs/get?run_id=' + str(run_id)
        resp = requests.get(url, auth=("token", token))

        respdict = json.loads(resp.text)
        current_state = respdict['state']['life_cycle_state']
        result_state = ''

        if current_state in ['TERMINATED']:
            result_state = respdict['state']['result_state']

        if current_state in ['INTERNAL_ERROR', 'SKIPPED']:
            return False

        if current_state in ['TERMINATED']:
            return (result_state in ['SUCCESS'])


def save_release_record(account_name, account_key, table_name, run_name, run_result):

    row = {'PartitionKey': 'dbr', 'RowKey': run_name,
           'run_name': run_name, 'run_result': run_result}

    table_service = TableService(
        account_name=account_name, account_key=account_key)
    table_service.insert_or_replace_entity(table_name, row)


def get_prev_script_names(account_name, account_key, table_name):

    table_service = TableService(
        account_name=account_name, account_key=account_key)
    rows = table_service.query_entities(
        table_name, filter="run_result eq true", select='run_name')

    releases = [row.run_name for row in rows]
    return releases

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    return [ atoi(c) for c in re.split(r'(\d+)', text) ]

def main():
    shard = ''
    token = ''
    cluster_id = ''
    workspace_path = ''
    storage_name = ''
    storage_key = ''
    table_name = ''

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'sh:t:c:w:st:sk:tb',
                                   ['shard=', 'token=', 'cluster_id=', 'workspace_path=', 'storage_name=', 'storage_key=', 'table_name='])
    except getopt.GetoptError:
        sys.stdout.write(
            'execute_release_scripts.py -sh <shard> -t <token>  -c <cluster_id> -w <workspace_path> -st <storage_name> -sk <storage_key> -tb <table_name>)')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-sh', '--shard'):
            shard = arg
        elif opt in ('-t', '--token'):
            token = arg
        elif opt in ('-c', '--cluster_id'):
            cluster_id = arg
        elif opt in ('-w', '--workspace_path'):
            workspace_path = arg
        elif opt in ('-st', '--storage_name'):
            storage_name = arg
        elif opt in ('-sk', '--storage_key'):
            storage_key = arg
        elif opt in ('-tb', '--table_name'):
            table_name = arg

    # get scope of release
    prev_script_names = get_prev_script_names(
        storage_name, storage_key, table_name)
    release_scripts = get_release_scripts(shard, token, workspace_path)

    fin_release_scripts = [
        script for script in release_scripts if script[0] not in prev_script_names]   

    if not fin_release_scripts:
        print('Release script list is empty.')
        sys.stdout.flush()
    else:
        fin_release_scripts = sorted(fin_release_scripts, key=lambda x: natural_keys(x[0]))
        print('\nScripts to be executed:\n')
        print("\n".join([s[0] + " (" + s[1] + ")" for s in fin_release_scripts]))
        print("")
        sys.stdout.flush()

    # run scripts        

    throw_exception = False
    for script_name, script_path in fin_release_scripts:

        try:

            job_id = get_release_job(shard, token, script_name)    
            if not job_id:
                job_id = create_release_job(shard, token, cluster_id, script_name, script_path)

            run_id = run_release_job(shard, token, job_id)

            print(f'\nScript: {script_name} (job_id: {job_id}, run_id: {run_id})')
            sys.stdout.flush()

            run_successful = get_run_status(shard, token, run_id)

            print(f'\nScript: {script_name} (result: {str(run_successful)}')
            sys.stdout.flush()
            
            if run_successful:
                delete_release_job(shard, token, job_id)            

            save_release_record(storage_name, storage_key,
                                table_name, script_name, run_successful)

            if not run_successful:
                throw_exception = True

        except Exception as e:
            print(e)
            sys.stdout.flush()
            throw_exception = True

    if throw_exception:
        raise Exception('script failure')


if __name__ == '__main__':
    main()