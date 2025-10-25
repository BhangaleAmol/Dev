import argparse
import sys
import re
import modules.jobs as jobs
import modules.workspace as workspace
import modules.table as table


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace_url")
    parser.add_argument("--cluster_id")
    parser.add_argument("--dbr_token")
    parser.add_argument(
        "--folder_path", default="/Repos/edm/repo/databricks/releases")
    parser.add_argument("--storage_name")
    parser.add_argument("--storage_key")
    parser.add_argument("--table_name", default="edmreleases")
    return parser.parse_args()


def get_release_scripts_from_workspace(folder_path, workspace_url, dbr_token):
    response = workspace.list_files(folder_path, workspace_url, dbr_token)
    result = [
        (object['path'].split('/')[-1], object['path'])
        for object in response['objects']
        if object['path'].split('/')[-1][0:3] == 'edm'
    ]
    return result


def get_release_scripts_from_config(storage_name, storage_key, table_name, run_result='true'):
    rows = table.run_query(
        storage_name, storage_key, table_name,
        f'run_result eq {run_result}', 'run_name')
    return [row.run_name for row in rows]


def get_release_scripts_for_execution(storage_name, storage_key, table_name, folder_path, workspace_url, dbr_token):
    release_scripts_cfg = get_release_scripts_from_config(
        storage_name, storage_key, table_name)
    release_scripts_ws = get_release_scripts_from_workspace(
        folder_path, workspace_url, dbr_token)
    return [s for s in release_scripts_ws if s[0] not in release_scripts_cfg]


def save_release_record(script_name, run_result, storage_name, storage_key, table_name):
    row = {'PartitionKey': 'dbr', 'RowKey': script_name,
           'run_name': script_name, 'run_result': run_result}
    table.insert_or_replace(row, storage_name, storage_key, table_name)


def sort_release_scripts(release_scripts):

    def convert_to_int(text):
        return int(text) if text.isdigit() else text

    def natural_keys(script_name):
        return [convert_to_int(c) for c in re.split(r'(\d+)', script_name)]

    return sorted(release_scripts, key=lambda x: natural_keys(x[0]))


def print_release_scripts(release_scripts):
    print('\nScripts to be executed:\n')
    print("\n".join([s[0] + " (" + s[1] + ")" for s in release_scripts]))
    print("")
    sys.stdout.flush()


def execute_release_scripts(release_scripts, workspace_url, dbr_token, cluster_id, storage_name, storage_key, table_name):

    throw_exception = False
    for script_name, script_path in release_scripts:
        try:
            job_id = jobs.get_job(script_name, workspace_url, dbr_token)
            if not job_id:
                job_id = jobs.create_job(
                    script_name, script_path, workspace_url, dbr_token, cluster_id)

            run_id = jobs.run_job(job_id, workspace_url, dbr_token)
            print(
                f'\nScript: {script_name} (job_id: {job_id}, run_id: {run_id})')
            sys.stdout.flush()

            run_successful = jobs.get_run_status(
                run_id, workspace_url, dbr_token)
            print(f'\n(result: {str(run_successful)})')
            sys.stdout.flush()

            if run_successful:
                jobs.delete_job(job_id, workspace_url, dbr_token)
            else:
                throw_exception = True

            save_release_record(script_name, run_successful,
                                storage_name, storage_key, table_name)

        except Exception as e:
            print(e)
            sys.stdout.flush()
            throw_exception = True

    if throw_exception:
        raise Exception('script failure')


def main():

    args = parse_arguments()

    release_scripts = get_release_scripts_for_execution(
        args.storage_name, args.storage_key, args.table_name,
        args.folder_path, args.workspace_url, args.dbr_token)

    if not release_scripts:
        print('Release script list is empty.')
        sys.stdout.flush()
        sys.exit()

    release_scripts = sort_release_scripts(release_scripts)
    print_release_scripts(release_scripts)
    execute_release_scripts(
        release_scripts,
        args.workspace_url, args.dbr_token, args.cluster_id,
        args.storage_name, args.storage_key, args.table_name)


if __name__ == '__main__':
    main()
