import json
import argparse
import sys
import re
import modules.jobs as jobs


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace_url", required=True)
    parser.add_argument("--dbr_token", required=True)
    parser.add_argument("--cluster_id", required=True)
    parser.add_argument("--run_name", required=True)
    parser.add_argument("--notebook_params", default="{}")
    parser.add_argument("--notebook_path")
    return parser.parse_args()


def main():
    try:
        args = parse_arguments()

        notebook_params = json.loads(args.notebook_params)
        run_id = jobs.run_job_from_workspace(
            args.run_name,
            args.notebook_path, notebook_params,
            args.workspace_url, args.dbr_token, args.cluster_id)

        print(f'run_id: {run_id}')
        sys.stdout.flush()
        run_successful = jobs.get_run_status(
            run_id, args.workspace_url, args.dbr_token)

    except Exception as e:
        raise Exception(e)

    if not run_successful:
        raise Exception(f'run_id {run_id} failed')


if __name__ == '__main__':
    main()
