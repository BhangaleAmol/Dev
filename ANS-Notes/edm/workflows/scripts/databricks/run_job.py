import json
import argparse
import sys
import modules.jobs as jobs


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace_url", required=True)
    parser.add_argument("--dbr_token", required=True)
    parser.add_argument("--cluster_id", required=True)
    parser.add_argument("--run_name", required=True)
    parser.add_argument("--git_url")
    parser.add_argument("--git_branch")
    parser.add_argument("--git_commit")
    parser.add_argument("--notebook_params", default="{}")
    parser.add_argument("--notebook_path", required=True)
    parser.add_argument("--notebook_source", default="workspace")
    return parser.parse_args()


def main():
    try:
        args = parse_arguments()

        notebook_params = json.loads(args.notebook_params)

        if args.notebook_source == 'workspace':
            run_id = jobs.run_job_from_workspace(
                args.run_name,
                args.notebook_path,
                notebook_params,
                args.workspace_url,
                args.dbr_token,
                args.cluster_id)

        if args.notebook_source == 'git' and args.git_branch is not None:
            run_id = jobs.run_job_from_branch(
                args.run_name,
                args.notebook_path,
                notebook_params,
                args.git_url,
                args.git_branch,
                args.workspace_url,
                args.dbr_token,
                args.cluster_id)

        if args.notebook_source == 'git' and args.git_commit is not None:
            run_id = jobs.run_job_from_commit(
                args.run_name,
                args.notebook_path,
                notebook_params,
                args.git_url,
                args.git_commit,
                args.workspace_url,
                args.dbr_token,
                args.cluster_id)

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
