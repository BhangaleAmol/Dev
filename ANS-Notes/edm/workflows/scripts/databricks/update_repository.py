import argparse
import sys
import modules.repository as repo


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace_url", required=True)
    parser.add_argument("--repository_id", required=True)
    parser.add_argument("--dbr_token", required=True)
    parser.add_argument("--git_branch", default="master")
    return parser.parse_args()


def main():
    args = parse_arguments()

    response = repo.update_repository(
        args.repository_id, args.git_branch, args.workspace_url, args.dbr_token)
    print(response)
    sys.stdout.flush()


if __name__ == '__main__':
    main()
