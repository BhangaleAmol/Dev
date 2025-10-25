import argparse
import sys
import modules.repository as repo


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace_url", required=True)
    parser.add_argument("--dbr_token", required=True)
    parser.add_argument("--repository_path", required=True)
    return parser.parse_args()


def main():

    args = parse_arguments()

    repository_id = repo.get_repository_id(
        args.repository_path, args.workspace_url, args.dbr_token)
    print(f'repository_id: {repository_id}')
    sys.stdout.flush()

    if repository_id is not None:
        response = repo.remove_repository(
            repository_id, args.workspace_url, args.dbr_token)
        print(response)
        sys.stdout.flush()


if __name__ == '__main__':
    main()
