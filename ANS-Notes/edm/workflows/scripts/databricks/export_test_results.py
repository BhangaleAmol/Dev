import argparse
import modules.workspace as workspace


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace_url", required=True)
    parser.add_argument("--dbr_token", required=True)
    parser.add_argument("--file_path")
    return parser.parse_args()


def main():
    try:
        args = parse_arguments()

        xml_content = workspace.export_file(
            args.file_path, args.workspace_url, args.dbr_token)
        file = open("report.xml", "a")
        file.write(xml_content)
        file.close()
    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    main()
