from databricks.functions.shared.dataframe import DataFrame
from databricks.functions.shared.secrets import get_secret
import requests


def send_mail_duplicate_records_found(duplicates_count: int, duplicates_sample: DataFrame, config: dict):
    title = f"""[{config.get('env_name').upper()}] ({config.get('notebook_name')}) - Found ({duplicates_count}) duplicate records"""

    message = f"notebook path: {config.get('notebook_path')}<br>"
    message += 'keys for first 50 duplicates:<br><br>'
    message += duplicates_sample.toPandas().to_html()

    url = get_secret("la-edm-ans-send-mail")
    data = {
        # "recipient": "edmnotifications@ansell.com;024980ad.ansell.com@amer.teams.ms",
        "recipient": "grzegorz.adamski@ansell.com",
        "title": title,
        "message": message
    }
    requests.post(url, json=data)
