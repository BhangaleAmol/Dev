# Databricks notebook source
def get_secret(secret_name):
  envName = os.getenv('ENV_NAME')
  secretScope = 'edm-ans-{}-dbr-scope'.format(envName)
  return dbutils.secrets.get(scope = secretScope, key = secret_name)

def send_mail(data, url):
  response = requests.post(url, json = data)
  print(response)
