import os
from requests import get
from re import sub
import getopt
import sys


def get_external_ip():
    text = get('http://checkip.dyndns.com').text
    return sub("[^\d\.]", "", text)


def set_storage_firewall_rule(resource_group, storage_name, ip):
    cmd = "az storage account network-rule add -g {0} --account-name {1} --ip-address {2}/24".format(
        resource_group, storage_name, ip)
    print(cmd)
    os.system('cmd /k "{0}"'.format(cmd))


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'rg:s',
                                   ['resource_group=', 'storage_name='])
    except getopt.GetoptError:
        print(
            'deploy_config_table.py -rg <resource_group> -s <storage_name>)')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-rg', '--resource_group'):
            resource_group = arg
        elif opt in ('-s', '--storage_name'):
            storage_name = arg


ip = get_external_ip()
set_storage_firewall_rule(resource_group, storage_name, ip)
