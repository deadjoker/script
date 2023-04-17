#!/bin/env python3
# -*- coding: utf-8 -*-
#
# File:        rbd_usage_exporter.py
#
# Description: A utility script that generate multiple fio threads
#              simultaneously to conduct a stress testing, specally
#              to ceph rbd cluster.
#
# Examples:    Use rbd_usage_exporter.py to calculate rbd disk usage
#              and export to prometheus. To use, simply do:
#
#                  rbd_usage_exporter.py --host 0.0.0.0 --port 9280
#
#              get help of this script, add -h or --help param:
#
#                  rbd_usage_exporter.py -h
#
# License:     GPL3
#
# Maintainer:  Zhenshi Zhou(https://github.com/deadjoker)
#
###############################################################################

import os
import threading
import time
import json
import argparse
import subprocess

from flask import Flask


class RBDUsageCollector(object):
    """
    RBDUsageCollector gathers rbd image usage data for all pools with
    rbd_stats_pools enabled and presents it in a format suitable for
    pulling via a Prometheus server.
    NOTE: By default not all rbd images usage can be calculate and the
    pool must be set to collect rbd_stats_pools by
    'ceph config set mgr mgr/prometheus/rbd_stats_pools pool'.
    see Ceph documentation for details.
    """

    def __init__(self, cluster_name, conf, keyring):
        super(RBDUsageCollector, self).__init__()
        self.cluster_name = cluster_name
        self.conf = conf
        self.keyring = keyring


    def collect(self):
        start = time.time()

        # add style to html in order to prettify the web page
        self._write_to_file('<pre style="word-wrap: break-word; white-space: pre-wrap;">')

        # add information of the metrics
        self._write_to_file('# HELP rbd_usage_bytes RBD used space in bytes')
        self._write_to_file('# TYPE rbd_usage_bytes gauge')
        self._write_to_file('# HELP rbd_total_provision_bytes RBD total size in bytes')
        self._write_to_file('# TYPE rbd_total_provision_bytes gauge')
        self._write_to_file('# HELP scrape_duration_seconds Duration of collecting in seconds')
        self._write_to_file('# TYPE scrape_start_time gauge')

        rbd_pools = self._get_pool()
        if rbd_pools:
            for p in rbd_pools:
                print('collect pool %s....' % p)
                rbd_usage = self._get_usage(p)
                self._update_usage_metrics(rbd_usage, p)

        duration = time.time() - start

        self._write_to_file('scrape_duration_seconds {}'.format(duration))
        self._write_to_file('</pre>')

        self._rename_tmp()


    def _get_pool(self):
        """
        The pools from which we want to get usage of rbd.
        Only pools with rbd_stats_pools enabled will be collected.
        :return: a list of pools
        """

        cmd = ["ceph", "-c", self.conf, "--cluster", self.cluster_name,
               "config", "get", "mgr", "mgr/prometheus/rbd_stats_pools"]

        output = subprocess.check_output(cmd).decode("utf-8")
        if output:
            pools = output.strip('\n').split(',')
            return pools
        else:
            return []


    def _get_usage(self, pool):
        """
        get all usage and total size provisioned of rbd in bytes.
        if image has object_map feature, calculate by its object_map,
        else use 'rbd du image'.
        :param pool: str, rbd in this pool will be calculated usage.
        :return: a list of rbd usage, for example:
        [{'name':'abc', 'id': '12345', 'provisioned_size: 1000, 'used_size': 500}]
        """

        image_info_list = []

        cmd = "rbd -c " + self.conf + " -k " + self.keyring + " du -p " + pool + " --format json"
        try:
            res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            info = res.stdout.read().decode("utf-8")
            info = json.loads(info)

            if len(info['images']) > 0:
                image_info_list += info['images']
        except Exception as e:
            print(e)

        return image_info_list


    def _update_usage_metrics(self, usage_list, pool):
        """
        Update promethes metrics with rbd usage data into file.
        :param usage_list: a list of all rbd usage.
        :param pool: update data of rbd from this pool.
        """

        for data in usage_list:
            self._write_to_file('''rbd_usage_bytes{{image="{}",pool="{}",id="{}"}} {}'''.format(
                data['name'], pool, data['id'], data['used_size']))

            self._write_to_file('''rbd_total_provision_bytes{{image="{}",pool="{}",id="{}"}} {}'''.format(
                data['name'], pool, data['id'], data['provisioned_size']))


    def _write_to_file(self, data):
        """
        write data to a temporary file called '/tmp/rbd_usage.prom.tmp'.
        :param data: a line data to write to file.
        """
        file = '/tmp/rbd_usage.prom.tmp'
        try:
            with open(file, 'a') as f:
                f.write(data + "\n")
        except Exception as e:
            print(e)


    def _rename_tmp(self):
        """
        rename 'rbd_usage.prom.tmp' to 'rbd_usage.prom' for flask to read.
        we need a temporary file for writing, and a complete file for reading,
        because open(file, 'w') will cover the former data in the file,
        while open(file, 'a') will result in multiple duplicated metrics
        with different values.
        """
        file_write_into = '/tmp/rbd_usage.prom.tmp'
        file_read_from = '/tmp/rbd_usage.prom'

        try:
            os.rename(file_write_into, file_read_from)
        except Exception as e:
            print(e)


    def remove_tmp_if_exist(self):
        """
        remove the temporary file before starting.
        only run on start.
        """

        file = '/tmp/rbd_usage.prom.tmp'
        if os.path.exists(file):
            os.remove(file)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Ceph config file and keyring as well as local binding port.'
    )
    parser.add_argument(
        '-c', '--conf',
        required=False,
        help='path to cluster configuration.',
        default=os.environ.get('CEPH_CONF', '/etc/ceph/ceph.conf')
    )
    parser.add_argument(
        '--cluster',
        required=False,
        help='cluster name',
        default=os.environ.get('CLUSTER_NAME', 'ceph'),
    )
    parser.add_argument(
        '-k', '--keyring',
        required=False,
        help='path to keyring',
        default=os.environ.get('CEPH_KEYRING', '/etc/ceph/ceph.client.admin.keyring')
    )
    parser.add_argument(
        '-H', '--host',
        required=False,
        help='ip address for the exporter to serve',
        default=os.environ.get('RBD_EXPORTER_SERVER', '0.0.0.0')
    )
    parser.add_argument(
        '-p', '--port',
        required=False,
        help='Port for the exporter to listen',
        default=int(os.environ.get('RBD_EXPORTER_PORT', '9280'))
    )

    return parser.parse_args()


def collect_metrics():
    args = parse_args()
    collector = RBDUsageCollector(args.cluster, args.conf, args.keyring)
    collector.remove_tmp_if_exist()

    while True:
        try:
            collector.collect()
        except Exception as e:
            print(e)


def flask_out():
    app = Flask(__name__)

    @app.route("/metrics", methods=['GET'])
    def getfile():
        file = '/tmp/rbd_usage.prom'

        if not os.path.exists(file):
            with open(file, "w") as f:
                f.write('')

        with open(file, "r") as f:
            data = f.read()
            return data

    args = parse_args()
    app.run(host=args.host, port=args.port)



if __name__ == "__main__":
    t_collect = threading.Thread(target=collect_metrics)
    t_flask = threading.Thread(target=flask_out)

    t_collect.start()
    t_flask.start()
