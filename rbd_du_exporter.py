#!/bin/env python3
# -*- coding: utf-8 -*-
#
# File:        rbd_du_exporter.py
#
# Description: A utility script that generate multiple fio threads
#              simultaneously to conduct a stress testing, specally
#              to ceph rbd cluster.
#
# Examples:    Use rbd_du_exporter.py to calculate rbd disk usage
#              and export to promethues. To use, simply do:
#
#                  rbd_du_exporter.py --host 0.0.0.0 --port 9280
#
#              get help of this script, add -h or --help param:
#
#                  rbd_du_exporter.py -h
#
# License:     GPL3
#
# Maintainer:  Zhenshi Zhou
#
###############################################################################

import os
import time
import json
import argparse
import rbd
import rados
import math
import subprocess

from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY


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
        # setup empty prometheus metrics
        self._setup_empty_prometheus_metrics()

        rbd_pools = self._get_pool()
        if rbd_pools:
            for p in rbd_pools:
                print('collect pool %s....' % p)
                rbd_usage = self._get_usage(p)
                self._update_usage_metrics(rbd_usage, p)

        duration = time.time() - start
        self._prometheus_metrics['scrape_duration_seconds'].add_metric(
            [], duration)

        for metric in list(self._prometheus_metrics.values()):
            yield metric



    def _setup_empty_prometheus_metrics(self):
        """
        The metrics we want to export.
        """

        self._prometheus_metrics = {
            'disk_usage':
                GaugeMetricFamily('rbd_usage_bytes',
                                  'RBD used space in bytes',
                                  labels=["image", "pool"]),
            'total_provision':
                GaugeMetricFamily('rbd_total_provision_bytes',
                                  'RBD total size bytes provisioned',
                                  labels=["image", "pool"]),
            'scrape_duration_seconds':
                GaugeMetricFamily('rbd_usage_scrape_duration_seconds',
                                  'Ammount of time each scrape takes',
                                  labels=[])
    }


    def _get_pool(self):
        """
        The pools from which we want to get usage of rbd.
        Only pools with rbd_stats_pools enabled will be collected.
        :return: a list of pools
        """

        # rbd_pools = []
        # with rados.Rados(conffile='/etc/ceph/ceph.conf') as cluster:
        #    pools = cluster.list_pools()
        #    for p in pools:
        #        with cluster.open_ioctx(p) as ioctx:
        #            if 'rbd' in ioctx.application_list():
        #                rbd_pools.append(p)

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
        with rados.Rados(conffile='/etc/ceph/ceph.conf') as cluster:
            with cluster.open_ioctx(pool) as ioctx:
                rbd_inst = rbd.RBD()
                image_list = rbd_inst.list(ioctx)

                for image_name in image_list:
                    try:
                        with rbd.Image(ioctx, image_name) as image:
                            if self._has_object_map_feature(image):
                                print('%s/%s usage calculate by object-map.' % (pool, image_name))
                                image_info = {}
                                image_id = image.id()
                                image_stat = image.stat()
                                image_size = image_stat['size']
                                num_objs = image_stat['num_objs']
                                obj_size = image_stat['obj_size']

                                allocated = 0
                                for i in range(1, num_objs):
                                    obj_name = 'rbd_object_map.' + image_id
                                    v = (ioctx.read(obj_name, 1, math.floor(i / 4))[0]
                                         >> (6 - 2 * (i % 4))) & 0x3
                                    if v == 1 or v == 2:
                                        allocated += 1
                                image_usage = allocated * obj_size

                                image_info['name'] = image_name
                                image_info['id'] = image_id
                                image_info['provisioned_size'] = image_size
                                image_info['used_size'] = image_usage

                                image_info_list.append(image_info)
                            else:
                                print('%s/%s has no object_map feature.' % (pool, image_name))
                                cmd = ["rbd", "-c", self.conf, "-k", self.keyring, "du", "-p", pool,
                                       image_name, "--format", "json"]

                                try:
                                    output = subprocess.check_output(cmd, timeout=300).decode("utf-8")
                                    output = json.loads(output)

                                    if 'images' in output.keys():
                                        image_info_list += output['images']
                                except Exception as e:
                                    print(e)
                    except Exception as e:
                        print(e)
                        continue

        return image_info_list


    def _update_usage_metrics(self, usage_list, pool):
        """
        Update promethes metrics with rbd usage data.
        :param usage_list: a list of all rbd usage.
        :param pool: update data of rbd from this pool.
        """

        for data in usage_list:
            self._prometheus_metrics['disk_usage'].add_metric(
                [data['name'], pool], data['used_size']
            )

            self._prometheus_metrics['total_provision'].add_metric(
                [data['name'], pool], data['provisioned_size']
            )


    def _has_object_map_feature(self, image):
        """
        check if image has object_map feature
        | mask | feature |
        | --- | ------ |
        | 1 | layering |
        | 2 | striping |
        | 4 | exclusive-lock |
        | 8 | object-map |
        | 16 | fast-diff |
        | 32 | deep-flatten |
        | 64 | journaling |
        | 128 | data-pool |
        | 256 | fast-diff-object-map |
        :param image: rbd.Image
        :return: bool
        """

        features = image.features()
        object_map_feature = 8

        return (features & object_map_feature ) == object_map_feature


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


def main():
    try:
        args = parse_args()
        REGISTRY.register(RBDUsageCollector(
            args.cluster, args.conf, args.keyring))

        start_http_server(args.port)
        print(("Polling {0}. Serving at port: {1}".format(args.host, args.port)))

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nInterrupted")
        exit(0)

if __name__ == "__main__":
    main()
