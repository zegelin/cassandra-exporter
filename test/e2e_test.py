# this end-to-end test does the following:
# 1. download Prometheus (for the current platform)
# 2. setup a multi-node Cassandra cluster with the exporter installed
# 3. configure Prometheus to scrape from the Cassandra nodes
# 4. verifies that Prometheus successfully scrapes the metrics
# 5. cleans up everything

import argparse
import contextlib
import http.server
import itertools
import json
import platform
import random
import re
import shutil
import socketserver
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.request
import urllib.error
from collections import namedtuple, defaultdict
from pathlib import Path
from types import SimpleNamespace

import cassandra.connection
from frozendict import frozendict

from utils.ccm import create_ccm_cluster, TestCluster
from utils.jar_utils import ExporterJar
from utils.path_utils import nonexistent_or_empty_directory_arg

import yaml

from utils.prometheus import PrometheusInstance, PrometheusArchive
from utils.schema import CqlSchema


class DummyPrometheusHTTPHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/metrics':
            self.send_error(404)

        self.send_response(200)
        self.end_headers()

        #if self.server.server_port == 9500:
        if random.choice([True, False]):
            self.wfile.write(b'# TYPE test_family gauge\n'
                             b'test_family 123\n')

        else:
            self.wfile.write(b'# TYPE test_family gauge\n'
                             b'test_family123\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cassandra_version', type=str, help="version of Cassandra to run", metavar="CASSANDRA_VERSION")

    parser.add_argument('-C', '--working-directory', type=nonexistent_or_empty_directory_arg, help="location to install Cassandra and Prometheus. Must be empty or not exist. (default is a temporary directory)")
    parser.add_argument('--keep-working-directory', help="don't delete the cluster directory on exit", action='store_true')

    parser.add_argument('-d', '--datacenters', type=int, help="number of data centers (default: %(default)s)", default=2)
    parser.add_argument('-r', '--racks', type=int, help="number of racks per data center (default: %(default)s)", default=3)
    parser.add_argument('-n', '--nodes', type=int, help="number of nodes per data center rack (default: %(default)s)", default=3)

    ExporterJar.add_jar_argument('--exporter-jar', parser)
    CqlSchema.add_schema_argument('--schema', parser)
    PrometheusArchive.add_archive_argument('--prometheus-archive', parser)

    args = parser.parse_args()

    if args.working_directory is None:
        args.working_directory = Path(tempfile.mkdtemp())

    def delete_working_dir():
        shutil.rmtree(args.working_directory)

    with contextlib.ExitStack() as defer:
        if not args.keep_working_directory:
            defer.callback(delete_working_dir)  # LIFO order -- this gets called last

        print('Setting up Prometheus...')
        prometheus = defer.push(PrometheusInstance(
            archive=args.prometheus_archive,
            working_directory=args.working_directory
        ))

        print('Setting up Cassandra...')
        ccm_cluster = defer.push(TestCluster(
            cluster_directory=args.working_directory / 'test-cluster',
            cassandra_version=args.cassandra_version,
            exporter_jar=args.exporter_jar,
            nodes=args.nodes, racks=args.racks, datacenters=args.datacenters,
            delete_cluster_on_stop=not args.keep_working_directory,
        ))


        # httpd = http.server.HTTPServer(("", 9500), DummyPrometheusHTTPHandler)
        # threading.Thread(target=httpd.serve_forever, daemon=True).start()
        #
        # httpd = http.server.HTTPServer(("", 9501), DummyPrometheusHTTPHandler)
        # threading.Thread(target=httpd.serve_forever, daemon=True).start()

        prometheus.set_scrape_config('cassandra', list(map(lambda n: f'localhost:{n.exporter_port}', ccm_cluster.nodelist())))
        # prometheus.set_scrape_config('cassandra', ['localhost:9500', 'localhost:9501'])
        prometheus.start()





        print('Starting cluster...')
        ccm_cluster.start()

        print('Pausing to wait for deferred MBean registrations to complete...')
        time.sleep(5)


        target_histories = defaultdict(dict)

        while True:
            targets = prometheus.get_targets()

            if len(targets['activeTargets']) > 0:
                for target in targets['activeTargets']:
                    labels = frozendict(target['labels'])

                    # even if the target health is unknown, ensure the key exists so the length check below
                    # is aware of the target
                    history = target_histories[labels]

                    if target['health'] == 'unknown':
                        continue

                    history[target['lastScrape']] = (target['health'], target['lastError'])

                if all([len(v) >= 5 for v in target_histories.values()]):
                    break

            time.sleep(1)

        x = dict((target, history) for target, history in target_histories.items()
             if any([health != 'up' for (health, error) in history.values()]))

        if len(x):
            print(x)







