# this end-to-end test does the following:
# 1. download Prometheus (for the current platform)
# 2. setup a multi-node Cassandra cluster with the exporter installed
# 3. configure Prometheus it to scrape from the Cassandra nodes
# 4. verifies that Prometheus successfully scrapes the metrics
# 5. cleans up everything
import argparse
import contextlib
import http.server
import itertools
import json
import platform
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
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

import cassandra.connection
from tqdm import tqdm

from utils.ccm import create_ccm_cluster, TestCluster
from utils.jar_utils import ExporterJar
from utils.path_utils import nonexistent_or_empty_directory_arg

import yaml

from utils.prometheus import PrometheusInstance


class DummyPrometheusHTTPHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/metrics':
            self.send_error(404)

        self.send_response(200)
        self.end_headers()

        self.wfile.write(b'# TYPE test_family gauge\n'
                         b'test_family 123\n')



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cassandra_version', type=str, help="version of Cassandra to run", metavar="CASSANDRA_VERSION")

    parser.add_argument('--working-directory', type=nonexistent_or_empty_directory_arg, help="location to install Cassandra and Prometheus. Must be empty or not exist. (default is a temporary directory)")
    parser.add_argument('--keep-working-directory', help="don't delete the cluster directory on exit", action='store_true')

    parser.add_argument('-d', '--datacenters', type=int, help="number of data centers (default: %(default)s)", default=2)
    parser.add_argument('-r', '--racks', type=int, help="number of racks per data center (default: %(default)s)", default=3)
    parser.add_argument('-n', '--nodes', type=int, help="number of nodes per data center rack (default: %(default)s)", default=1)

    parser.add_argument('-j', '--exporter-jar', type=ExporterJar.from_path, help="location of the cassandra-exporter jar, either agent or standalone (default: %(default)s)", default=str(ExporterJar.default_jar_path()))
    # parser.add_argument('-s', '--schema', type=schema_yaml, help="CQL schema to apply (default: %(default)s)", default=str(default_schema_path()))

    parser.add_argument('-x', '--prometheus-archive-url', type=str, help='Prometheus binary release archive URL (default: %(default)s)', default=PrometheusInstance.default_prometheus_archive_url())

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
            prometheus_archive_url=args.prometheus_archive_url,
            base_directory=args.working_directory / 'prometheus'
        ))

        # print('Setting up Cassandra...')
        # ccm_cluster = defer.push(TestCluster(
        #     cluster_directory=args.working_directory / 'test-cluster',
        #     cassandra_version=args.cassandra_version,
        #     node_count=1,
        #     delete_cluster_on_stop=not args.keep_working_directory
        # ))


        httpd = http.server.HTTPServer(("", 9500), DummyPrometheusHTTPHandler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()

        httpd = http.server.HTTPServer(("", 9501), DummyPrometheusHTTPHandler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()

        # prometheus.set_scrape_config('cassandra', list(map(lambda n: f'localhost:{n.exporter_port}', ccm_cluster.nodelist())))
        prometheus.set_scrape_config('cassandra', ['localhost:9500', 'localhost:9501'])
        prometheus.start()





        #print('Starting cluster...')
        # ccm_cluster.start()

        while True:
            targets = prometheus.get_targets()

            for target in targets['activeTargets']:

            break


        for _ in tqdm(itertools.count()):
            targets = prometheus.get_targets()

            cassandra_target = None

            for target in targets['activeTargets']:
                if target['labels']['job'] == 'cassandra':
                    cassandra_target = target
                    break

            if cassandra_target is not None:
                if cassandra_target['health'] == 'up':
                    break

            time.sleep(1)


        data = prometheus.query('test_family')
        pass






