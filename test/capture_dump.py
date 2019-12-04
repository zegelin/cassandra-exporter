# spin up a multi-node CCM cluster with cassandra-exporter installed, apply a schema, and capture the metrics output

import argparse
import contextlib
import os
import tempfile
import time
import urllib.request
from pathlib import Path

import cassandra.cluster
import cassandra.connection


from utils.ccm import TestCluster
from utils.jar_utils import ExporterJar
from utils.path_utils import existing_file_arg
from utils.schema import CqlSchema


def cluster_directory(path):
    path = Path(path)

    if path.exists():
        if not path.is_dir():
            raise argparse.ArgumentTypeError(f'"{path}" must be a directory.')

        if next(path.iterdir(), None) is not None:
            raise argparse.ArgumentTypeError(f'"{path}" must be an empty directory.')

    return path


def output_directory(path):
    path = Path(path)

    if path.exists():
        if not path.is_dir():
            raise argparse.ArgumentTypeError(f'"{path}" must be a directory.')

        # the empty directory check is done later, since it can be skipped with --overwrite-output

    return path




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cassandra_version', type=str, help="version of Cassandra to run", metavar="CASSANDRA_VERSION")
    parser.add_argument('output_directory', type=output_directory, help="location to write metrics dumps", metavar="OUTPUT_DIRECTORY")

    parser.add_argument('-o', '--overwrite-output', action='store_true', help="don't abort when the output directory exists or is not empty")

    parser.add_argument('--cluster-directory', type=cluster_directory, help="location to install Cassandra. Must be empty or not exist. (default is a temporary directory)")
    parser.add_argument('--keep-cluster-directory', type=bool, help="don't delete the cluster directory on exit")
    parser.add_argument('--keep-cluster-running', type=bool, help="don't stop the cluster on exit (implies --keep-cluster-directory)")

    parser.add_argument('-d', '--datacenters', type=int, help="number of data centers (default: %(default)s)", default=2)
    parser.add_argument('-r', '--racks', type=int, help="number of racks per data center (default: %(default)s)", default=3)
    parser.add_argument('-n', '--nodes', type=int, help="number of nodes (default: %(default)s)", default=6)

    parser.add_argument('-j', '--exporter-jar', type=ExporterJar.from_path, help="location of the cassandra-exporter jar, either agent or standalone (default: %(default)s)", default=str(ExporterJar.default_jar_path()))
    parser.add_argument('-s', '--schema', type=CqlSchema.from_path, help="CQL schema to apply (default: %(default)s)", default=str(CqlSchema.default_schema_path()))

    args = parser.parse_args()

    if args.cluster_directory is None:
        args.cluster_directory = Path(tempfile.mkdtemp()) / "test-cluster"

    if args.output_directory.exists() and not args.overwrite_output:
        if next(args.output_directory.iterdir(), None) is not None:
            raise argparse.ArgumentTypeError(f'"{args.output_directory}" must be an empty directory.')

    os.makedirs(args.output_directory, exist_ok=True)

    with contextlib.ExitStack() as defer:
        ccm_cluster = defer.push(TestCluster(
            args.cluster_directory, args.cassandra_version
        ))

        print('Starting cluster...')
        ccm_cluster.start()

        print('Applying schema...')
        ccm_cluster.apply_schema()

        print('Connecting to cluster...')
        contact_points = map(lambda n: cassandra.connection.DefaultEndPoint(*n.network_interfaces['binary']), ccm_cluster.nodelist())

        cql_cluster = cassandra.cluster.Cluster(list(contact_points))
        with cql_cluster.connect() as cql_session:
            print('Applying schema...')
            for stmt in args.schema:
                print('Executing "{}"...'.format(stmt.split('\n')[0]))
                cql_session.execute(stmt)

        # the collector defers registrations by a second or two.
        # See com.zegelin.cassandra.exporter.Harvester.defer()
        print('Pausing to wait for deferred MBean registrations to complete...')
        time.sleep(5)

        print('Capturing metrics dump...')
        for node in ccm_cluster.nodelist():
            url = f'http://{node.ip_addr}:{node.exporter_port}/metrics?x-accept=text/plain'
            destination = args.output_directory / f'{node.name}.txt'
            urllib.request.urlretrieve(url, destination)

            print(f'Wrote {url} to {destination}')


