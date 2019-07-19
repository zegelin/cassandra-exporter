# spin up a multi-node CCM cluster with cassandra-exporter installed, apply a schema, and capture the metrics output

import argparse
import re
import shutil
import subprocess
import time
import zipfile

import ccmlib.cluster
import ccmlib.node
import os
from enum import Enum, auto
from pathlib import Path
import contextlib
import urllib.request
import cassandra.cluster
import cassandra.connection
import tempfile
import xml.etree.ElementTree as ElementTree
from collections import namedtuple

import yaml


def schema_yaml(path):
    path = existing_file(path)

    with open(path, 'r') as f:
        schema_yaml = yaml.load(f, Loader=yaml.SafeLoader)

        if not isinstance(schema_yaml, list):
            raise argparse.ArgumentTypeError(f'root of the schema YAML must be a list. Got a {type(schema_yaml).__name__}.')

        for i, o in enumerate(schema_yaml):
            if not isinstance(o, str):
                raise argparse.ArgumentTypeError(f'schema YAML must be a list of statement strings. Item {i} is a {type(o).__name__}.')

        return schema_yaml


def existing_file(path):
    path = Path(path)
    if not path.exists():
        raise argparse.ArgumentTypeError(f'file "{path}" does not exist.')

    if not path.is_file():
        raise argparse.ArgumentTypeError(f'"{path}" is not a regular file.')

    return path


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


class ExporterJar(namedtuple('ExporterJar', ['path', 'type'])):
    class ExporterType(Enum):
        AGENT = ('Premain-Class', 'com.zegelin.cassandra.exporter.Agent')
        STANDALONE = ('Main-Class', 'com.zegelin.cassandra.exporter.Application')

    @classmethod
    def from_path(cls, path):
        path = existing_file(path)

        # determine the JAR type (agent or standalone) via the Main/Premain class
        try:
            with zipfile.ZipFile(path) as zf:
                manifest = zf.open('META-INF/MANIFEST.MF').readlines()

                def parse_line(line):
                    m = re.match('(.+): (.+)', line.decode("utf-8").strip())
                    return None if m is None else m.groups()

                manifest = dict(filter(None, map(parse_line, manifest)))

                type = next(iter([t for t in ExporterJar.ExporterType if t.value in manifest.items()]), None)
                if type is None:
                    raise argparse.ArgumentTypeError(f'"{path}" is not a cassandra-exporter jar.')

                return cls(path, type)

        except (zipfile.BadZipFile, KeyError):
            raise argparse.ArgumentTypeError(f'"{path}" is not a jar.')


def default_jar_path():
    project_dir = Path(__file__).parents[1]
    try:
        root_pom = ElementTree.parse(project_dir / 'pom.xml').getroot()
        project_version = root_pom.find('{http://maven.apache.org/POM/4.0.0}version').text

        return project_dir / f'agent/target/cassandra-exporter-agent-{project_version}.jar'

    except:
        return None


def default_schema_path():
    test_dir = Path(__file__).parent
    return test_dir / "schema.yaml"


parser = argparse.ArgumentParser()
parser.add_argument('cassandra_version', type=str, help="version of Cassandra to run", metavar="CASSANDRA_VERSION")
parser.add_argument('output_directory', type=output_directory, help="location to write metrics dumps", metavar="OUTPUT_DIRECTORY")

parser.add_argument('-o', '--overwrite-output', action='store_true', help="don't abort when the output directory exists or is not empty")

parser.add_argument('--cluster-directory', type=cluster_directory, help="location to install Cassandra. Must be empty or not exist. (default is a temporary directory)")
parser.add_argument('--keep-cluster-directory', type=bool, help="don't delete the cluster directory on exit")


parser.add_argument('-d', '--datacenters', type=int, help="number of data centers (default: %(default)s)", default=2)
parser.add_argument('-r', '--racks', type=int, help="number of racks per data center (default: %(default)s)", default=3)
parser.add_argument('-n', '--nodes', type=int, help="number of nodes per data center rack (default: %(default)s)", default=1)

parser.add_argument('-j', '--exporter-jar', type=ExporterJar.from_path, help="location of the cassandra-exporter jar, either agent or standalone (default: %(default)s)", default=str(default_jar_path()))
parser.add_argument('-s', '--schema', type=schema_yaml, help="CQL schema to apply (default: %(default)s)", default=str(default_schema_path()))

args = parser.parse_args()


if args.cluster_directory is None:
    args.cluster_directory = Path(tempfile.mkdtemp()) / "cluster"

if args.cluster_directory.exists():
    args.cluster_directory.rmdir() # CCM wants to create this


if args.output_directory.exists() and not args.overwrite_output:
    if next(args.output_directory.iterdir(), None) is not None:
        raise argparse.ArgumentTypeError(f'"{args.output_directory}" must be an empty directory.')

os.makedirs(args.output_directory, exist_ok=True)


ccm_cluster = ccmlib.cluster.Cluster(
    path=args.cluster_directory.parent,
    name=args.cluster_directory.name,
    version=args.cassandra_version,
    create_directory=True # if this is false, various config files wont be created...
)

ccm_cluster.populate(nodes=args.nodes * args.racks * args.datacenters)


def shutdown_cluster():
    print('Stopping cluster...')
    ccm_cluster.stop()


def delete_cluster_dir():
    shutil.rmtree(args.cluster_directory)


with contextlib.ExitStack() as defer:
    if not args.keep_cluster_directory:
        defer.callback(delete_cluster_dir)

    defer.callback(shutdown_cluster)

    for i, node in enumerate(ccm_cluster.nodelist()):
        print(f'Configuring node {node.name}')

        node.exporter_port = 9500 + i

        if args.exporter_jar.type == ExporterJar.ExporterType.AGENT:
            node.set_environment_variable('JVM_OPTS', f'-javaagent:{args.exporter_jar.path}=-l:{node.exporter_port}')

        # set dc/rack manually, since CCM doesn't support custom racks
        node.set_configuration_options({
            'endpoint_snitch': 'GossipingPropertyFileSnitch'
        })

        rackdc_path = os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties')

        node.rack_idx = (int(i / args.nodes) % args.racks) + 1
        node.dc_idx = (int(i / (args.nodes * args.racks))) + 1

        with open(rackdc_path, 'w') as f:
            f.write(f'dc=dc{node.dc_idx}\nrack=rack{node.rack_idx}\n')

    print('Starting cluster...')
    ccm_cluster.start()

    if args.exporter_jar.type == ExporterJar.ExporterType.STANDALONE:
        print('Starting standalone exporters...')

        for node in ccm_cluster.nodelist():
            logfile = open(Path(node.get_path()) / 'logs' / 'cassandra-exporter.log', 'w')

            command = ['java',
                    '-jar', args.exporter_jar.path,
                    '--listen', f':{node.exporter_port}',
                    '--jmx-service-url', f'service:jmx:rmi:///jndi/rmi://localhost:{node.jmx_port}/jmxrmi',
                    '--cql-address', f'localhost:{node.network_interfaces["binary"][1]}'
                    ]
            print(' '.join(map(str, command)))
            proc = subprocess.Popen(command, stdout=logfile, stderr=subprocess.STDOUT)

            defer.callback(proc.terminate)

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


