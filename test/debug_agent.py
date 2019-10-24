import argparse
import os
from pathlib import Path

from ccmlib.cluster_factory import ClusterFactory

from utils.ccm import create_ccm_cluster
from utils.jar_utils import ExporterJar


def yesno_bool(b: bool):
    return ('n', 'y')[b]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cassandra_version', type=str, help="version of Cassandra to run", metavar="CASSANDRA_VERSION")
    parser.add_argument('cluster_directory', type=Path, help="location", metavar="CLUSTER_DIRECTORY")

    parser.add_argument('--jvm-debug-wait-attach', dest='jvm_debug_wait_attach', help="suspend JVM on startup and wait for debugger to attach", action='store_true')
    parser.add_argument('--no-jvm-debug-wait-attach', dest='jvm_debug_wait_attach', help="suspend JVM on startup and wait for debugger to attach", action='store_false')
    parser.add_argument('--jvm-debug-address', type=str, help="address/port for JVM debug agent to listen on", default='5005')

    parser.add_argument('--exporter-args', type=str, help="exporter agent arguments", default='-l:9500')
    parser.add_argument('-j', '--exporter-jar', type=ExporterJar.from_path, help="location of the cassandra-exporter jar, either agent or standalone (default: %(default)s)", default=str(ExporterJar.default_jar_path()))

    parser.set_defaults(jvm_debug_wait_attach=True)

    args = parser.parse_args()

    print(f'Cluster directory is: {args.cluster_directory}')

    if not args.cluster_directory.exists() or \
            (args.cluster_directory.exists() and next(args.cluster_directory.iterdir(), None) is None):

        # non-existent or empty directory -- new cluster
        ccm_cluster = create_ccm_cluster(args.cluster_directory, args.cassandra_version, node_count=1)

    else:
        # existing, non-empty directory -- assume existing cluster
        print('Loading cluster...')
        ccm_cluster = ClusterFactory.load(args.cluster_directory.parent, args.cluster_directory.name)

    node = ccm_cluster.nodelist()[0]
    print(f'Configuring node {node.name}')

    node.set_environment_variable('JVM_OPTS', f'-javaagent:{args.exporter_jar.path}={args.exporter_args} -agentlib:jdwp=transport=dt_socket,server=y,suspend={yesno_bool(args.jvm_debug_wait_attach)},address={args.jvm_debug_address}')

    print(f'JVM remote debugger listening on {args.jvm_debug_address}. JVM will suspend on start.')
    print('Starting single node cluster...')

    launch_bin = node.get_launch_bin()
    args = [launch_bin, '-f']
    env = node.get_env()

    os.execve(launch_bin, args, env)







