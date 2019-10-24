import shutil
import signal
import subprocess
from pathlib import Path
from typing import List

from ccmlib.cluster import Cluster

from utils.jar_utils import ExporterJar


class TestCluster(Cluster):
    standalone_processes: List[subprocess.Popen] = []

    def __init__(self, cluster_directory: Path, cassandra_version: str,
                 nodes: int, racks: int, datacenters: int,
                 exporter_jar: ExporterJar,
                 stop_on_exit: bool = True, delete_cluster_on_stop: bool = True):

        if cluster_directory.exists():
            cluster_directory.rmdir()  # CCM wants to create this

        super().__init__(
            path=cluster_directory.parent,
            name=cluster_directory.name,
            version=cassandra_version,
            create_directory=True  # if this is false, various config files wont be created...
        )

        self.stop_on_exit = stop_on_exit
        self.delete_cluster_on_stop = delete_cluster_on_stop

        self.exporter_jar = exporter_jar

        self.populate(nodes, racks, datacenters)

    def populate(self, nodes: int, racks: int = 1, datacenters: int = 1,
                 debug=False, tokens=None, use_vnodes=False, ipprefix='127.0.0.', ipformat=None,
                 install_byteman=False):
        result = super().populate(nodes, debug, tokens, use_vnodes, ipprefix, ipformat, install_byteman)

        for i, node in enumerate(self.nodelist()):
            node.exporter_port = 9500 + i

            if self.exporter_jar.type == ExporterJar.ExporterType.AGENT:
                node.set_environment_variable('JVM_OPTS', f'-javaagent:{self.exporter_jar.path}=-l:{node.exporter_port}')

            # set dc/rack manually, since CCM doesn't support custom racks
            node.set_configuration_options({
                'endpoint_snitch': 'GossipingPropertyFileSnitch'
            })

            rackdc_path = Path(node.get_conf_dir()) / 'cassandra-rackdc.properties'

            node.rack_idx = (int(i / nodes) % racks) + 1
            node.dc_idx = (int(i / nodes * racks)) + 1

            with open(rackdc_path, 'w') as f:
                f.write(f'dc=dc{node.dc_idx}\nrack=rack{node.rack_idx}\n')

        return result

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=True, wait_other_notice=True, jvm_args=None,
              profile_options=None, quiet_start=False, allow_root=False, **kwargs):

        result = super().start(no_wait, verbose, wait_for_binary_proto, wait_other_notice, jvm_args, profile_options,
                               quiet_start, allow_root, **kwargs)

        # start the standalone exporters, if requested
        if self.exporter_jar.type == ExporterJar.ExporterType.STANDALONE:
            for node in self.nodelist():
                process = self.exporter_jar.start_standalone(
                    logfile_path=Path(node.get_path()) / 'logs' / 'cassandra-exporter.log',
                    listen_address=('localhost', node.exporter_port),
                    jmx_address=('localhost', node.jmx_port),
                    cql_address=node.network_interfaces["binary"]
                )

                self.standalone_processes.append(process)

        return result

    def stop(self, wait=True, signal_event=signal.SIGTERM, **kwargs):
        result = super().stop(wait, signal_event, **kwargs)

        # shutdown standalone exporters, if they're still running
        for p in self.standalone_processes:
            p.terminate()

            if wait:
                p.wait()

        if self.delete_cluster_on_stop:
            shutil.rmtree(self.get_path())

        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.stop_on_exit:
            self.stop()



def create_ccm_cluster(cluster_directory: Path, cassandra_version: str, node_count: int):
    if cluster_directory.exists():
        cluster_directory.rmdir()  # CCM wants to create this

    print('Creating cluster...')
    ccm_cluster = Cluster(
        path=cluster_directory.parent,
        name=cluster_directory.name,
        version=cassandra_version,
        create_directory=True  # if this is false, various config files wont be created...
    )

    ccm_cluster.populate(nodes=node_count)

    return ccm_cluster

