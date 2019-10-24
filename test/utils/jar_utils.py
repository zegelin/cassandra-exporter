import argparse
import re
import subprocess
import zipfile
from collections import namedtuple
from enum import Enum
from pathlib import Path
from xml.etree import ElementTree

from utils.path_utils import existing_file_arg


class ExporterJar(namedtuple('ExporterJar', ['path', 'type'])):
    class ExporterType(Enum):
        AGENT = ('Premain-Class', 'com.zegelin.cassandra.exporter.Agent')
        STANDALONE = ('Main-Class', 'com.zegelin.cassandra.exporter.Application')

    @classmethod
    def from_path(cls, path):
        path = existing_file_arg(path)

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

    def start_standalone(self, listen_address: (str, int),
                         jmx_address: (str, int),
                         cql_address: (str, int),
                         logfile_path: Path):

        logfile = logfile_path.open('w')

        def addr_str(address: (str, int)):
            return ':'.join(address)

        command = ['java',
                   '-jar', self.path,
                   '--listen', addr_str(listen_address),
                   '--jmx-service-url', f'service:jmx:rmi:///jndi/rmi://{addr_str(jmx_address)}/jmxrmi',
                   '--cql-address', addr_str(cql_address)
                   ]
        print(' '.join(map(str, command)))
        return subprocess.Popen(command, stdout=logfile, stderr=subprocess.STDOUT)

    @staticmethod
    def default_jar_path():
        project_dir = Path(__file__).parents[2]
        try:
            root_pom = ElementTree.parse(project_dir / 'pom.xml').getroot()
            project_version = root_pom.find('{http://maven.apache.org/POM/4.0.0}version').text

            return project_dir / f'agent/target/cassandra-exporter-agent-{project_version}.jar'

        except:
            return None


