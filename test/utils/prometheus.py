import json
import platform
import re
import subprocess
import tarfile
import time
import urllib.request
import urllib.error
from collections import namedtuple
from contextlib import contextmanager
from enum import Enum, auto
from pathlib import Path
from typing import List

import yaml
from tqdm import tqdm

import logging

from utils.log_mixin import LogMixin


class _TqdmIOStream(object):
    def __init__(self, stream, t):
        self._stream = stream
        self._t = t

    def read(self, size):
        buf = self._stream.read(size)
        self._t.update(len(buf))
        return buf

    def __enter__(self, *args, **kwargs):
        self._stream.__enter__(*args, **kwargs)
        return self

    def __exit__(self, *args, **kwargs):
        self._stream.__exit__(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._stream, attr)



class PrometheusArchive(namedtuple('PrometheusArchive', ['url'])):
    logger = logging.getLogger(f'{__name__}.{__qualname__}')

    @classmethod
    def default_prometheus_archive_url(cls):
        def architecture_str():
            machine_aliases = {
                'x86_64': 'amd64'
            }

            machine = platform.machine()
            machine = machine_aliases.get(machine, machine)

            system = platform.system().lower()

            return f'{system}-{machine}'

        asset_pattern = re.compile(r'prometheus-.+\.' + architecture_str() + '\.tar\..+')

        with urllib.request.urlopen('https://api.github.com/repos/prometheus/prometheus/releases/latest') as response:
            release_info = json.load(response)

        for asset in release_info['assets']:
            if asset_pattern.fullmatch(asset['name']) is not None:
                return asset['browser_download_url']

    @classmethod
    def add_archive_argument(cls, name, parser):
        try:
            default_url = PrometheusArchive.default_prometheus_archive_url()
            default_help = '(default: %(default)s)'

        except Exception as e:
            cls.logger.warning('failed to determine Prometheus archive URL', exc_info=True)

            default_url = None
            default_help = f'(default: failed to determine archive URL)'

        parser.add_argument(name, type=PrometheusArchive,
                            help="Prometheus binary release archive (tar, tar+gz, tar+bzip2) URL (schemes: http, https, file) " + default_help,
                            required=default_url is None,
                            default=str(default_url))

    def download(self, destination: Path):
        print(f'Downloading {self.url} to {destination}...')

        archive_roots = set()

        with urllib.request.urlopen(self.url) as response:
            with tqdm(total=int(response.headers.get('Content-length')), unit='bytes', unit_scale=True, miniters=1) as t:
                with tarfile.open(fileobj=_TqdmIOStream(response, t), mode='r|*') as archive:
                    for member in archive:
                        t.set_postfix(file=member.name)

                        archive_roots.add(Path(member.name).parts[0])

                        archive.extract(member, destination)

        return destination / next(iter(archive_roots))


class PrometheusInstance(object):
    prometheus_directory: Path = None
    prometheus_process: subprocess.Popen = None

    def __init__(self, archive: PrometheusArchive, working_directory: Path, listen_address='localhost:9090'):
        self.prometheus_directory = archive.download(working_directory)
        self.listen_address = listen_address

    def start(self, wait=True):
        self.prometheus_process = subprocess.Popen(
            args=[str(self.prometheus_directory / 'prometheus'),
                  f'--web.listen-address={self.listen_address}'],
            cwd=str(self.prometheus_directory)
        )

        if wait:
            while not self.is_ready():
                time.sleep(1)

    def stop(self):
        if self.prometheus_process is not None:
            self.prometheus_process.terminate()

    @contextmanager
    def _modify_config(self):
        config_file_path = self.prometheus_directory / 'prometheus.yml'

        with config_file_path.open('r+') as stream:
            config = yaml.load(stream)

            yield config

            stream.seek(0)
            yaml.dump(config, stream)
            stream.truncate()

    def set_scrape_config(self, job_name: str, static_targets: List[str]):
        with self._modify_config() as config:
            config['scrape_configs'] = [{
                'job_name': job_name,
                'scrape_interval': '1s',
                'static_configs': [{
                    'targets': static_targets
                }]
            }]

    def is_ready(self):
        try:
            with urllib.request.urlopen(f'http://{self.listen_address}/-/ready') as response:
                return response.status == 200

        except urllib.error.URLError as e:
            if isinstance(e.reason, ConnectionRefusedError):
                return False

            raise e

    def _api_call(self, path):
        with urllib.request.urlopen(f'http://{self.listen_address}{path}') as response:
            response_envelope = json.load(response)

            if response_envelope['status'] != 'success':
                raise Exception(response.url, response.status, response_envelope)

            return response_envelope['data']

    def get_targets(self):
        return self._api_call('/api/v1/targets')

    def query(self, q):
        return self._api_call(f'/api/v1/query?query={q}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

        if self.prometheus_process is not None:
            self.prometheus_process.__exit__(exc_type, exc_val, exc_tb)



