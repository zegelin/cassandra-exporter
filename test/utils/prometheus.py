import json
import platform
import re
import subprocess
import tarfile
import time
import urllib.request
import urllib.error
from contextlib import contextmanager
from enum import Enum, auto
from pathlib import Path
from typing import List

import yaml
from tqdm import tqdm


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


class PrometheusInstance(object):
    prometheus_directory: Path = None
    prometheus_process: subprocess.Popen = None

    @staticmethod
    def default_prometheus_archive_url():
        try:
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

        except:
            pass

        return None

    def __init__(self, prometheus_archive_url: str, base_directory: Path, listen_address='localhost:9090'):
        self.prometheus_directory = self._download_prometheus(prometheus_archive_url, base_directory)

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
                'job_name': 'cassandra',
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

    @staticmethod
    def _download_prometheus(archive_url: str, destination: Path):
        print(f'Downloading {archive_url} to {destination}...')

        archive_roots = set()

        with urllib.request.urlopen(archive_url) as response:
            with tqdm(total=int(response.headers.get('Content-length')), unit='bytes', unit_scale=True, miniters=1) as t:
                with tarfile.open(fileobj=_TqdmIOStream(response, t), mode='r|*') as archive:
                    for member in archive:
                        t.set_postfix(file=member.name)

                        archive_roots.add(Path(member.name).parts[0])

                        archive.extract(member, destination)

        return destination / next(iter(archive_roots))