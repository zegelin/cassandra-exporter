import argparse
import io

import ccmlib.cluster
import os
import urllib.request
import re
from collections import namedtuple, defaultdict, Counter
from enum import Enum, auto, unique
from frozendict import frozendict
import itertools

from prometheus_client.parser import text_fd_to_metric_families
import prometheus_client.samples


class MetricsDump(namedtuple('MetricsDump', ['path', 'metric_families'])):
    __slots__ = ()

    @classmethod
    def from_file(cls, path):
        with open(path, 'rt', encoding='utf-8') as fd:
            return MetricsDump.from_lines(fd)

    @classmethod
    def from_lines(cls, lines):
        with io.StringIO(lines) as fd:
            return MetricsDump.from_fd(fd)

    @classmethod
    def from_fd(cls, fd):
        def parse_lines():
            for family in text_fd_to_metric_families(fd):
                # freeze the labels dict so its hashable and the keys can be used as a set
                family.samples = [sample._replace(labels=frozendict(sample.labels)) for sample in family.samples]

                yield family

        metric_families = list(parse_lines())

        path = '<memory>'
        if isinstance(fd, io.BufferedReader):
            path = fd.name

        return MetricsDump(path, metric_families)


ValidationResult = namedtuple('ValidationResult', ['duplicate_families', 'duplicate_samples'])
DiffResult = namedtuple('DiffResult', ['added_families', 'removed_families', 'added_samples', 'removed_samples'])

# patch Sample equality & hash so that only name + labels are the identity (ignore value, timestamp, etc)
prometheus_client.samples.Sample.__eq__ = lambda self, o: (isinstance(o, prometheus_client.samples.Sample) and self.name == o.name and self.labels == o.labels)
prometheus_client.samples.Sample.__hash__ = lambda self: hash((self.name, self.labels))




def validate_dump(dump: MetricsDump) -> ValidationResult:
    def find_duplicate_families():
        def family_name_key_fn(f):
            return f.name

        families = sorted(dump.metric_families, key=family_name_key_fn)  # sort by name
        family_groups = itertools.groupby(families, key=family_name_key_fn)  # group by name
        family_groups = ((k, list(group)) for k, group in family_groups)  # convert groups to lists

        return {name: group for name, group in family_groups if len(group) > 1}

    def find_duplicate_samples():
        samples = itertools.chain(family.samples for family in dump.metric_families)
        #sample_groups =

        return


    return ValidationResult(
        duplicate_families=find_duplicate_families(),
        duplicate_samples=find_duplicate_samples()
    )

    # duplicate_metric_families = [key for key, value
    #                              in Counter([metric.name for metric in families]).items()
    #                              if value > 1]

    # if len(duplicate_metric_families):
    #     print('The following metric families are duplicated:')
    #     for family_name in duplicate_metric_families:
    #         print(f'\t{family_name}')


    # # find duplicate samples
    # for family in args.dump.metric_families:
    #     duplicate_samples = [key for key, value
    #                          in Counter(family.samples).items()
    #                          if value > 1]
    #
    #     if len(duplicate_samples) == 0:
    #         continue
    #
    #     print(f'Metric family "{family.name}" contains duplicate samples:')
    #
    #     for sample in duplicate_samples:
    #         print(f'\t{sample}')


def validate_dump_entrypoint(args):
    result = validate_dump(args.dump)

    if len(result.duplicate_families):
        print('The following metric families are duplicated:')

        for name, group in result.duplicate_families.items():
            print(f'\t{name}')

    pass



def diff_dump(from_dump: MetricsDump, to_dump):
    def diff_families():
        from_families = [(metric.name, metric.type) for metric in from_dump.metric_families]
        to_families = [(metric.name, metric.type) for metric in to_dump.metric_families]

        pass

    diff_families()

    return DiffResult([], [], [], [])

def diff_dump_entrypoint(args):
    pass






def prometheus_metrics(path):
    try:
        return MetricsDump.from_file(path)

    except Exception as e:
        raise argparse.ArgumentTypeError(f"error while parsing '{path}': {e}") from e


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    validate_parser = subparsers.add_parser('validate', help='validate a metrics dump for common problems')
    validate_parser.add_argument("dump", type=prometheus_metrics, metavar="DUMP")
    validate_parser.set_defaults(entrypoint=validate_dump_entrypoint)

    diff_parser = subparsers.add_parser('diff', help='diff two metrics dumps')
    diff_parser.add_argument("from", type=prometheus_metrics, metavar="FROM")
    diff_parser.add_argument("to", type=prometheus_metrics, metavar="TO")
    diff_parser.set_defaults(entrypoint=diff_dump_entrypoint)


    args = parser.parse_args()
    args.entrypoint(args)










# def index_metrics(metrics):
#     for metric in metrics:
#         metric.samples = {sample.labels: sample for sample in metric.samples}
#
#     return {metric.name: metric for metric in metrics}
#
#
# # index the metrics (convert lists to dicts) -- this removes duplicated families/samples
# from_metric_families = index_metrics(known_metric_families)
# to_metric_families = index_metrics(latest_metric_families)
#
# # find differences
# known_names = set(known_metric_families.keys())
# latest_names = set(latest_metric_families.keys())
#
# removed_names = known_names.difference(latest_names)
# if len(removed_names):
#     print('The following metric families no longer exist:')
#     for name in removed_names:
#         print(f'\t{name}')
#
# added_names = latest_names.difference(known_names)
# if len(added_names):
#     print('The following metric families are new:')
#     for name in added_names:
#         print(f'\t{name}')
#
for name in latest_names.intersection(known_names):
#     known_metric = known_metric_families[name]
#     latest_metric = latest_metric_families[name]
#
#     known_labels = set(known_metric.samples.keys())
#     latest_labels = set(latest_metric.samples.keys())
#
#     removed_labels = known_labels.difference(latest_labels)
#     if len(removed_labels):
#         print(f'The following samples no longer exist for metric family "{name}":')
#         for labels in removed_labels:
#             print(f'\t{labels}')
#
#     added_labels = latest_labels.difference(known_labels)
#     if len(added_labels):
#         print(f'The following samples are new for metric family "{name}":')
#         for labels in added_labels:
#             print(f'\t{labels}')
#
#
#     pass
#
#
# pass


#
# # cluster.stop()
#
# # cluster.set_environment_variable()