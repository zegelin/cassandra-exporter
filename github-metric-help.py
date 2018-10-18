import collections
import itertools
import json
import re
import urllib.request


def combine_dicts(dicts):
    res = collections.defaultdict(list)

    for (key, value) in itertools.chain.from_iterable(map(dict.items, dicts)):
        res[key].append(value)
    return res


label_help = {
    r'cassandra_cache.*': {
        'cache': 'The cache name.'
    },
    r'cassandra_table_.*': {
        'keyspace': 'The keyspace name.',
        'table': 'The table name.',
        'table_type': 'Type of table: `table`, `index` or `view`.',
        'compaction_strategy_class': 'The compaction strategy class of the table.'
    }
}

def get_label_help(family, label):
    for (pattern, labels) in label_help.items():
        if not re.match(pattern, family):
            continue

        return labels.get(label, '_No help specified._')



request = urllib.request.Request(url='http://localhost:9500/metrics', headers={'Accept': 'application/json'})
response = urllib.request.urlopen(request)

data = json.load(response)

print('== Contents')

print('''.Metric Families
|===
|Metric Family |Type |Help
''')

for (familyName, metricFamily) in sorted(data['metricFamilies'].items()):
    print('|', '<<_{},{}>>'.format(familyName, familyName))
    print('|', metricFamily['type'].lower())
    print('|', metricFamily.get('help', '_No help specified._'))
    print()

print('|===')



def exclude_system_table_labels(labels):
    if labels.get('keyspace') in ('system_traces', 'system_schema', 'system_auth', 'system', 'system_distributed'):
        return {}

    return labels



print('== Metric Families')
for (familyName, metricFamily) in sorted(data['metricFamilies'].items()):
    print('===', familyName)
    print(metricFamily.get('help', '_No help specified._'))
    print()

    print('''.Available Labels
|===
|Label |Help |Example
''')

    labels = combine_dicts(map(lambda m: exclude_system_table_labels(m.get('labels') or {}), metricFamily['metrics']))

    if len(labels) == 0:
        print('3+| _No labels defined._')

    else:
        for label, examples in labels.items():
            print('|', '`{}`'.format(label))
            print('|', get_label_help(familyName, label))
            print('|', ', '.join(map(lambda e: '`{}`'.format(e), set(examples))))

    print()


    print('|===')


    print()


