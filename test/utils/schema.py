import argparse
import yaml
from pathlib import Path
from collections import namedtuple

from utils.path_utils import existing_file_arg


class CqlSchema(namedtuple('CqlSchema', ['path', 'statements'])):
    @classmethod
    def from_path(cls, path):
        path = existing_file_arg(path)

        with open(path, 'r') as f:
            schema = yaml.load(f, Loader=yaml.SafeLoader)

            if not isinstance(schema, list):
                raise argparse.ArgumentTypeError(f'root of the schema YAML must be a list. Got a {type(schema).__name__}.')

            for i, o in enumerate(schema):
                if not isinstance(o, str):
                    raise argparse.ArgumentTypeError(f'schema YAML must be a list of statement strings. Item {i} is a {type(o).__name__}.')

            return cls(path, schema)

    @staticmethod
    def default_schema_path():
        test_dir = Path(__file__).parents[1]
        return test_dir / "schema.yaml"

    @staticmethod
    def add_schema_argument(name, parser):
        parser.add_argument(name, type=CqlSchema.from_path,
                            help="CQL schema to apply (default: %(default)s)",
                            default=str(CqlSchema.default_schema_path()))
