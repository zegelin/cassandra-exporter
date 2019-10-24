import argparse
from pathlib import Path


def existing_file_arg(path):
    path = Path(path)
    if not path.exists():
        raise argparse.ArgumentTypeError(f'file "{path}" does not exist.')

    if not path.is_file():
        raise argparse.ArgumentTypeError(f'"{path}" is not a regular file.')

    return path


def nonexistent_or_empty_directory_arg(path):
    path = Path(path)

    if path.exists():
        if not path.is_dir():
            raise argparse.ArgumentTypeError(f'"{path}" must be a directory.')

        if next(path.iterdir(), None) is not None:
            raise argparse.ArgumentTypeError(f'"{path}" must be an empty directory.')

    return path