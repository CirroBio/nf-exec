#!/usr/bin/env python3
"""Normalise the input dataset selection into a list of S3 roots.

The two launch paths hand this parameter different things. The form's dataset picker
stores a full S3 root per selection and joins them on a comma; an API caller passes
dataset IDs, because that is what a dataset listing returns. Resolve both here so the
workflow receives one shape, and so the recorded parameters say plainly which datasets
a run read.
"""
import re

# s3://<project-bucket>/datasets/<dataset-id>, the layout every dataset in a project
# shares. The output dataset's own root is the only one the script is handed, and the
# input datasets sit beside it.
DATASETS_PREFIX = re.compile(r'/datasets/.*$')


def as_list(value) -> list[str]:
    """The selection in order, from either a comma-joined string or a list."""
    items = value if isinstance(value, list) else str(value).split(',')
    return [s for s in (str(item).strip() for item in items) if s]


def resolve(selection, dataset_root: str) -> list[str]:
    """Full S3 roots for the selection, resolving bare dataset IDs against the project.

    A token containing a separator is already a path and is left alone; anything else
    is a dataset ID.
    """
    datasets_root = DATASETS_PREFIX.sub('', dataset_root) + '/datasets'
    return [
        token if '/' in token else f'{datasets_root}/{token}'
        for token in as_list(selection)
    ]


if __name__ == '__main__':
    from cirro.helpers.preprocess_dataset import PreprocessDataset

    ds = PreprocessDataset.from_running()

    datasets = resolve(ds.params['input_datasets'], ds.dataset_root)
    ds.logger.info(f"Staging {len(datasets)} input dataset(s):")
    for i, uri in enumerate(datasets, start=1):
        ds.logger.info(f"  inputs/{i} <- {uri}")

    ds.add_param('input_datasets', datasets, overwrite=True)
