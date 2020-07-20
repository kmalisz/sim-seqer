#!/usr/bin/env python3
"""
The tool gets the data from data source and creates a structure of groupped files
"""
import abc
import argparse
import logging
import os

import pandas as pd
import yaml

log = logging.getLogger(__name__)


class ReferenceConverter(abc.ABC):
    def __init__(self, out_dir, group_by_columns):
        self.group_by_columns = sorted(set(group_by_columns))
        self.out_dir = out_dir

    def run(self):
        self.configure_source()
        self.get_and_store_groups()

    @property
    def root_grouping_path(self):
        return os.path.join(self.out_dir, '.'.join(self.group_by_columns))

    @abc.abstractmethod
    def yield_groups(self):
        pass

    @abc.abstractmethod
    def configure_source(self):
        pass

    def build_file_path(self, group_values, group_chunk_number):
        path = self.root_grouping_path
        for name, value in zip(self.group_by_columns, group_values):
            path = os.path.join(path, f'{name}.{value}')

        path = os.path.join(path, f'{group_chunk_number:03d}.parquet')

        return path

    def get_and_store_groups(self):
        for group_values, group_chunk_number, df in self.yield_groups():
            if not isinstance(group_values, tuple):
                group_values = (group_values,)
            log.info(f'Storing group {group_values} (chunk: {group_chunk_number})...')
            path = self.build_file_path(group_values, group_chunk_number)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_parquet(path, compression='GZIP')
            log.info(f'...saved in {path}')


class CsvReferenceConverter(ReferenceConverter):
    def __init__(self, csv_path, out_dir, group_by_columns):
        super(CsvReferenceConverter, self).__init__(out_dir, group_by_columns)
        self.df = None
        self.csv_path = csv_path

    def configure_source(self):
        self.df = pd.read_csv(self.csv_path)

    def yield_groups(self):
        for group_values, sub_df in self.df.groupby(self.group_by_columns):
            yield group_values, 0, sub_df


class PostgresReferenceConverter(ReferenceConverter):
    CHUNK_SIZE = 100000

    def configure_source(self):
        # establish connection to the DB
        pass

    def yield_groups(self):
        # probably double for loop
        # execute get_groups_query (to know what groups are in the DB)
        # for group in groups
        #   - execute get_group_query with params for given group
        #   - read query results in chunks
        pass


reference_converter_klass_by_source_type = {
    'csv': CsvReferenceConverter,
    'postgres': PostgresReferenceConverter
}


def main():
    print(__file__)
    args = get_args()
    validate_args(args)
    log.info(f'Reading config from {os.path.realpath(args.config)}')

    if args.reference:
        references_config = parse_config(args.config)
        reference_config = references_config[args.reference]

    log.info(f'Starting creating reference {args.reference} grouped by {args.grouping} columns')

    if args.reference_csv:
        converter = CsvReferenceConverter(args.reference_csv, args.outdir, args.grouping)
    elif args.reference:
        raise ValueError(f'Unsupported reference {args.reference}')
    else:
        raise ValueError('Either --reference-csv or --reference has to be specified')

    converter.run()

    log.info('Finished')


def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=os.path.join(os.path.dirname(__file__), 'converter-config.yml'))
    parser.add_argument('--reference', help='name of reference from config file to be refreshed')
    parser.add_argument('--reference-csv', help='Path to csv file. Exclusive for for csv reference '
                                                'source type')
    parser.add_argument('--grouping', nargs='*', help='Space separated list of columns by which '
                                                      'the program will group data',
                        required=True)
    parser.add_argument('--outdir', help='Path where the reference files will be stored',
                        required=True)
    return parser.parse_args()


def validate_args(args):
    if args.reference is None and args.reference_csv is None:
        raise ValueError('You have to provide either reference name (--reference) or '
                         'csv path (--reference-csv)')
    if args.reference and args.reference_csv:
        raise ValueError('Only one of --reference and --reference-csv can be passed')


def parse_config(config_path):
    with open(config_path) as f:
        return yaml.load(f)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
