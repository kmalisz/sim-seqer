#!/usr/bin/env python3
"""
The tool gets the data from data source and creates a structure of groupped files
"""
import argparse
from pprint import pprint
import pandas as pd
import yaml
import abc
import os
import logging

log = logging.getLogger(__name__)

CSV_SOURCE_CONFIG_PROPS = {'csv'}
POSTGRES_SOURCE_CONFIG_PROPS = {'get_group_query',
                                'get_groups_query',
                                'postgres_db_connection_string_env'}


class ReferenceConverter(abc.ABC):
    def __init__(self, out_dir, source_config, group_by_columns):
        self.group_by_columns = sorted(group_by_columns)
        self.out_dir = out_dir
        self.source_config = source_config
        self.groups = None

    def run(self):
        self.configure_source()
        self.get_and_store_groups()

    @property
    def grouping_tag(self):
        return '.'.join(self.group_by_columns)

    @abc.abstractmethod
    def yield_groups(self):
        pass

    @abc.abstractmethod
    def configure_source(self):
        pass

    def build_file_path(self, group, group_chunk_number):
        path = os.path.join(self.out_dir,
                            self.grouping_tag)
        for name, value in zip(self.group_by_columns, group):
            path = os.path.join(path, f'{name}.{value}')

        path = os.path.join(path, f'{group_chunk_number:03d}.parquet')

        return path

    def get_and_store_groups(self):
        for group_values, group_chunk_number, df in self.yield_groups():
            log.info(f'Storing group {group_values} (chunk: {group_chunk_number})...')
            path = self.build_file_path(group_values, group_chunk_number)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_parquet(path, compression='GZIP')
            log.info(f'...saved in {path}')


class CsvReferenceConverter(ReferenceConverter):
    def __init__(self, *args, **kwargs):
        super(CsvReferenceConverter, self).__init__(*args, **kwargs)
        self.df = None

    def configure_source(self):
        self.df = pd.read_csv(self.source_config['path'])

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
    args = get_args()
    references_config = parse_config(args.config)
    reference_config = references_config[args.reference]

    log.info(f'Starting creating reference {args.reference} grouped by {args.grouping} columns')
    create_reference(args.outdir,
                     args.reference,
                     reference_config['source'],
                     args.grouping)
    log.info('Finished')


def get_source_type(source_config):
    return source_config['type']


def create_reference(out_dir, name, source_config, group_by_columns):
    source_type = get_source_type(source_config)
    reference_converter_klass = reference_converter_klass_by_source_type[source_type]
    rc = reference_converter_klass(os.path.join(out_dir, name), source_config, group_by_columns)
    rc.run()


def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='converter-config.yml')
    parser.add_argument('--reference', help='name of reference from config file to be refreshed',
                        required=True)
    parser.add_argument('--grouping', nargs='*', help='Space separated list of columns by which '
                                                      'the program will group data',
                        required=True)
    parser.add_argument('--outdir', help='Path where the reference files will be stored',
                        required=True)
    return parser.parse_args()


def parse_config(config):
    with open(config) as f:
        return yaml.load(f)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
