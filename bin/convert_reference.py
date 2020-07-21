#!/usr/bin/env python3
"""
The tool gets the data from data source and creates a structure of groupped files
"""
import argparse
import logging
import os
from collections import defaultdict
from typing import Dict, Tuple

import pandas as pd
import psycopg2

log = logging.getLogger(__name__)

REFERENCES = {
    'oas-heavy': {
        'type': 'oas',
        'chain': 'Heavy',
        'select_statement_by_column': {
            'h_cdr3_len': 'char_length(chain.cdr3aa)+1',
            'h_v': "SPLIT_PART(SPLIT_PART(chain.v, '*', '1'), 'IG', '2')",
            'h_pep_mature': 'chain.aa',
            'h_cdr3': 'chain.cdr3aa',
            'id': 'chain.id'
        },
        'reference_columns': ['id', 'h_v', 'h_cdr3_len', 'h_pep_mature', 'h_cdr3'],
        'cdr3_col': 'h_cdr3',
        'pep_mature_col': 'h_pep_mature',
        'cdr3_len_col': 'h_cdr3_len'
    },
    'oas-light': {
        'type': 'oas',
        'chain': 'Light',
        'select_statement_by_column': {
            'l_cdr3_len': 'char_length(chain.cdr3aa)+1',
            'l_v': "SPLIT_PART(SPLIT_PART(chain.v, '*', '1'), 'IG', '2')",
            'l_pep_mature': 'chain.aa',
            'l_cdr3': 'chain.cdr3aa',
            'id': 'chain.id'
        },
        'reference_columns': ['id', 'l_v', 'l_cdr3_len', 'l_pep_mature', 'l_cdr3'],
        'cdr3_col': 'l_cdr3',
        'pep_mature_col': 'l_pep_mature',
        'cdr3_len_col': 'l_cdr3_len'
    }
}


class ReferenceConverter():
    def __init__(self, out_dir, group_by_columns):
        self.group_by_columns = sorted(set(group_by_columns))
        self.out_dir = out_dir

    def run(self):
        self.configure_source()
        self.get_and_store_groups()

    @property
    def root_grouping_path(self):
        return os.path.join(self.out_dir, '.'.join(self.group_by_columns))

    def yield_groups(self):
        raise NotImplementedError

    def configure_source(self):
        raise NotImplementedError

    def build_file_path(self, group_values, group_chunk_number):
        path = self.root_grouping_path
        for name, value in zip(self.group_by_columns, group_values):
            path = os.path.join(path, f'{name}.{value}')

        path = os.path.join(path, f'{group_chunk_number:03d}.parquet')

        return path

    def get_and_store_groups(self):
        for group_values, i_chunk, chunk_df in self.yield_groups():
            if not isinstance(group_values, tuple):
                group_values = (group_values,)
            log.info(f'Storing chunk {i_chunk:03d} of group {group_values}, '
                     f'size: {len(chunk_df)}')
            path = self.build_file_path(group_values, i_chunk)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            chunk_df.to_parquet(path, compression='GZIP')
            log.info(f'...saved in {path}')


class CsvReferenceConverter(ReferenceConverter):
    def __init__(self, csv_path, out_dir, group_by_columns):
        super(CsvReferenceConverter, self).__init__(out_dir, group_by_columns)
        self.df = None
        self.csv_path = csv_path

    def configure_source(self):
        self.df = pd.read_csv(self.csv_path)

    def yield_groups(self):
        for group_values, chunk_df in self.df.groupby(self.group_by_columns):
            yield group_values, 0, chunk_df


class OasReferenceConverter(ReferenceConverter):
    CHUNK_SIZE = 100000

    def __init__(self, reference_name, out_dir, group_by_columns):
        super(OasReferenceConverter, self).__init__(out_dir, group_by_columns)
        self.reference_name = reference_name
        self.db_connection_string_env_name = None
        self.get_groups_query = None

    def configure_source(self):
        self.db_connection_string_env_name = 'OAS_CONNECTION_STRING'

    def get_chain_ids_by_group_values(self) -> Dict[tuple, list]:
        get_groups_query = self.compose_get_groups_ids_query()
        with psycopg2.connect(os.environ.get(self.db_connection_string_env_name)) as connection:
            chain_ids_by_group = defaultdict(list)
            groups_ids_chunks = pd.read_sql(get_groups_query, connection, chunksize=1e6)
            for groups_ids_df in groups_ids_chunks:
                for group_values, group_df in groups_ids_df.groupby(self.group_by_columns):
                    if not isinstance(group_values, tuple):
                        group_values = (group_values,)
                    chain_ids_by_group[group_values].extend(list(group_df['id']))

        return chain_ids_by_group

    def yield_groups(self) -> Tuple[tuple, int, pd.DataFrame]:
        log.info(f'Getting ids of all distinct tuples of {self.group_by_columns} from database')
        chain_ids_by_group = self.get_chain_ids_by_group_values()

        n_total_groups = len(chain_ids_by_group)
        log.info(f'{n_total_groups} groups identified')
        for i_group, (group_values, ids) in enumerate(chain_ids_by_group.items()):
            log.info(f'Processing group identified by: {group_values} '
                     f'({i_group + 1}/{n_total_groups})')
            get_group_data_query = self.compose_get_group_data_query(ids)
            with psycopg2.connect(os.environ.get(self.db_connection_string_env_name)) as connection:
                df = pd.read_sql(get_group_data_query, con=connection, chunksize=self.CHUNK_SIZE)
                for i_chunk, chunk_df in enumerate(df):
                    self.process_raw_data(chunk_df)
                    yield group_values, i_chunk, chunk_df

    def compose_get_groups_ids_query(self):
        query = ''
        query += 'SELECT '
        select_statements = [
            '{} AS {}'.format(
                REFERENCES[self.reference_name]['select_statement_by_column'][column],
                column)
            for column in ['id'] + self.group_by_columns
        ]
        query += ', '.join(select_statements) + ' '
        # query += 'FROM (SELECT * from chain LIMIT 100) as chain '  # TODO remove limit (next line)
        query += 'FROM chain '
        query += 'JOIN data_unit on chain.data_unit_id = data_unit.id '
        query += "WHERE data_unit.chain='{}'".format(REFERENCES[self.reference_name]['chain'])
        return query

    def compose_get_group_data_query(self, ids):
        query = ''
        query += 'SELECT '
        select_statements = [
            '{} AS {}'.format(REFERENCES[self.reference_name]['select_statement_by_column'][column],
                              column)
            for column in REFERENCES[self.reference_name]['reference_columns']
        ]
        query += ', '.join(select_statements) + ' '
        query += 'FROM chain '
        ids_str = ','.join([str(_id) for _id in ids])
        query += f'WHERE chain.id IN ({ids_str})'
        return query

    def process_raw_data(self, df):
        def find_cdr3(cdr3, pep_mature):
            import re
            return [m.start() for m in re.finditer(f'(?={cdr3})', pep_mature)]

        cdr3_col = REFERENCES[self.reference_name]['cdr3_col']
        cdr3_len_col = REFERENCES[self.reference_name]['cdr3_len_col']
        pep_mature_col = REFERENCES[self.reference_name]['pep_mature_col']

        new_cdr3_list = []
        for cdr3, pep_mature, cdr3_len in zip(df[cdr3_col], df[pep_mature_col], df[cdr3_len_col]):
            cdr3_beginning = find_cdr3(cdr3, pep_mature)
            if len(cdr3_beginning) == 1:
                new_cdr3 = pep_mature[cdr3_beginning[0]:cdr3_beginning[0] + len(cdr3) + 1]
                if len(new_cdr3) != cdr3_len:
                    log.warning(f'Length of extended cdr3 does not match the queried length. '
                                f'Omitting record')
                    new_cdr3 = None
            else:
                log.warning(f'Could not identify single cdr3 region in pep_mature, omitting record'
                            f'(cdr3: {cdr3}, pep_mature: {pep_mature})')
                new_cdr3 = None

            new_cdr3_list.append(new_cdr3)

        df[cdr3_col] = pd.Series(new_cdr3_list, df.index)
        return df[~df[cdr3_col].isna()]


reference_converter_klass_by_type = {
    'oas': OasReferenceConverter,
}


def main():
    args = get_args()
    validate_args(args)

    log.info(f'Starting creating reference {args.reference} grouped by {args.grouping} columns')
    if args.reference_csv:
        converter = CsvReferenceConverter(args.reference_csv, args.outdir, args.grouping)
    elif args.reference:
        reference_config = REFERENCES.get(args.reference, None)
        if reference_config is None:
            raise ValueError(f'The reference with name {args.reference} does not exist. '
                             f'Did you mean one of {list(REFERENCES.keys())}')
        reference_converter_klass = reference_converter_klass_by_type[reference_config['type']]
        converter = reference_converter_klass(args.reference, args.outdir, args.grouping)
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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
