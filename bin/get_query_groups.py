#! /usr/bin/env python

import argparse
import os

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-query', help='query file path', type=str)
    parser.add_argument('-list-of-targets', help='target file path', type=str,  nargs='*')
    parser.add_argument('-align', type=str, nargs='*')
    parser.add_argument('-match', type=str, nargs='*')
    parser.add_argument('-output', type=str)
    parser.add_argument('--output-query-fasta', default='output_query_fasta.csv')
    parser.add_argument('--output-reference', default='output_reference.csv')

    args = parser.parse_args()
    get_query_groups(args)


def dataframe_to_fasta(df, id_column, align, output):
    with open(output + '.fasta', 'w+') as fasta:
        for index, row in df.iterrows():
            fasta.write('>{}\n'.format(row[id_column]))
            fasta.write('{}\n'.format(''.join(row[align])))


def get_query_groups(args):
    query_df = pd.read_csv(args.query)

    matches = list(set(args.match))
    matches.sort()
    groups = query_df.groupby(matches, as_index=False)

    sorted_align = sorted(list(set(args.align)))
    queries_fasta_paths = []
    references_paths = []

    for group, data in groups:
        identifier = '.'.join([str(el) for el in list(group)])
        output_dir = args.output + '.'.join(matches)
        output_path = os.path.join(output_dir, identifier + '.fasta')
        queries_fasta_paths.append([identifier, output_path])

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        references_paths.append([identifier, identifier_in_reference_paths(
            identifier, args.list_of_targets)])
        dataframe_to_fasta(data, 'pair_id', sorted_align, output_path)

    pd.DataFrame(queries_fasta_paths, columns=['identifier', 'query_fasta_path']).to_csv(args.output_query_fasta, index=False)
    pd.DataFrame(references_paths, columns=['identifier', 'query_fasta_path']).to_csv(args.output_reference, index=False)


def identifier_in_reference_paths(identifier, list_of_reference_paths):
    for reference_path in list_of_reference_paths:
        if identifier in reference_path:
            return reference_path
    raise(ValueError('Group identifier {} not in reference path'.format(identifier)))


if __name__ == '__main__':
    main()
