#!/usr/bin/env python3

import argparse
import os

import pandas as pd

from helpers import dataframe_to_fasta


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-reference_file', help='reference file path', type=str)
    parser.add_argument('-align', help='list of columns for alignment', type=str, nargs='+')
    parser.add_argument('-chunk_size', help='Size of the output chunks', type=int)
    parser.add_argument('-output_dir', help='path to output directory', type=str)

    args = parser.parse_args()
    format_chunks(args)


def format_chunks(args):
    sorted_align = sorted(list(set(args.align)))
    reference_df = pd.read_parquet(args.reference_file, columns=sorted_align + ['id'])
    reference_df['aligned'] = reference_df[sorted_align].apply(
        lambda row: ''.join(row.values.astype(str)), axis=1)
    reference_df.drop(columns=sorted_align, inplace=True)

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    for i in range(0, reference_df.shape[0], args.chunk_size):
        chunk = reference_df[i: i+args.chunk_size]
        dataframe_to_fasta(chunk, 'id', 'aligned',
                           os.path.join(args.output_dir, '{}.fasta'.format(i)))


if __name__ == '__main__':
    main()
