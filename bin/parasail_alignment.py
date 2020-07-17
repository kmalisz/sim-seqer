#! /usr/bin/env python

import argparse
import re
from collections import Counter

import pandas as pd
import parasail


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-query', help='query file path', type=str)
    parser.add_argument('-target', help='target file path', type=str)
    parser.add_argument('-out', help='output file path', type=str)
    parser.add_argument('--gap', type=int, default=11)
    parser.add_argument('--match', type=int, default=1)
    parser.add_argument('--method', type=str, default='nw')
    parser.add_argument('--min-similarity', type=float, default=0.8)
    args = parser.parse_args()
    parasail_alignment(args)


def parasail_alignment(args):
    loaded_queries = parasail.sequences_from_file(args.query)
    queries = [query for query in loaded_queries if len(query.seq) > 0]
    loaded_targets = parasail.sequences_from_file(args.target)
    targets = [target for target in loaded_targets if len(target.seq) > 0]
    results = []

    i = 0
    for query in queries:
        partial_results = get_alignment_results(query, targets, args)
        results += [result for result in partial_results if result['similarity'] >= args.min_similarity]
        i += 1
        if i > 100:
            break
    dataframe = pd.DataFrame(results)
    dataframe.to_csv(args.out + '.csv', index=False)
    # dataframe.to_parquet(args.out + '.parq')


def get_alignment_results(query, targets, args):
    results = []
    for target in targets:
        results.append(run_parasail(query, target, args))
    return results


def run_parasail(query, target, args):
    result = parasail.nw_trace_scan_16(query.seq, target.seq, args.gap, args.match, parasail.blosum62)
    comp = result.traceback.comp
    cigar = result.cigar
    counts = Counter(comp)
    matches = counts['|']
    similar = counts[':'] + matches
    gaps = counts[' ']
    length = len(comp)
    gap_regex = re.compile(r'\s+')
    return { #todo fill up qend, send and qcov metrics
        'score': result.score,
        'qseqid': int(query.name),
        'sseqid': int(target.name),
        'qstart': cigar.beg_query,
        'qend': '',
        'sstart': cigar.beg_ref,
        'send': '',
        'qlen': len(result.query),
        'slen': len(result.ref),
        'pident': matches,
        'ppos': similar,
        'similarity': round(float(similar) / length, 2),
        'identity': round(float(matches) / length, 2),
        'length': length,
        'gaps': gaps,
        'gapopen': len(gap_regex.findall(comp)),
        'qcov': ''#round(float(length - gaps) / len(query.seq), 2)
    }


if __name__ == '__main__':
    main()
