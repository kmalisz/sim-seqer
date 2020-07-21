#!/usr/bin/env python3

import argparse
import re
from collections import Counter

import pandas as pd
import parasail


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-query', help='Query file path', type=str)
    parser.add_argument('-target', help='Target file path', type=str)
    parser.add_argument('-out', help='Output file path', type=str)
    parser.add_argument('--gap', help='Cost to open a new gap',
                        type=int, default=11)
    parser.add_argument('--match', help='Reward for a match', type=int, default=1)
    parser.add_argument('--method', help='Alignment method',
                        type=str, default='global', choices=['global', 'local'])
    parser.add_argument('--min-similarity', help='Minimal similarity required to return a match',
                        type=float, default=0.7)
    parser.add_argument('--min-coverage', help='Minimal coverage required to return a match',
                        type=float, default=0.9)
    parser.add_argument('--max-n-gap-open', help='Maximum allowed gap openings for match to be returned',
                        type=int, default=1)
    args = parser.parse_args()
    parasail_alignment(args)


def parasail_alignment(args):
    loaded_queries = parasail.sequences_from_file(args.query)
    queries = [query for query in loaded_queries if len(query.seq) > 0]
    loaded_targets = parasail.sequences_from_file(args.target)
    targets = [target for target in loaded_targets if len(target.seq) > 0]
    results = []

    for query in queries:
        partial_results = get_alignment_results(query, targets, args)
        results += [result for result in partial_results if (
            result['similarity'] >= args.min_similarity and
            result['gapopen'] <= args.max_n_gap_open
            #todo add check for qcov when ready
            )]
    results_df = pd.DataFrame(results)
    results_df.to_parquet('{}.parq'.format(args.out))


def get_alignment_results(query, targets, args):
    results = []
    for target in targets:
        results.append(run_parasail(query, target, args))
    return results


def run_parasail(query, target, args):
    if args.method == 'global':
        result = parasail.nw_trace_scan_16(query.seq, target.seq, args.gap, args.match, parasail.blosum62)
    elif args.method == 'local':
        result = parasail.sw_trace_scan_16(query.seq, target.seq, args.gap, args.match, parasail.blosum62)
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
