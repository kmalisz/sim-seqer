#!/usr/bin/env nextflow

nextflow.preview.dsl=2

params.query = '/Users/tomaszwrzesinski/Documents/nextflow-hackathon-2020/sim-seqer/tests/data/heavy_tiny/query.csv'
params.match = 'h_v h_cdr3_len'
params.align = 'h_pep_mature'
params.list_of_reference_files = ['root/h_cdr3_len.h_v/12.IGHV1-69/ref.csv', 'root/h_cdr3_len.h_v/17.IGHV1-46/ref.csv', 'root/h_cdr3_len.h_v/12.IGHV1-46/ref.csv']

process GET_QUERY_GROUPS {
    publishDir 'results/', mode: 'copy'


    input:
    path query
    val list_of_reference_files
    val align
    val match

    output:
    path 'fasta_dir/**/*.fasta', emit: fasta_dir
    path 'query_fasta.csv', emit: query_fasta_csv
    path 'ref_paths.csv', emit: ref_paths_csv


    script:
    list_of_reference_files_space_separated = list_of_reference_files.join(' ')
    """
    get_query_groups.py -query $query -list-of-targets $list_of_reference_files_space_separated -align $align -match $match -output 'fasta_dir/' \
    --output-query-fasta 'query_fasta.csv' --output-reference 'ref_paths.csv'
    """
}

workflow {
    query_ch = Channel.fromPath(params.query)
    GET_QUERY_GROUPS(query_ch, params.list_of_reference_files, params.align, params.match).set{ query_groups_ch }
    query_groups_ch.query_fasta_csv.splitCsv(header:true).map{ row-> tuple(row.identifier, file('results/' + row.query_fasta_path)) }.view()
    query_groups_ch.ref_paths_csv.splitCsv(header:true).map{ row-> tuple(row.identifier, file('results/' + row.query_fasta_path)) }.view()
}