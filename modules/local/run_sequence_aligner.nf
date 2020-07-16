#!/usr/bin/env nextflow
/*
 * Run sequence alignment against reference
*/

process RUN_SEQUENCE_ALIGNER {
    input:
    path query_path
    path target_path
    path output_path

    output:
    path output_path

    script:  // This script is bundled with the pipeline, in nf-core/simseqer/bin/
    """
    parasail_alignment.py -query $query_path \
                     -target $target_path\
                     -out $output_path
    REFERENCE_LIST=\$(cat $output_path)
    """
}
