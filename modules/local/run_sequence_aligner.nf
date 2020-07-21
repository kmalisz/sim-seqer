/*
 * Run sequence alignment against reference
*/

process RUN_SEQUENCE_ALIGNER {
    input:
    tuple group, file(target_path), file(query_path)
    val alignment_type
    val min_similarity
    val min_coverage
    val max_n_gap_open

    output:
    path 'results/**'

    script:  // This script is bundled with the pipeline, in nf-core/simseqer/bin/
    """
    mkdir results
    parasail_alignment.py -query $query_path \
                          -target $target_path \
                          -out ./results/alignment.csv \
                          --method $alignment_type \
                          --max-n-gap-open $max_n_gap_open
    """
}
