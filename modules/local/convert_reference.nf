#!/usr/bin/env nextflow

nextflow.preview.dsl = 2
params.reference = ''
params.grouping = ''
params.csv = ''
params.outdir = ''


process CONVERT_REFERENCE {
    input:
        val reference
        val grouping
        path reference_csv

    output:
        path 'reference_root', emit: outdir

    script:  // This script is bundled with the pipeline, in nf-core/simseqer/bin/
    """
    convert_reference.py --reference $reference\
                         --grouping $grouping\
                         --csv $reference_csv\
                         --outdir ./reference_root
    """
}

/*
 * Run the workflow
 */
workflow {
    CONVERT_REFERENCE(params.reference, params.grouping, params.csv).outdir.view()
}

