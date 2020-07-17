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
    def ref = reference_csv.name != 'NO_FILE' ? "--reference-csv $reference_csv" : '--reference ' + reference
    """
    convert_reference.py $ref\
                         --grouping $grouping\
                         --outdir ./reference_root
    """
}

/*
 * Run the workflow
 */
workflow {
    CONVERT_REFERENCE(params.reference, params.grouping, file(params.csv)).outdir.view()
}

