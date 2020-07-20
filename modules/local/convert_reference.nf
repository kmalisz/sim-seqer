#!/usr/bin/env nextflow

nextflow.preview.dsl = 2
params.reference = ''
params.grouping = ''
params.csv = ''
params.outdir = ''


process CONVERT_REFERENCE {
    // TODO: Publish only database references which should be cashed not csv
    publishDir "${params.references_cache_dir}", pattern: 'reference_root/**', saveAs: { filename -> filename.replace('reference_root/', '') },  mode: params.publish_dir_mode

    input:
        val reference
        val grouping
        path reference_csv

    output:
        path 'reference_root/**'

    script:  // This script is bundled with the pipeline, in nf-core/simseqer/bin/
    def ref = reference_csv.name != 'NO_FILE' ? "--reference-csv $reference_csv" : '--reference ' + reference
    """
    convert_reference.py $ref\
                         --grouping $grouping\
                         --outdir ./reference_root/$reference

    """
}

/*
 * Run the workflow
 */
workflow {
    CONVERT_REFERENCE(params.reference, params.grouping, file(params.csv)).outdir.view()
}

