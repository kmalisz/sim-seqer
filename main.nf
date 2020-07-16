#!/usr/bin/env nextflow
/*
========================================================================================
                         nf-core/simseqer
========================================================================================
 nf-core/simseqer Analysis Pipeline.
 #### Homepage / Documentation
 https://github.com/nf-core/simseqer
----------------------------------------------------------------------------------------
*/

nextflow.preview.dsl = 2

/*
 * Print help message if required
 */
if (params.help) {
    // TODO nf-core: Update typical command used to run pipeline
    def command = "nextflow run nf-core/simseqer --input samplesheet.csv -profile docker"
    log.info Headers.nf_core(workflow, params.monochrome_logs)
    log.info Schema.params_help("$baseDir/nextflow_schema.json", command)
    exit 0
}

/*
 * Validate parameters
 */
if (params.input) { ch_input = file(params.input, checkIfExists: true) } else { exit 1, "Input samplesheet file not specified!" }


/*
 * Check parameters
 */
Checks.aws_batch(workflow, params)     // Check AWS batch settings
Checks.hostname(workflow, params, log) // Check the hostnames against configured profiles

/*
 * Print parameter summary
 */
// Has the run name been specified by the user?
// this has the bonus effect of catching both -name and --name
run_name = params.name
if (!(workflow.runName ==~ /[a-z]+_[a-z]+/)) {
    run_name = workflow.runName
}
summary = Schema.params_summary(workflow, params, run_name)
log.info Headers.nf_core(workflow, params.monochrome_logs)
log.info summary.collect { k,v -> "${k.padRight(20)}: $v" }.join("\n")
log.info "-\033[2m----------------------------------------------------\033[0m-"

workflow_summary = Schema.params_mqc_summary(summary)
ch_workflow_summary = Channel.value(workflow_summary)

/*
 * Include local pipeline modules
 */
include './modules/local/output_documentation' params(params)
include './modules/local/get_software_versions' params(params)
include './modules/local/check_samplesheet' params(params)

/*
 * Run the workflow
 */
workflow {

    CHECK_SAMPLESHEET(ch_input)
        .splitCsv(header:true, sep:',')
        .map { check_samplesheet_paths(it) }
        .set { ch_raw_reads }

    FASTQC(ch_raw_reads)

    OUTPUT_DOCUMENTATION(
        ch_output_docs,
        ch_output_docs_images)

    GET_SOFTWARE_VERSIONS()

    MULTIQC(
        ch_multiqc_config,
        ch_multiqc_custom_config.collect().ifEmpty([]),
        FASTQC.out.collect(),
        GET_SOFTWARE_VERSIONS.out.yml.collect(),
        ch_workflow_summary)
}

/*
 * Send completion email
 */
workflow.onComplete {
    def multiqc_report = []
    Completion.email(workflow, params, summary, run_name, baseDir, multiqc_report, log)
    Completion.summary(workflow, params, log)
}
