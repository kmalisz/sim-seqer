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
def helpMessage() {
    def command = "nextflow run main.nf --query query.csv --align h_cdr3 --match h_cdr3_len --reference oas -profile docker"
    log.info Headers.nf_core(workflow, params.monochrome_logs)
    log.info Schema.params_help("$baseDir/nextflow_schema.json", command)
}

if (params.help) {
    helpMessage()
    exit 0
}

/*
 * Validate parameters
 */
validParams = ['query', 'reference', 'references_dir', 'match', 'align', 'alignment_type', 'allow_refresh',
               'force_refresh', 'min_similarity', 'min_coverage', 'max_n_gap_open', 'outdir', 'publish_dir_mode',
               'name', 'email', 'email_on_fail', 'plaintext_email', 'monochrome_logs', 'tracedir', 'max_memory',
               'max_cpus', 'max_time', 'config_profile_name', 'config_profile_description', 'help']

error = []
params.keySet().each { key ->
	if( !validParams.contains(key) ) { error.add(key) }
}
if( !error.empty ) {
	log.error "Unknown parameter: ${error}\n"
	log.error "Valid parameters: ${validParams}\n"
	helpMessage()
	exit 1
}

if (params.query) { ch_query = file(params.query, checkIfExists: true) } else { exit 1, "Query file not specified!" }

    // TODO: validate all params:
/*
        summary['Match']            = params.match
        summary['Align']            = params.align
        summary['Reference']        = params.reference
        summary['References Dir']   = params.references_dir
        summary['Allow Refresh']    = params.allow_refresh
        summary['Force Refresh']    = params.force_refresh
        summary['Alignment Type']   = params.alignment_type
        summary['Min Similarity']   = params.min_similarity
        summary['Min Coverage']     = params.min_coverage
        summary['Max N Gap Open']   = params.max_n_gap_open
*/
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
include { VALIDATE_QUERY_FILE } from './modules/local/validate_input' params(params)

/*
 * Run the workflow
 */

reference_path = file([params.references_dir, params.reference, params.match?.tokenize(' ').sort().join('.')].join('/'))

workflow {

    VALIDATE_QUERY_FILE(ch_query, params.match, params.align)
        .set { ch_query_valid }

    if (ch_query_valid) {
        println(reference_path)
    }

    if (reference_path.exists()){
        ch_reference_files = Channel.fromPath( reference_path + '**/*.csv' )
    } else {
        ch_reference_files = Channel.empty()
    }
}

/*
 * Send completion email
 */
workflow.onComplete {
    Completion.email(workflow, params, summary, run_name, baseDir, log)
    Completion.summary(workflow, params, log)
}
