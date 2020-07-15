#!/usr/bin/env nextflow

params.match = []
params.query = ''

match_list = params.match.tokenize(' ')

query_ch = Channel.fromPath(params.query)

process check_if_match_column_exists_in_query_file {
    input:
    file query
    each val(match)

    output:
    val exit_code

    script:
    
    file_columns = $query.getProperties()
    



}
    