
process GET_REFERENCE {
    publishDir "${params.outdir}/pipeline_info", mode: params.publish_dir_mode

    input:
    var db_name
    var match
    var align
    var allow-refresh
    var force-refresh

    output:
    env REFERENCE_LIST
    path ./reference.txt

    script:  // This script is bundled with the pipeline, in nf-core/simseqer/bin/
    """
    get_reference.py -db $db_name\
                     -match $match\
                     -align $align\
                     -force-refresh $force-refresh\
                     -allow-refresh $allow-refresh\
                     -output ./reference.txt
    REFERENCE_LIST=\$(cat ./reference.txt)
    """
}