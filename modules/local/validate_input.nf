process VALIDATE_QUERY_FILE {
    input:
    file query
    val match
    val align

    output:
    val true

    script:
    """
    #!/usr/bin/env bash

    header=`head -n 1 $query`

    if [[ \$(echo \$header | grep -L "pair_id") ]]
    then
        printf "No column pair_id in the query file!"
        exit 1
    fi

    for match_column in $match
    do 
        if [[ \$(echo \$header | grep -L \$match_column) ]] 
        then
            match_column_no_len=`echo \$match_column | sed 's/_len//g'`
            if [[ \$(echo \$header | grep -L \$match_column_no_len) ]]
            then
                printf "No column \${match_column_no_len} in the query file!"
                exit 1
            fi
        fi
    done

    for align_column in $align
    do 
        if [[ \$(echo \$header | grep -L \$align_column) ]] 
        then
            printf "No column \${match_column} in the query file!"
            exit 1
        fi
    done
    """
}
