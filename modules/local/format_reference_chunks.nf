process FORMAT_REFERENCE_CHUNKS {
    input:
    tuple val(group), file(reference)
    val chunk_size
    val align

    output:
    tuple val(group), file('chunks/**.fasta')

    script:
    """
    mkdir chunks
    format_reference_chunks.py -reference_file ${reference} \
                               -align ${align} \
                               -chunk_size ${chunk_size} \
                               -output_dir ./chunks
    """
}
