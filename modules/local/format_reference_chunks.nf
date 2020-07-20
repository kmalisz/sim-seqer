process FORMAT_REFERENCE_CHUNKS {
    input:
    tuple val(group), file(reference)
    val chunk_size

    output:
    tuple val(group), file('chunks/**.fasta')

    script:
    """
    mkdir chunks
    # TODO: implement python script
    # format_reference_chunks.py --reference_file ${reference} --chunk_size ${chunk_size} --out_dir ./chunks
    cp ${reference} ./chunks/${reference}.fasta
    """
}
