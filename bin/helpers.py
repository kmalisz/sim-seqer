

def dataframe_to_fasta(data, id_column, align, output):
    with open(output, 'w+') as fasta:
        for _, row in data.iterrows():
            fasta.write('>{}\n'.format(row[id_column]))
            fasta.write('{}\n'.format(''.join(row[align])))
