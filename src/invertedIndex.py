import re

from pyspark import SparkContext

"""
clean lines in a file
lines_tup - tuple of (fname, lines)
return - tuple of (set(cleaned word), fname)
"""
def clean(lines_tup) :
    fname, lines = lines_tup
    cleaned_words = set( re.split('\W+', lines.lower()) )
    return (cleaned_words, fname)

"""
create list of fnames for word
words_file - tuple of (set(word), fname)
return - tuple of (word, list(fnames))
"""
def word_map(words_file) :
    words, file = words_file
    fname = file.split('/')[-1]
    tup = ()
    for word in words :
        if word : tup += ( (word,[fname]), )
    return tup

def main(inDir, outDir) :
    spark = SparkContext()

    # get files into spark
    files = spark.wholeTextFiles(inDir)

    # clean text in files
    # create (word, fname) map
    # create (word, list(fnames)) map
    # assign index to map
    index = files.map(clean) \
                 .map(word_map) \
                 .flatMap(lambda x: x) \
                 .reduceByKey(lambda x, y : x + y) \
                 .zipWithIndex()

    # output the dictionary
    dFile = open(outDir + '/dictionary.txt','w')

    dictionary = index.map(lambda x: (x[0][0],x[1])).collect()
    for entry in dictionary :
        dFile.write('{}\t{}\n'.format(entry[0], entry[1]))
    dFile.close()

    # create inverted index
    index.map(lambda x : (x[1],x[0][1])) \
         .saveAsTextFile(outDir + '/inverted_index')

    spark.stop()

if __name__ == "__main__":
    inDir = 'indexing'
    outDir = 'output'
    main(inDir, outDir)
