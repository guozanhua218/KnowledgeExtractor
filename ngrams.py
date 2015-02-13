import sys
import math

from operator import add
from pyspark import SparkContext



#------------------------------------------------------------------------------------
def parseFile(strs,columns):


    mystring = ''
    mylist = strs.split(',')
    badstring = 0
    for n in columns:
        if (len(mylist[n].encode('ascii','ingore')) > 0):
            mystring = mystring + ' ' + mylist[n].encode('ascii','ingore')
        else:
            badstring = 1

    if badstring == 0:
        mystring = mystring[1:]
        return mystring
    else:
        return ''
    


#------------------------------------------------------------------------------------
def switchKeyValue(tuple):

    newtuple = (tuple[1],tuple[0])
    return newtuple


#------------------------------------------------------------------------------------
def toFrequency(tuple,totalWords):

    newtuple = (tuple[0],float(tuple[1])/float(totalWords))
    newtuple = (newtuple[0],newtuple[1])
    return newtuple


#------------------------------------------------------------------------------------
def wordcount(tokens):


    #Count
    totalCount = tokens.count()


    #Map
    tokens = tokens.map(lambda x: (x,1))
    

    #Reduce
    tokens = tokens.reduceByKey(add)
    

    #Sort
    tokens = tokens.map(lambda x: switchKeyValue(x))
    tokens = tokens.sortByKey(ascending=False)
    tokens = tokens.map(lambda x: switchKeyValue(x))


    #To Frequency
    tokens = tokens.map(lambda x: toFrequency(x,totalCount))


    #Return
    return tokens


#------------------------------------------------------------------------------------
def display(dictionary,nItems):


    collected = dictionary.collect()
    for x in range(nItems):
        item = collected[x]
        print item


#------------------------------------------------------------------------------------
def frequency(tuple):

    return tuple[1]


#------------------------------------------------------------------------------------
def PMI(base,target,threshold):


    collocations = []
    base = base.collect()
    target = target.collect()
    dictbase = dict(base)
    for key,value in target:
        keys = key.split(' ')
        val = 1;
        for thiskey in keys:
            val = val * dictbase[thiskey]
        PMI = math.log(value / val,2)
        collocations.append((key, PMI, value))

    

    collocations.sort(key=lambda x: x[2],reverse=True)
    collocations = filter(lambda x: x[2] > threshold,collocations)
    for key,pmi,freq in collocations:
        if pmi > 15:
            print key, pmi, freq






#------------------------------------------------------------------------------------
if __name__ == "__main__":



    #Init
    threshold = 0.0000000001
    sc = SparkContext(sys.argv[1], "Python Spark NGram Finder")


    #Read File
    lines = sc.textFile('/user/gijs/genesis/' + sys.argv[2] + '/' + sys.argv[2] + '.tokens')


    #Create Unigrams
    unigrams = lines.map(lambda x: parseFile(x,[5]))
    unigrams = unigrams.filter(lambda item: len(item)>0)


    #Create Bigrams
    bigrams = lines.map(lambda x: parseFile(x,[4,5]))
    bigrams = bigrams.filter(lambda item: len(item)>0)
    

    #Create Trigrams
    trigrams = lines.map(lambda x: parseFile(x,[3,4,5]))
    trigrams = trigrams.filter(lambda item: len(item)>0)


    #Unigrams Wordcount
    unigrams = wordcount(unigrams)
    unigrams = unigrams.filter(lambda item: frequency(item)>threshold)


    #Bigrams Wordcount
    bigrams = wordcount(bigrams)
    bigrams = bigrams.filter(lambda item: frequency(item)>threshold)
    

    #Trigrams Wordcount
    trigrams = wordcount(trigrams)
    trigrams = trigrams.filter(lambda item: frequency(item)>threshold)

    
    #Mutual Information in bigrams
    PMI(unigrams,bigrams,0.000001)


    """


    #Trigrams Wordcount
    #trigrams = wordcount(trigrams)
    #display(trigrams,10)


    """

