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
def wordcount(tokens,threshold):


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
    tokens = tokens.filter(lambda x: x[1]>threshold)

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
def ratio(tuple):

    
    key = tuple[0]
    values = tuple[1]
    ratio = values[0]/values[1]
    return key,ratio


#------------------------------------------------------------------------------------
def getkeywords(target,base):


    #Join
    keywords = target.join(base)
    keywords = keywords.map(lambda x: ratio(x))
    keywords = keywords.map(lambda x: switchKeyValue(x))
    keywords = keywords.sortByKey(ascending=False)
    keywords = keywords.map(lambda x: switchKeyValue(x))
    keywords = keywords.filter(lambda x: x[1] > 2.00000)    


    #Print keywords
    collected = keywords.collect()
    for keyword,strength in collected:
        print keyword + ': ' + str(strength)


    #Return
    keywords = keywords.map(lambda x: x[0])
    return keywords


#------------------------------------------------------------------------------------
if __name__ == "__main__":


    #Init
    threshold = 0.00001
    sc = SparkContext(sys.argv[1], "Python Spark NGram Finder")


    #Read Base
    base = sc.textFile('/user/gijs/genesis/' + sys.argv[2] + '/' + sys.argv[2] + '.tokens')


    #Create Base Dictionary
    base = base.map(lambda x: parseFile(x,[5]))
    base = base.filter(lambda item: len(item)>0)
    base = wordcount(base,threshold)
    

    #Read Target
    target = sc.textFile('/user/gijs/genesis/' + sys.argv[3] + '/' + sys.argv[3] + '.tokens')


    #Create Target Dictionary
    target = target.map(lambda x: parseFile(x,[5]))
    target = target.filter(lambda item: len(item)>0)
    target = wordcount(target,threshold)
    

    #Join
    keywords = getkeywords(target,base)


    #Display and save
    keywords.saveAsTextFile('/user/gijs/genesis/' + sys.argv[3] + '/' + sys.argv[3] + '.keywords')



