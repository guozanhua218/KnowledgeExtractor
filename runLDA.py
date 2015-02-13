#!/usr/bin/python

import string
import numpy
import sys
import re
import os
import onlineldavb
import wikirandom


#---------------------------------------------------------------------
def grabAbstracts(abstracts,batchsize,abstractID):

    docset = []
    text = ''
    articlenames = []
    for abstract in abstracts[0:batchsize]:
        docset.append(abstract)
    for ids in abstractID[0:batchsize]:
        articlenames.append(str(ids))

    #Remove entries
    abstracts = abstracts[batchsize:]
    abstractID = abstractID[batchsize:]

    #Return
    return list(docset), articlenames, abstracts, abstractID


#---------------------------------------------------------------------
if __name__ == '__main__':

    
    #Init
    batchsize = 64
    K = 100
    os.system("clear")


    #Our vocabulary
    vocab = file('data/' + sys.argv[1] + '/' + sys.argv[1] + '.keywords').readlines()
    vocab = [strs.rstrip() for strs in vocab]
    print vocab

    """
    W = len(vocab)


    #Read in Abstracts
    abstracts = file('data/' + sys.argv[1] + '/' + sys.argv[1] + '.abstracts').readlines()
    abstracts = [strs.rstrip() for strs in abstracts]
    D = len(abstracts)
    abstractID = range(0,D)
    nBatches = D / batchsize


    #Initialize the algorithm with alpha=1/K, eta=1/K, tau_0=1024, kappa=0.7
    lda = onlineldavb.OnlineLDA(vocab, K, D, 1./K, 1./K, 1024., 0.7)


    #Run
    for iteration in range(0, nBatches):

        #Grab Abstracts
        (docset, articlenames, abstracts,abstractID) = grabAbstracts(abstracts,batchsize,abstractID)
        #(docset, articlenames) = wikirandom.get_random_wikipedia_articles(batchsize)

        #Give them to online LDA
        (gamma, bound) = lda.update_lambda(docset)
      
        #Compute an estimate of held-out perplexity
        (wordids, wordcts) = onlineldavb.parse_doc_list(docset, lda._vocab)
        perwordbound = bound * len(docset) / (D * sum(map(sum, wordcts)))
        print '%d:  rho_t = %f,  held-out perplexity estimate = %f' % (iteration, lda._rhot, numpy.exp(-perwordbound))

        #Save to file
        if (iteration % 10 == 0):
            numpy.savetxt('data/' + sys.argv[1] + '/lambda.dat', lda._lambda)
            numpy.savetxt('data/' + sys.argv[1] + '/gamma.dat', gamma)
       
    """
