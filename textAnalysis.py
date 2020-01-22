## CONFIGURACIÓN DEL ENTORNO

import findspark
findspark.init()
import pyspark
import random
import re
import os.path
sc = pyspark.SparkContext(master="local[1]", appName="PEC1_dcastellot")

## CARGAMOS NUESTRO TEXTO Y LIMPIAMOS

def removePunctuation(text):

    cleanUp = re.sub(r'[^a-z0-9\s]','',text.lower().strip())
    
    def spaceRepl(matchobj):
        if matchobj.group(0) == ' ': return ' '
        else: return ' '

    cleanUp_ns = re.sub(' {1,10000}', spaceRepl, cleanUp) #Tiene que haber codigo más limpio para quitar los espacios...
    
    return(cleanUp_ns)
        
#print(removePunctuation('Hi, you!'))
#print(removePunctuation(' No under_score!'))
#print(removePunctuation(' *      Remove punctuation then spaces  * '))

## ESTE CÓDIGO HA SIDO USADO PARA CARGAR FICHEROS DE HDFS
# .textFile lee archivos de texto en HDFS

pathToFolder = '/user/data'
fileName = 'TuTexto.txt'

fileName = os.path.join(pathToFolder, fileName)

textoRDD = sc.textFile(fileName, 8).map(removePunctuation).filter(lambda x: len(x)>0)
#loremRDD.take(10)

# EXTRAEMOS LAS PALANRAS DEL TEXT EN UN RDD

textoWordsRDD = textoRDD.flatMap(lambda line: line.split(' '))
textoWordsCount = textoWordsRDD.count()

# Variable RRD de las palabras en el texto

distintWordsMapRDD = loremWordsRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

distintWordsRDD = distintWordsMapRDD.keys().distinct()

#print(distintWordsRDD.take(10))
print ('\nEn total hay {} palabras distintas en el texto\n'.format(distintWordsRDD.count()))   

## ANÁLISIS DEL TEXTO

def wordCount(wordListRDD):

    resultTuples = wordListRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    return (resultTuples) #devolvemos los pares clave valor
    
# Vemos las palabras más frecuentes

top15WordsAndCounts = wordCount(textoWordsRDD).takeOrdered(15, key = lambda x: -x[1])

print('\nPalabras más frecuentes:\n')
print(top15WordsAndCounts)

## Análisis específico:

# Palabras distintas con solo dos u

def check2U(word) :
    count = 0
    
    for i in range (0, len(word)):
        if word[i] == 'u' :
            count = count + 1
    
    if count == 2: 
        return True
    
    return False
 
#check2U('Oraculopu')

u2WordsRDD = distintWordsMapRDD.map(lambda x : x[0]).map(lambda x: (x, check2U(x))).filter(lambda x: x[1]==True).collect()

print ('En total hay {} palabras distintas con exactamente 2 Us'.format(len(u2WordsRDD)))

# Podemos aplicar otros filtros para detectar diferencias:

# Función que devielve True si la palabra tiene exactamente 9 letras

def is9(word) :
    
    length = len(word)
            
    print (length)
    
    if length == 9: 
        return True
    
    return False
 
#is9('123456789')

# Devuelve True para palabras con más vocales que consonantes

def vocalWord(word) :
    
    cons = 0;
    voc = 0;
    
    for i in range (0,len(word)):
        if word[i] == 'a' or word[i] =='e' or word[i] =='i' or word[i] =='o' or word[i] =='u': voc = voc + 1
        else: cons = cons + 1;
    
    if voc > cons : return True
    
    return False 

#vocalWord('peeeeedro')



