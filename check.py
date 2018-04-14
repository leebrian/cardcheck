#!/usr/local/bin/python

# Mess-around project to learn more python. 
# I organize my card collection according to price. Only $2 (although this might change) plus cards are kept in my trade box. 
# $1-2 cards are kept in a separate less accessed box. Anything under $1 is kept as a "bulk." Since prices change, it's a bit of a pain to check each card's value.
# This program will compare current prices to an older version of the inventory and produce a report reports:
# Report 1) Cards that dropped from trade box, $TRADE->$1; $TRADE->bulk w/ gross delta; by color, alphabatized
# Report 2) Cards that changed from dollar box, $1->$TRADE; $1->bulk w/gross delta; by color, alphabatized
# Report 3) Cards that increased from bulk box, bulk->$1; bulk-$TRADE w/gross delta; by color, alphabatized
# Overall change in value, any notes
#
# So I have to export from deckbox.org, then compare to old listCardsCSVs
# Export doesn't have color, so I have to compare to a library fetched from mtgjson.org, if listCardsCSVs aren't found then it probably means
# I need to update mtgjson.org (TODO:maybe check the filesize against header from https://mtgjson.com/json/AllCards.json.zip)
# 
# I'll schedule this to run every month or so
# Goals: learn csv with pandas, http stuff with requests, json stuff
#

import datetime
import io
import json
import logging
import os
import shutil
import subprocess
import sys
import zipfile
from operator import itemgetter
from pathlib import Path
from time import strftime
from timeit import default_timer as timer

import pandas
import requests

dtScriptStart = datetime.datetime.now()

#write a dictionary of http cookies to a local file
def makeCookies(cookies):
    with open("cookies.json","w") as file:
        json.dump(cookies,file)
    return

#read a dictionary of http cookies from a local file
def eatCookies():
    with open("cookies.json","r") as file:
        return json.load(file)

#go get a card json library, write it to disk, unzip it and return it as a Path
#keep the zip too so we cn compare byte size for updates
def getCardLibrary(libFile):
    print("in getCardLib:" + str(libFile))
    response = requests.get(MAGIC_CARD_JSON_URL, stream=True)
    bytes = io.BytesIO(response.content)
    
    with open(libFile.with_suffix(".zip"), "wb") as file:
        file.write(bytes.read(-1))

    zip = zipfile.ZipFile(bytes)

    zip.extractall(libFile.parent)
    return libFile

#clean up and prep the data frame, remove unnecessary columns, change formats
#expects a DataFrame in, returns a cleaned DataFrame back
def cleanCardDataFrame(df):

    #remove all the columns I don't need
    listColumnsIDontNeed = {"Type","Tradelist Count","Rarity","Language","Signed","Artist Proof","Altered Art","Misprint","Promo","Textless","My Price"}
    for colName in listColumnsIDontNeed:
        if colName in df.columns:
            del df[colName]

    #convert price to a number (dont' care about dollar sign)
    df["Price"] = df["Price"].str.replace("$","").astype(float)

    #should be fewer columns now, and price should be a float
    #df.info()
    return df

#read the runLog, runlog has when-run (YYYYMMDDHHMMSS), old-file, new-file
#why json? because I want to be able to sort and add elements and hierarchies and stuff if I want, and trying to work more with json
def readRunLog():
    debug("reading the log")
    dictRunLog = {}
    if Path(RUN_LOG_FILE_NAME).exists():
        with open(RUN_LOG_FILE_NAME,"r") as file:
            dictRunLog =  json.load(file)
    debug("current run log: " + str(dictRunLog))
    return dictRunLog

#write out the runLog, runlog has when-run (YYYYMMDDHHMMSS), old-file, new-file
#overwriting this file every time kind of worries me, so I'm going to read the current file, merge over it with what is passed and write combined back out
def writeRunLog(dictRunLogIn):
    debug("writing the log")
    dictRunLog = readRunLog()
    dictRunLog.update(dictRunLogIn)
    with open(RUN_LOG_FILE_NAME,"w") as file:
        json.dump(dictRunLog,file)
    return

#figure out what the right file is to compare current file to, pass in fun file dict, return a file that exists in data
def determineCompareFile(dictRunLog):
    #sort run log by old-file
    dictRunLog = sorted(dictRunLog.items(),key=itemgetter(0))
    runLogSize = len(dictRunLog)
    #print("size run log: " + str(runLogSize)+ str(dictRunLog) + "::::" + str(dictRunLog[runLogSize-1][1]["old-file"]))

    lastCompared = None

    #get the last item in the run log to find the last old-file 
    if runLogSize > 0:
        lastCompared = dictRunLog[runLogSize-1][1]["old-file"]

    print("LastCompared: "+str(lastCompared))

    #find all the csvs in data/
    listCardsCSVs = list(filter(lambda x: str(x).endswith(".csv"),os.listdir(DATA_DIR_NAME)))
    #sort them all, oldest to newest by file name
    listCardsCSVs = sorted(listCardsCSVs)

    debug("listCardsCSVs:" + str(len(listCardsCSVs)) + str(listCardsCSVs))

    #find the lastCompared in list, default to -1 if no match
    indexLastCompared = None
    try:
        indexLastCompared = listCardsCSVs.index(lastCompared)
    except ValueError:
        indexLastCompared = -1

    debug("indexLastCompared: (-1 means I've never compared this file) " + str(indexLastCompared))

    #Check the next card csv file up chronologically
    indexToCompare = indexLastCompared+1
    #there should not be a situation where there's not a file after the last compared, but if so, just run against the latest file
    if (indexToCompare >= len(listCardsCSVs)):
        indexToCompare = len(listCardsCSVs)-1

    debug("indexToCompare" + str(indexToCompare))

    toCompareFileName = listCardsCSVs[indexToCompare]
    #print("toCompareFileName: " + str(toCompareFileName))
    return toCompareFileName

#set up the logging for printing to the console stream
def configureLogging():
    logger = logging.getLogger(__name__)
    
    
    print("Checking arguments, if --debug sent in as argument, then debug log level" + str(sys.argv))
    if (len(sys.argv) > 1):
        logLevel = sys.argv[1]
        if str(logLevel).endswith("debug"):
            logger.setLevel(logging.DEBUG)
            logger.addHandler(logging.StreamHandler())
            print(str(logLevel))
    print("Debug level: " + str(logger.getEffectiveLevel()))
        
#log a debug message so I don't have to type getLogger... a million times, 
def debug(msg):
    logger = logging.getLogger(__name__)
    logger.debug(msg)

#Return a unique key based on a row, sortcat+name+edition+condition+foil+cardNumber
def makeMushedKey(row):
    return row["SortCategory"]+"-"+row["Name"]+"-"+row["Edition"]+"-"+row["Condition"]+"-"+str(row["Card Number"])+"-"+str(row["Foil"])

#Update count/price stats for a row; deltas are calculated from the stats dictionary with mushedKeys as key
def updateRowStats(row,dictStats):
    key = makeMushedKey(row)

    oldCount = 0.0
    if pandas.notnull(row["OldCount"]):
        oldCount = row["OldCount"]
    newCount = 0.0
    if pandas.notnull(row["NewCount"]):
        newCount = row["NewCount"]
    oldPrice = 0.0
    if pandas.notnull(row["OldPrice"]):
        oldPrice = row["OldPrice"]
    newPrice = 0.0
    if pandas.notnull(row["NewPrice"]):
        newPrice = row["NewPrice"]
    countChange = newCount - oldCount
    priceChange = newPrice - oldPrice

    dictStats[key] = {"count-change":countChange,
    "price-change":priceChange,
    "total-change":(newPrice*newCount)-(oldPrice*oldCount),
    "old-count":row["OldCount"],
    "new-count":row["NewCount"],
    "old-price":row["OldPrice"],
    "new-price":row["NewPrice"]}

    #for debugging purposes, I like to look at certain cards
    if key.startswith("XXXAether Hub"):
        print(str(row))
        print(str(dictStats[key]))
        print("newPrice: " + str(newPrice)+ ":" + str(row["NewPrice"]))
        print("newCount: " + str(newCount))
        print(str(newPrice*newCount))
        print(str(oldPrice*oldCount))

#print out the general stats about a dictionary, totalquantity change, total price change, number cards, number up, number down
def printStats(dict, label="Unknown"):
    print("Stats for [" + label + "]")
    list = dict.values()
    totalQuantityChange = sum(item["count-change"] for item in list)
    totalPriceChange = sum(item["total-change"] for item in list)
    numberNegative = 0
    numberPositive = 0
    totalGain = 0.0
    totalLoss = 0.0

    for item in list:
        if item["total-change"] > 0:
            numberPositive+=1
            totalGain+=item["total-change"]
        if item["total-change"] < 0:
            numberNegative +=1
            totalLoss+=item["total-change"]

    print("total items: " + str(len(list)))
    print("netQuantityChange:" + str(int(totalQuantityChange)) + "; cardsIncreasedValue: " + str(numberPositive) + "; cardsDecreasedValue: " + str(numberNegative))
    print("netPriceChange: " + "${:,.2f}".format(totalPriceChange) + "; grossPositive: " + "${:,.2f}".format(totalGain) + "; grossNegative: " + "${:,.2f}".format(totalLoss))

#Fetch or build the AllCards library
#out: dict representation of the AllCards.json file
def buildCardLibrary():

    #now let's make sure that there's the most recent card library in json format
    cardLibraryFile = Path(DATA_DIR_NAME + "AllCards.json")
    debug("magic card lib file:" + str(cardLibraryFile) + ":"+str(cardLibraryFile.exists()))
    cardLibraryDict = None

    #if we don't have the file yet, go get it
    if not cardLibraryFile.exists():
        print("no card db")    
        cardLibraryFile = getCardLibrary(cardLibraryFile)
        print("lib file after :" + str(cardLibraryFile))

        #print(cardLibraryDict)
    else:
        debug("card db exists")
        
        #check to make sure is most recent
        localZipSize = cardLibraryFile.with_suffix(".zip").stat().st_size
        debug("localZipSize " + str(localZipSize))

        #check the header from json url
        head = requests.head(MAGIC_CARD_JSON_URL)
        remoteZipSize = int(head.headers["Content-Length"])
        debug("remoteZipSize content-length " + str(remoteZipSize))
        
        #only fetch if local (from a previous fetch) is a different size
        if (localZipSize != remoteZipSize):
            print("not equal size, let's get a fresh card lib")
            #backup the current file, just in case
            timestampMod = os.path.getmtime(cardLibraryFile)
            dtMod = datetime.datetime.fromtimestamp(timestampMod)
            strModDate = dtMod.strftime("%Y%m%d")
            shutil.copy(cardLibraryFile,DATA_DIR_NAME + strModDate + "-" + os.path.basename(cardLibraryFile) + ".bak")
            cardLibraryFile = getCardLibrary(cardLibraryFile)


    with cardLibraryFile.open() as file:
            cardLibraryDict = json.load(file)

    return cardLibraryDict

#Figure out the card sort category based on the card name, look up in AllCards lib
#in:card name
#out: string category. I organize as White/Black/Blue/Green/Red/Colorless/Land/Gold/Unknown
def lookupSortCategory(strCardName,dictLib):
    strSortCategory = "Unknown"
    dictCard = dictLib.get(strCardName)
    if dictCard is not None:
        listColors = dictCard.get("colors",{})
        if len(listColors) > 1:
            strSortCategory = "Gold"
        elif len(listColors) == 1:
            strSortCategory = listColors[0]
        elif len(listColors) == 0:#colorless could be Land or other colorless (Art, Eldrazi, whatever)
            listTypes = dictCard.get("types",{})
            if "Land" in listTypes:
                strSortCategory = "Land"
            else:
                strSortCategory = "Colorless"
    else:#maybe if we can't find it there's something special
        if strCardName.find("//") > -1:#cards with split names are stored weird in AllCards
            listNames = strCardName.split("//")
            strSortCategory = lookupSortCategory(listNames[0].strip(), dictLib)
    return strSortCategory

#perform the merge and post merge clean and prep to ready for processing
#in-dataframe with today's cards, dataframe with comparison cards
#out-dataframr ready for processing
def buildMergeDF(dfNew, dfOld):
    #merge with a double outer join of old and new
    dfMergeCards = pandas.merge(dfNew,dfOld,how="outer",on=["Name","Edition","Foil","Condition","Card Number"])
    dfMergeCards = dfMergeCards[["Name","Edition","Condition","Foil","Card Number","Count","CompareCount","Price","ComparePrice"]]
    dfMergeCards = dfMergeCards.rename(index=str,columns={"Count":"NewCount","CompareCount":"OldCount","Price":"NewPrice","ComparePrice":"OldPrice"})
    #dfMergeCards.info()
    #clean up foil flag
    dfMergeCards["Foil"].fillna("N",inplace=True)
    dfMergeCards["Foil"] = dfMergeCards["Foil"].replace("foil","Y")

    #clean up null values in old set for new cards, set up a new field for new cards
    dfMergeCards["OldCount"].fillna(-1,inplace=True)
    dfMergeCards.eval("IsNew = (OldCount == -1)",inplace=True)
    dfMergeCards.loc[dfMergeCards["OldCount"] == -1, "OldCount"] = 0 
    dfMergeCards["OldPrice"].fillna(0.0,inplace=True)

    #clean up null values in new set for deleted cards, set up a new field for deleted cards
    dfMergeCards["NewCount"].fillna(-1,inplace=True)
    dfMergeCards.eval("IsGone = (NewCount == -1)",inplace=True)
    dfMergeCards.loc[dfMergeCards["NewCount"] == -1, "NewCount"] = 0 
    dfMergeCards["NewPrice"].fillna(0.0,inplace=True)


    #add some explicit columns for convenience in the log csv. Don't think I need these ultimately because of querying
    dfMergeCards.eval("CountChange = (NewCount - OldCount)",inplace=True)
    dfMergeCards.eval("PriceChange = (NewPrice - OldPrice)",inplace=True)
    dfMergeCards.eval("TotalChange = ((NewPrice*NewCount) - (OldPrice*OldCount))",inplace=True)

    dictCardLibrary = buildCardLibrary()
    #print(str(len(dictCardLibrary)))

    #set a new column called SortCategory with categories how I organize my cards
    dfMergeCards["SortCategory"] = dfMergeCards["Name"].apply(lookupSortCategory,args=(dictCardLibrary,))

    dfMergeCards.to_csv(DATA_DIR_NAME + "last-merged.csv")
    print("Comparing #TodayRecords to #CompareRecords in #MergedRecords" + str(len(dfTodaysCards)) + ":" + str(len(dfCompareCards)) + ":" + str(len(dfMergeCards)))
    return dfMergeCards

#write a new log entry to the run log
def updateRunLog(
    strOldFileName,strNewFileName,dictRunLog,dtScriptStart,dtScriptEnd,countBulkToTrades,countBulkToDollar,countDollarToTrades,countDollarToBulk,countTradeToDollar,
    countTradeToBulk,countNewCards,countGoneCards,countUnchCards):
    intTotalCardsProcessed = countBulkToTrades+countBulkToDollar+countDollarToTrades+countDollarToBulk+countTradeToDollar+countTradeToBulk+countNewCards+countGoneCards+countUnchCards
    dictRunLog[dtScriptStart.strftime("%Y%m%d-%H:%M:%S:%f")] = {"old-file" : strOldFileName, "new-file" : strNewFileName, "bulk-to-trades" : countBulkToTrades, "bulk-to-dollar" : countBulkToDollar, "dollar-to-trades" : countDollarToTrades, "dollar-to-bulk" : countDollarToBulk, "trades-to-dollar" : countTradeToDollar, "trades-to-bulk" : countTradeToBulk, "new-cards" : countNewCards, "gone-cards" : countGoneCards, "unch-cards" : countUnchCards, "total-cards" : intTotalCardsProcessed, "elapsed-time" : (dtScriptEnd.timestamp()-dtScriptStart.timestamp())}
    print(str(dictRunLog[dtScriptStart.strftime("%Y%m%d-%H:%M:%S:%f")]))
    #writeRunLog(dictRunLog)

MAGIC_CARD_JSON_URL = "https://mtgjson.com/json/AllCards.json.zip"
DATA_DIR_NAME = "data/"
RUN_LOG_FILE_NAME = DATA_DIR_NAME + "run-log.json"
TRADE_BOX_THRESHOLD = 2 #this might change, but it's $2 for now

print("Hello World!!!!")

configureLogging()

#reuse the datetime from the perf logger
strToday = dtScriptStart.strftime("%Y%m%d")
#print(strToday)

strTodayFileName = strToday+"-magic-cards.csv"
print("CSV that I want for today: " + strTodayFileName)

#now call out to deckbox.org to get inventory as csv, this command is dumped from firefox and seems to work
#found this code from handy site https://curl.trillworks.com/
#cookies are private to my account, so I want to read and write them from my local file that doesn't get committed
cookies = eatCookies()

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Host': 'deckbox.org',
    'Referer': 'https://deckbox.org/sets/1016639',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:59.0) Gecko/20100101 Firefox/59.0',
}

params = (
    ('format', 'csv'),
    ('f', ''),
    ('s', ''),
    ('o', ''),
    ('columns', 'Price,Rarity,Color'),
)

#if there's no file for today, then go get it
if Path(DATA_DIR_NAME + strTodayFileName).exists():
    print("j'exist, donc il ne faut que je getter le file")
else:
    print("je n'exist pas, donc if faut que je getter le file")
    response = requests.get('https://deckbox.org/sets/export/1016639', headers=headers, params=params, cookies=cookies)
    response.encoding = "UTF-8"
    #print(response.text)
    todayFile = open(DATA_DIR_NAME + strTodayFileName,"w")
    todayFile.write(response.text)
    todayFile.close()



print("OK cool, now I have a CSV of my library, a dictionary of every magic card ever that's up to date. Now I can check for price diffs")

#get today's file
dfTodaysCards = pandas.read_csv(DATA_DIR_NAME + strTodayFileName)
dfTodaysCards = cleanCardDataFrame(dfTodaysCards)

#getting older file is a bit trickier, check the run log, find the most recent run, find the old file used, get the next recent old file to compare with
dictRunLog = readRunLog()

strCompareFileName = determineCompareFile(dictRunLog)
print("ToCompareAgainst: " + strCompareFileName)

dfCompareCards = pandas.read_csv(DATA_DIR_NAME + strCompareFileName)
dfCompareCards = cleanCardDataFrame(dfCompareCards)
#dfTodaysCards.info()
dfCompareCards = dfCompareCards.rename(index=str,columns={"Count":"CompareCount","Price":"ComparePrice"})
#dfCompareCards.info()

dfMergeCards = buildMergeDF(dfTodaysCards,dfCompareCards) 

#set up a couple of stats dictionaries, 
#trade changes-dropped-dollar cards,count change, price change, total change, old-count,new-count,old-price,new-price
dictTradesToDollar = {}
#trade changes-dropped-bulk cards,count change, price change, total change, old-count,new-count,old-price,new-price
dictTradesToBulk = {}
#dollar changes-upped-trade cards, count change, price change, total change, old-count,new-count,old-price,new-price
dictDollarToTrade = {}
#dollar changes-dropped-bulk cards, count change, price change, total change, old-count,new-count,old-price,new-price
dictDollarToBulk = {}
#bulk changes-upped-trade cards, count change, price change, total change, old-count,new-count,old-price,new-price
dictBulkToTrade = {}
#bulk changes-upped-dollar cards, count change, price change, total change, old-count,new-count,old-price,new-price
dictBulktoDollar = {}
#new cards
dictNewCards = {}
#removed cards
dictGoneCards = {}
#unchanged cards
dictUnchangedCards = {}
#general stats
dictGeneralStats = {}

rowsProcessed = 0

timeLoopStart = timer()
for index,row in dfMergeCards.iterrows():
    #print(str(index) + ":" + str(row))
    #is this a new card? This should not require any updates. Since new cards are already categorized
    updateRowStats(row,dictGeneralStats)
    if row["IsNew"]:
        debug("New (not in the old file):" + str(row))
        #dictNewCards[makeMushedKey(row)] = [{"Count":row["NewCount"],"Price":row["NewPrice"]}]
        updateRowStats(row,dictNewCards)
        rowsProcessed+=1
    elif row["IsGone"]:
        debug("Gone (not in the new file):" + str(row))
        updateRowStats(row,dictGoneCards)
        rowsProcessed+=1
    else:
        #check for changes to trade
        if row["NewPrice"] >= TRADE_BOX_THRESHOLD:
            #print("NewPrice is over $2: " + str(row))
            if row["OldPrice"] < TRADE_BOX_THRESHOLD: #if new>2&old<2,this means new item for trade box
                if row["OldPrice"] < 1: #if new>2&old<1,upgrade from bulk
                    updateRowStats(row,dictBulkToTrade)
                else:#if it's not going to trade then it's going to dollar
                    updateRowStats(row,dictDollarToTrade)
                rowsProcessed+=1
            else:#if new>2,old>2, then don't do anything (ie, no change) 
                updateRowStats(row,dictUnchangedCards)
                rowsProcessed+=1
        #change for changes to dollar
        elif row["NewPrice"] >= 1:
            if row["OldPrice"] >= TRADE_BOX_THRESHOLD:#if new>1&old>2, downgrade from trades to dollar
                updateRowStats(row,dictTradesToDollar)
                rowsProcessed+=1
            elif row["OldPrice"] < 1:#if new>1&old<1, upgrade from bulk to dollar
                updateRowStats(row,dictBulktoDollar)
                rowsProcessed+=1
            else:#if new>1&old>1, do nothing (ie, no change)
                updateRowStats(row,dictUnchangedCards)
                rowsProcessed+=1
        #check for downgrades to bulk
        elif row["NewPrice"] < 1:
            if row["OldPrice"] >= TRADE_BOX_THRESHOLD:#if new<1&old>2, downgrade from trades to bulk
                updateRowStats(row,dictTradesToBulk)
                rowsProcessed+=1
            elif row["OldPrice"] >= 1:#if new<1&old>1, downgrade from dollar to bulk
                updateRowStats(row,dictDollarToBulk)
                rowsProcessed+=1
            else:#if new<1&old<1, do nothing (ie, no change)
                updateRowStats(row,dictUnchangedCards)
                rowsProcessed+=1
            
print("total rows processed: " + str(rowsProcessed) + " out of (" + str(len(dfMergeCards)) + ")")
      
debug("dictNewCards: " + str(len(dictNewCards))+str(dictNewCards))     



printStats(dictGeneralStats, "Overall")
printStats(dictNewCards, "NewCards")
printStats(dictGoneCards, "GoneCards")
printStats(dictUnchangedCards, "UnchangedCards")
printStats(dictBulkToTrade, "Bulk Upgraded to Trade Box")
printStats(dictBulktoDollar, "Bulk Upgraded to Dollar Box")
printStats(dictDollarToTrade, "Dollar Upgraded to Trade Box")
printStats(dictDollarToBulk, "Dollar Downgraded to Bulk Box")
printStats(dictTradesToDollar, "Trades Downgraded to Dollar Box")
printStats(dictTradesToBulk, "Trades Downgraded to Bulk Box")

timeLoopEnd = timer()
print("Total time elapsed for loop: " + str(timeLoopEnd-timeLoopStart))


timeQueryStart = timer()

#query for bulk to trades (good)
dfBulkToTrades = dfMergeCards.query("(IsNew != True) & (OldPrice < 1) & (NewPrice >= "+ str(TRADE_BOX_THRESHOLD)+")")
#query for bulk to dollar (good)
dfBulkToDollar = dfMergeCards.query("(IsNew != True) & (OldPrice < 1) & ( (NewPrice >= 1) & (NewPrice < " + str(TRADE_BOX_THRESHOLD) +") )")
#query for dollar to trades (good)
dfDollarToTrades = dfMergeCards.query("(IsNew != True) & ( (OldPrice < "+str(TRADE_BOX_THRESHOLD)+") & (OldPrice >= 1) ) & (NewPrice >= "+ str(TRADE_BOX_THRESHOLD)+")")
#query for dollar to bulk (bad)
dfDollarToBulk = dfMergeCards.query("(IsNew != True) & ( (OldPrice < "+str(TRADE_BOX_THRESHOLD)+") & (OldPrice >= 1) ) & (NewPrice < 1)")
#query for trades to dollar (bad)
dfTradesToDollar = dfMergeCards.query("(IsNew != True) & (OldPrice > "+str(TRADE_BOX_THRESHOLD)+") & (NewPrice >= 1) & (NewPrice < "+str(TRADE_BOX_THRESHOLD) +")")
#query for trade to bulk (bad)
dfTradesToBulk = dfMergeCards.query("(IsNew != True) & (OldPrice > "+str(TRADE_BOX_THRESHOLD)+") & (NewPrice <1)")
#query for new cards
dfNewCards = dfMergeCards.query("(IsNew == True)")
#query for removed cards
dfGoneCards = dfMergeCards.query("(IsGone == True)")
#query for unch
dfUnchCards = dfMergeCards.query("(IsNew != True) & (IsGone != True) & (" \
    + " ( (OldPrice >= "+str(TRADE_BOX_THRESHOLD)+") & (NewPrice >= "+str(TRADE_BOX_THRESHOLD)+") )" \
    + "| ( ( (OldPrice >= 1) & (OldPrice < "+str(TRADE_BOX_THRESHOLD)+") ) & ( (NewPrice >= 1) & (NewPrice < "+str(TRADE_BOX_THRESHOLD)+") ) )" \
    + "| ( (OldPrice < 1) & (NewPrice < 1) )"\
    +")")

intBulkToTrades = len(dfBulkToTrades)
print("upgrades from bulk to trades: " + str(intBulkToTrades))
intBulkToDollar = len(dfBulkToDollar)
print("upgrades from bulk to dollar: " + str(intBulkToDollar))
intDollarToTrades = len(dfDollarToTrades)
print("upgrades from dollar to trades: " + str(intDollarToTrades))
intDollarToBulk = len(dfDollarToBulk)
print("dowgrades from dollar to bulk: " + str(intDollarToBulk))
intTradesToDollar = len(dfTradesToDollar)
print("dowgrades from trade to dollar: " + str(intTradesToDollar))
intTradesToBulk = len(dfTradesToBulk)
print("dowgrades from trade to bulk: " + str(intTradesToBulk))
intNewCards = len(dfNewCards)
print("new cards: " + str(intNewCards))
intGoneCards = len(dfGoneCards)
print("gone cards: " + str(intGoneCards))
intUnchCards = len(dfUnchCards)
print("unchanged cards: " + str(intUnchCards))
print("total cards from query: " + str(intBulkToTrades+intBulkToDollar+intDollarToTrades+intDollarToBulk+intTradesToDollar+intTradesToBulk+intNewCards+intGoneCards+intUnchCards))
print("total cards from row by row processing: " + str(len(dictGeneralStats)))

timeQueryEnd = timer()
print("Total time elapsed for query: " + str(timeQueryEnd-timeQueryStart))

dtScriptEnd = datetime.datetime.now()
print("Total time elapsed: " + str(dtScriptEnd.timestamp()-dtScriptStart.timestamp()))

updateRunLog(strCompareFileName,strTodayFileName,dictRunLog,dtScriptStart, dtScriptEnd, intBulkToTrades,intBulkToDollar,intDollarToTrades,intDollarToBulk,intTradesToDollar,intTradesToBulk,intNewCards,intGoneCards,intUnchCards)
