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
# I need to update mtgjson.org 
# 
# I'll schedule this to run every month or so
# Goals: learn csv with pandas, http stuff with requests, json stuff
#

import datetime
import io
import json
import logging
import os
import re
import shutil
import smtplib
import subprocess
import sys
import zipfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from operator import itemgetter
from pathlib import Path
from time import strftime
from timeit import default_timer as timer

import pandas
import requests

dtScriptStart = datetime.datetime.now()

#write a dictionary of http cookies to a local file
def makeCookies(cookies):
    with open(COOKIE_FILE_NAME,"w") as file:
        json.dump(cookies,file)
    return

#read a dictionary of http cookies from a local file
def eatCookies():
    with open(COOKIE_FILE_NAME,"r") as file:
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
def writeRunLog(strTimestampKey,dictLogEntry):
    debug("writing the log")
    dictRunLog = readRunLog()
    dictRunLog[strTimestampKey] = dictLogEntry
    with open(RUN_LOG_FILE_NAME,"w") as file:
        json.dump(dictRunLog,file)


#figure out what the right file is to compare current file to, pass in fun file dict, return a file that exists in data
#the compare file should be the oldest, or the "new-file" from the last run log
def determineCompareFile(dictRunLog):
    #sort run log by old-file
    dictRunLog = sorted(dictRunLog.items(),key=itemgetter(0))
    runLogSize = len(dictRunLog)
    #print("size run log: " + str(runLogSize)+ str(dictRunLog) + "::::" + str(dictRunLog[runLogSize-1][1]["old-file"]))

    lastCompared = None
    lastNew = None

    #get the last item in the run log to find the last old-file 
    if runLogSize > 0:
        lastCompared = dictRunLog[runLogSize-1][1]["old-file"]
        lastNew = dictRunLog[runLogSize-1][1]["new-file"]

    print("LastCompared: "+str(lastCompared))
    print("LastNewFile: " +str(lastNew))

    #find all the csvs in data/
    listCardsCSVs = list(filter(lambda x: str(x).endswith("magic-cards.csv"),os.listdir(DATA_DIR_NAME)))
    #sort them all, oldest to newest by file name
    listCardsCSVs = sorted(listCardsCSVs)

    debug("listCardsCSVs:" + str(len(listCardsCSVs)) + str(listCardsCSVs))

    #find the lastCompared in list, default to -1 if no match
    indexLastCompared = None
    indexLastNew = None
    try:
        indexLastCompared = listCardsCSVs.index(lastCompared)
        indexLastNew = listCardsCSVs.index(lastNew)
    except ValueError:
        indexLastCompared = -1
        indexLastNew = -1

    #print("indexLastCompared: (-1 means I've never compared this file) " + str(indexLastCompared))
    print("indexLastNew: (-1 means I've never compared this file) " + str(indexLastNew))

    #if there's no last new match in the directory, use the oldest
    if (indexLastNew == -1):
        toCompareFileName = listCardsCSVs[0]
    else:
        toCompareFileName = lastNew


    #Check the next card csv file up chronologically
    #indexToCompare = indexLastCompared+1
    #there should not be a situation where there's not a file after the last compared, but if so, just run against the latest file
    #if (indexToCompare >= len(listCardsCSVs)):
    #    indexToCompare = len(listCardsCSVs)-1

    #debug("indexToCompare" + str(indexToCompare))

    #toCompareFileName = listCardsCSVs[indexToCompare]
    #print("toCompareFileName: " + str(toCompareFileName))
    return toCompareFileName

#load and return configuration dictionary from JSON and config logging
def configure():
    configureLogging()
    with open(CONFIG_FILE_NAME,"r") as file:
        return json.load(file)

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
    return row["SortCategory"]+"-"+row["Name"]+"-"+row["Edition"]+"-"+row["Condition"]+"-"+str(row["CardNumber"])+"-"+str(row["IsFoil"])

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
    netInventoryQuantityChange = sum(item["count-change"] for item in list)
    netValueChange = sum(item["total-change"] for item in list)
    quantityChangeNegative = 0
    quantityChangePositive = 0
    totalGain = 0.0
    totalLoss = 0.0

    for item in list:
        if item["total-change"] > 0:
            quantityChangePositive+=1
            totalGain+=item["total-change"]
        if item["total-change"] < 0:
            quantityChangeNegative +=1
            totalLoss+=item["total-change"]

    print("total items: " + str(len(list)))
    print("netInventoryQuantityChange:" + str(int(netInventoryQuantityChange)) + "; cardsIncreasedValue: " + str(quantityChangePositive) + "; cardsDecreasedValue: " + str(quantityChangeNegative))
    print("netValueChange: " + "${:,.2f}".format(netValueChange) + "; grossPositive: " + "${:,.2f}".format(totalGain) + "; grossNegative: " + "${:,.2f}".format(totalLoss))

#returns a string of stats for a card query derived data frame; totalquantity change, total price change, number cards, number up, number down
def stringStats(df):
    df = calcStatsDict(df)

    strStats = "Total cards: {total-cards} ({total-inventory} inv, net: {net-inventory-change-quantity}).".format(**df)
    strStats += " Changed quantity: {number-change-quantity}; Net: {net-change-quantity} ({net-inventory-change-quantity} inv). Gross increased: {count-positive-quantity}; Gross decreased: {count-negative-quantity}.".format(**df)
    strStats += " Changed price: {number-change-price} ({number-inventory-change-price} inv); Net: {net-change-price} ({net-inventory-change-price} inv). Gross increased: {count-positive-price} ({total-inventory-price-positive} inv); Gross decreased: {count-negative-price} ({total-inventory-price-positive} inv).".format(**df)
    strStats += " Total inventory value: ${total-value:,.2f}; Net value change: ${net-value-change:,.2f} (positive: ${total-gain:,.2f}; negative: ${total-loss:.2f}).".format(**df)

    return strStats

#sometimes I want a dataframe's stats formatted up for html
def htmlStats(df):
    df = calcStatsDict(df)

    html = "<table border=1 class=\"dataframe\" style=\"font-size : 16px\">"
    html += "<tr><td>"
    html += "Total cards:</td><td colspan=2> {total-cards} ({total-inventory} inv, net: {net-inventory-change-quantity}).".format(**df)
    html += "</td></tr>"
    html += "<tr><td>"
    html += "Changed quantity:</td><td> {number-change-quantity}; Net: {net-change-quantity} ({net-inventory-change-quantity} inv).</td><td>Gross increased: {count-positive-quantity}; Gross decreased: {count-negative-quantity}.".format(**df)
    html += "</td></tr>"
    html += "<tr><td>"
    html += "Changed price:</td><td> {number-change-price} ({number-inventory-change-price} inv); Net: {net-change-price} ({net-inventory-change-price} inv).</td><td>Gross increased: {count-positive-price} ({total-inventory-price-positive} inv); Gross decreased: {count-negative-price} ({total-inventory-price-negative} inv).".format(**df)
    html += "</td></tr>"
    html += "<tr><td>"
    html += " Total inventory value:</td><td> ${total-value:,.2f}.</td><td>Net value change: ${net-value-change:,.2f} (positive: ${total-gain:,.2f}; negative: ${total-loss:.2f}).".format(**df)
    html += "</td></tr></table>"
    
    return html

#returns a dictionary of a data frame's general stats; totalquantity change, total price change, number cards, number up, number down
def calcStatsDict(df):
    netInventoryQuantityChange = df["CountChange"].sum()
    quantityChangeNegative = df[df["CountChange"] < 0]["CountChange"].count()
    quantityChangePositive = df[df["CountChange"] > 0]["CountChange"].count()
    totalChangeQuantity = quantityChangeNegative + quantityChangePositive
    netChangeQuantity = quantityChangePositive - quantityChangeNegative
    
    priceChangeNegative = df[df["OldPrice"] > df["NewPrice"]]["NewCount"].count()
    priceChangePositive = df[df["OldPrice"] < df["NewPrice"]]["NewCount"].count()
    totalPriceChange = priceChangeNegative + priceChangePositive
    netPriceChange = priceChangePositive - priceChangeNegative
    totalInventoryPriceChangeNegative = df[df["OldPrice"] > df["NewPrice"]]["NewCount"].sum()
    totalInventoryPriceChangePositive = df[df["OldPrice"] < df["NewPrice"]]["NewCount"].sum()
    totalInventoryPriceChange = totalInventoryPriceChangeNegative + totalInventoryPriceChangePositive
    netInventoryPriceChange = totalInventoryPriceChangePositive - totalInventoryPriceChangeNegative
    
    totalInventory = df["NewCount"].sum()
    
    totalValue = df.eval("NewCount*NewPrice").sum()
    netValueChange = df["TotalChange"].sum()
    totalGain = df[df["TotalChange"] > 0]["TotalChange"].sum()
    totalLoss = df[df["TotalChange"] < 0]["TotalChange"].sum()

    return {"total-cards" : len(df), "total-inventory": totalInventory,
        "number-change-quantity" : totalChangeQuantity, "net-change-quantity" : netChangeQuantity, 
        "net-inventory-change-quantity" : netInventoryQuantityChange,
        "count-negative-quantity" : quantityChangeNegative, "count-positive-quantity" : quantityChangePositive,
        "count-negative-price" : priceChangeNegative, "count-positive-price" : priceChangePositive,
        "number-change-price" : totalPriceChange, "net-change-price" : netPriceChange,
        "total-inventory-price-negative" : totalInventoryPriceChangeNegative, "total-inventory-price-positive" : totalInventoryPriceChangePositive,
        "number-inventory-change-price" : totalInventoryPriceChange, "net-inventory-change-price" : netInventoryPriceChange,
        "net-value-change" : netValueChange, "total-gain" : totalGain, "total-loss" : totalLoss, "total-value" : totalValue }

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
    dfMergeCards = dfMergeCards[["Name","Edition","Condition","Foil","Card Number","Count","OldCount","Price","OldPrice"]]
    dfMergeCards = dfMergeCards.rename(index=str,columns={"Foil":"IsFoil","Card Number":"CardNumber","Count":"NewCount","Price":"NewPrice"})
    #dfMergeCards.info()
    #clean up foil flag
    dfMergeCards["IsFoil"].fillna(False,inplace=True)
    dfMergeCards["IsFoil"] = dfMergeCards["IsFoil"].replace("foil",True)

    #clean up null values in old set for new cards, set up a new field for new cards
    dfMergeCards["OldCount"].fillna(-1,inplace=True)
    dfMergeCards["OldCount"] = dfMergeCards["OldCount"].astype("int")
    dfMergeCards.eval("IsNew = (OldCount == -1)",inplace=True)
    dfMergeCards.loc[dfMergeCards["OldCount"] == -1, "OldCount"] = 0 
    dfMergeCards["OldPrice"].fillna(0.0,inplace=True)

    #clean up null values in new set for deleted cards, set up a new field for deleted cards
    dfMergeCards["NewCount"].fillna(-1,inplace=True)
    dfMergeCards["NewCount"] = dfMergeCards["NewCount"].astype("int")
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

    #reorder the columns how I like them
    dfMergeCards = dfMergeCards[["SortCategory","Name","Edition","Condition","IsFoil","CardNumber","OldCount","NewCount","OldPrice","NewPrice","IsNew","IsGone","CountChange","PriceChange","TotalChange"]]

    dfMergeCards = dfMergeCards.sort_values(by=["SortCategory","Name"])

    dfMergeCards.to_csv(DATA_DIR_NAME + "last-merged.csv")
    print("Comparing #TodayRecords to #CompareRecords in #MergedRecords" + str(len(dfTodaysCards)) + ":" + str(len(dfOldCards)) + ":" + str(len(dfMergeCards)))
    return dfMergeCards

#write a new log entry to the run log
def updateRunLog(
    strOldFileName,strNewFileName,dtScriptStart,dtScriptEnd,dictResultStats):
    #intTotalCardsProcessed = countBulkToTrades+countBulkToDollar+countDollarToTrades+countDollarToBulk+countTradeToDollar+countTradeToBulk+countNewCards+countGoneCards+countUnchCards
    dictLogEntry = {"old-file" : strOldFileName, "new-file" : strNewFileName, "elapsed-time" : (dtScriptEnd.timestamp()-dtScriptStart.timestamp())}
    dictLogEntry.update(dictResultStats)
    #dictRunLog[dtScriptStart.strftime("%Y%m%d-%H:%M:%S:%f")] = dictLogEntry
    #print(str(dictRunLog[dtScriptStart.strftime("%Y%m%d-%H:%M:%S:%f")]))
    writeRunLog(dtScriptStart.strftime("%Y%m%d-%H:%M:%S:%f"), dictLogEntry)

#send an email message
def sendMail(strHTML, dictConfig):
    #start = datetime.datetime.now().timestamp()
    server = smtplib.SMTP(host=dictConfig["outgoing-smtp"],port=dictConfig["smtp-port"])
    #print(str(datetime.datetime.now().timestamp()-start))
    server.starttls()
    #print(str(datetime.datetime.now().timestamp()-start))
    server.login(dictConfig["smtp-account-user"],dictConfig["smtp-account-pass"])
    #print(str(datetime.datetime.now().timestamp()-start))
    message = MIMEMultipart()
    message["from"] = dictConfig["from-email"]
    message["to"] = dictConfig["to-email"]
    message["subject"] = "Card comparison, go sort some cards"
    #print(str(datetime.datetime.now().timestamp()-start))
    message.attach(MIMEText(htmlString,"html"))
    server.send_message(message)
    #print(str(datetime.datetime.now().timestamp()-start))
    server.quit()
    #print(str(datetime.datetime.now().timestamp()-start))
    
#loops through a merged dataframe, processing for reports
#I did this initially before seeing that query/eval was quite faster (~.5 for query vs ~2 loop)
#keeping this as a method as it may be useful for debugging in the future
def loopDataFrame(df):
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
                
    debug("total rows processed: " + str(rowsProcessed) + " out of (" + str(len(dfMergeCards)) + ")")
        
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
    print("total cards from row by row processing: " + str(len(dictGeneralStats)))
    print("Total time elapsed for loop: " + str(timeLoopEnd-timeLoopStart))

#run the queries needed for reporting
#return a dictionary of all the query result dataframes, dictionary of stats
def queryForReports(df):
    timeQueryStart = timer()

    results = {}
    stats = {}
    
    #query for bulk to trades (good)
    dfBulkToTrades = dfMergeCards.query("(IsNew != True) & (OldPrice < 1) & (NewPrice >= "+ str(TRADE_BOX_THRESHOLD)+")")
    results["bulk-to-trades"] = dfBulkToTrades
    stats["count-bulk-to-trades"] = len(dfBulkToTrades)

    #query for bulk to dollar (good)
    dfBulkToDollar = dfMergeCards.query("(IsNew != True) & (OldPrice < 1) & ( (NewPrice >= 1) & (NewPrice < " + str(TRADE_BOX_THRESHOLD) +") )")
    results["bulk-to-dollar"] = dfBulkToDollar
    stats["count-bulk-to-dollar"] = len(dfBulkToDollar)

    #query for dollar to trades (good)
    dfDollarToTrades = dfMergeCards.query("(IsNew != True) & ( (OldPrice < "+str(TRADE_BOX_THRESHOLD)+") & (OldPrice >= 1) ) & (NewPrice >= "+ str(TRADE_BOX_THRESHOLD)+")")
    results["dollar-to-trades"] = dfDollarToTrades
    stats["count-dollar-to-trades"] = len(dfDollarToTrades)

    #query for dollar to bulk (bad)
    dfDollarToBulk = dfMergeCards.query("(IsNew != True) & ( (OldPrice < "+str(TRADE_BOX_THRESHOLD)+") & (OldPrice >= 1) ) & (NewPrice < 1)")
    results["dollar-to-bulk"] = dfDollarToBulk
    stats["count-dollar-to-bulk"] = len(dfDollarToBulk)

    #query for trades to dollar (bad)
    dfTradesToDollar = dfMergeCards.query("(IsNew != True) & (OldPrice > "+str(TRADE_BOX_THRESHOLD)+") & (NewPrice >= 1) & (NewPrice < "+str(TRADE_BOX_THRESHOLD) +")")
    results["trades-to-dollar"] = dfTradesToDollar
    stats["count-trades-to-dollar"] = len(dfTradesToDollar)

    #query for trade to bulk (bad)
    dfTradesToBulk = dfMergeCards.query("(IsNew != True) & (OldPrice > "+str(TRADE_BOX_THRESHOLD)+") & (NewPrice <1)")
    results["trades-to-bulk"] = dfTradesToBulk
    stats["count-trades-to-bulk"] = len(dfTradesToBulk)

    #query for new cards
    dfNewCards = dfMergeCards.query("(IsNew == True)")
    results["new-cards"] = dfNewCards
    stats["count-new-cards"] = len(dfNewCards)

    #query for removed cards
    dfGoneCards = dfMergeCards.query("(IsGone == True)")
    results["gone-cards"] = dfGoneCards
    stats["count-gone-cards"] = len(dfGoneCards)

    #query for unch- these are cards with no need to be moved from their location
    dfUnchCards = dfMergeCards.query("(IsNew != True) & (IsGone != True) & (" \
        + " ( (OldPrice >= "+str(TRADE_BOX_THRESHOLD)+") & (NewPrice >= "+str(TRADE_BOX_THRESHOLD)+") )" \
        + "| ( ( (OldPrice >= 1) & (OldPrice < "+str(TRADE_BOX_THRESHOLD)+") ) & ( (NewPrice >= 1) & (NewPrice < "+str(TRADE_BOX_THRESHOLD)+") ) )" \
        + "| ( (OldPrice < 1) & (NewPrice < 1) )"\
        +")")
    results["unch-cards"] = dfUnchCards
    stats["count-unch-cards"] = len(dfUnchCards)

    stats["count-all-results"] = stats["count-unch-cards"]+stats["count-gone-cards"]+stats["count-new-cards"] \
        +stats["count-trades-to-bulk"]+stats["count-trades-to-dollar"]+stats["count-dollar-to-bulk"]+stats["count-dollar-to-trades"] \
        +stats["count-bulk-to-dollar"]+stats["count-bulk-to-trades"]
    timeQueryEnd = timer()
    print("Total time elapsed for query: " + str(timeQueryEnd-timeQueryStart))

    return results, stats

#if a file for today doesn't exist, go fetch it from Deckbox and write it to strTodayFileName
#after this function, strTodayFileName should always exist
def fetchAndWriteDeckboxLibrary(strTodayFileName) :

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

#returns a temp dataframe with all columns renamed to have spaces before capital letters, this lets html line break the headers
def renameColsForHTML(df):
    #dictResults["trades-to-dollar"].rename(columns=lambda x: ' '.join(x.replace('_', ' ')[i:i+L] for i in range(0,len(x),L)) if df[x].dtype in ['float64','int64'] else x )    
    #for every capital letter except the first, replace it with " " plus itself
    return df.rename(columns=lambda x: re.sub("(?<!^)(?=[A-Z])",lambda y: " " + y.group(0),x))

#returns formatted html string from dataframe using the conventions of my reports
#basically I want left-aligned, wrapping column headers, links for card names, currency formatted, green background for positive
def toHTMLDefaulter(df):
    goodColor = "#8FBC8F"
    badColor = "#E9967A"
    formattedDF = renameColsForHTML(df)
    formattedDF[["Count Change"]] = formattedDF[["Count Change"]].applymap(lambda x: "<div style=\"background-color: " + (badColor if x < 0 else goodColor if x > 0 else "") +"\">" + str(x) + "</div>")
    formattedDF[["Price Change","Total Change"]] = formattedDF[["Price Change","Total Change"]].applymap(lambda x: "<div style=\"background-color: " + (badColor if x < 0 else goodColor if x > 0 else "") +"\">" + "${:,.2f}".format(float(x)) +"</div>")
    formattedDF[["Old Price","New Price"]] = formattedDF[["Old Price","New Price"]].applymap(lambda x: "${:,.2f}".format(float(x)))
    formattedDF = formattedDF.rename(index=str,columns={"Sort Category":"Sort","Condition":"Cond","Is Foil":"Foil","Card Number":"Card#","Old Count":"Old#","New Count":"New#","Old Price":"Old$","New Price":"New$","Is New":"New","Is Gone":"Del","Count Change":"\u0394Q","Price Change":"\u0394$","Total Change":"\u03a3\u0394$"},inplace=False)
    #print(str(formattedDF))
    html = formattedDF.to_html(index=False,justify="left",index_names=False,escape=False,float_format=lambda x: "${:,.2f}".format(float(x)),formatters={"Name": lambda x: "<a href=\"https://deckbox.org/mtg/" + x + "\" target=_blank>" + x + "</a>"})
    #print(html)
    return html

#make a relatively decent looking report that gets emailed out and written to disk
def buildHTMLReport(dfMergeCards,dictResults,dictResultStats):
    cssInlineStyle = """background-color: rgba(0, 0, 0, 0);
    border-bottom-color: rgb(0, 0, 0);
    border-bottom-style: none;
    border-bottom-width: 0px;
    border-collapse: collapse;
    border-image-outset: 0px;
    border-image-repeat: stretch;
    border-image-slice: 100%;
    border-image-source: none;
    border-image-width: 1;
    border-left-color: rgb(0, 0, 0);
    border-left-style: none;
    border-left-width: 0px;
    border-right-color: rgb(0, 0, 0);
    border-right-style: none;
    border-right-width: 0px;
    border-top-color: rgb(0, 0, 0);
    border-top-style: none;
    border-top-width: 0px;
    box-sizing: border-box;
    color: rgb(0, 0, 0);
    display: table;
    font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
    font-size: 12px;
    margin-left: 0px;
    margin-right: 0px;
    margin-top: 12px;
    text-size-adjust: 100%;
    -webkit-border-horizontal-spacing: 0px;
    -webkit-border-vertical-spacing: 0px;
    -webkit-tap-highlight-color: rgba(0, 0, 0, 0);"""

    #TODO-replace this with a template file for easier formatting
    pandas.set_option('display.max_colwidth', -1)#since I want links in html, I have to have a really long colwidth, -1=no limit, rather than figuring out max needed. I typically don't display so this doesn't matter
    htmlStringWriter = io.StringIO()
    htmlStringWriter.write("<html><style>.dataframe,body{" + cssInlineStyle + "}</style>")
    htmlStringWriter.write("<body>")
    htmlStringWriter.write("<h1>Comparing shifts in magic card prices in my library.</h1>")
    strPrettyTodayFileName = strTodayFileName.split("-")[0]
    strPrettyTodayFileName = datetime.date(int(strPrettyTodayFileName[0:4]),int(strPrettyTodayFileName[4:6]),int(strPrettyTodayFileName[6:8])).strftime("%B %d, %Y")
    strPrettyOldFileName = strOldFileName.split("-")[0]
    strPrettyOldFileName = datetime.date(int(strPrettyOldFileName[0:4]),int(strPrettyOldFileName[4:6]),int(strPrettyOldFileName[6:8])).strftime("%B %d, %Y")
    htmlStringWriter.write("<h2>" + strPrettyTodayFileName + " with " + strPrettyOldFileName +"</h2>")
    htmlStringWriter.write("<table border=0 class=\"dataframe\" style=\"font-size : 18px\">")
    htmlStringWriter.write("<tr><td>")
    htmlStringWriter.write("Total cards processed: </td><td><b>" + str(len(dfMergeCards)) +"</b>")
    htmlStringWriter.write("</td><td colspan=3>&nbsp;</td></tr>")
    htmlStringWriter.write("<tr><td>")
    htmlStringWriter.write("New cards:</td><td><b>" + str(dictResultStats["count-new-cards"]) + "</b>")
    htmlStringWriter.write("</td><td colspan=3>&nbsp;</td></tr>")
    htmlStringWriter.write("<tr><td>")
    htmlStringWriter.write("Gone cards:</td><td><b>" + str(dictResultStats["count-gone-cards"])  + "</b>")
    htmlStringWriter.write("</td><td colspan=3>&nbsp;</td></tr>")
    htmlStringWriter.write("<tr><td>")
    htmlStringWriter.write("Unchanged cards:</td><td><b>" + str(dictResultStats["count-unch-cards"])  + "</b> ")
    htmlStringWriter.write("</td><td colspan=3>&nbsp;</td></tr>")
    htmlStringWriter.write("<tr><td>")
    htmlStringWriter.write("Positive card shifts: </td><td>")
    htmlStringWriter.write("<b>" + str(dictResultStats["count-bulk-to-dollar"]+dictResultStats["count-bulk-to-trades"]+dictResultStats["count-dollar-to-trades"]) + "</b> ")
    htmlStringWriter.write("</td><td>")
    htmlStringWriter.write("From Dollar to Trades: <b>" + str(dictResultStats["count-dollar-to-trades"]) + "</b>; ")
    htmlStringWriter.write("</td><td>")
    htmlStringWriter.write("From Bulk to Trades: <b>" + str(dictResultStats["count-bulk-to-trades"]) + "</b>; ")
    htmlStringWriter.write("</td><td>")
    htmlStringWriter.write("From Bulk to Dollar: <b>" + str(dictResultStats["count-bulk-to-dollar"]) + "</b> ")
    htmlStringWriter.write("</td></tr>")
    htmlStringWriter.write("<tr><td>")
    htmlStringWriter.write("Negative card shifts: </td><td>")
    htmlStringWriter.write("<b>" + str(dictResultStats["count-trades-to-dollar"]+dictResultStats["count-trades-to-bulk"]+dictResultStats["count-dollar-to-bulk"]) + "</b> ")
    htmlStringWriter.write("</td><td>")
    htmlStringWriter.write("From Trades to Dollar: <b>" + str(dictResultStats["count-trades-to-dollar"]) + "</b>; ")
    htmlStringWriter.write("</td><td>")
    htmlStringWriter.write("From Trades to Bulk: <b>" + str(dictResultStats["count-trades-to-bulk"]) + "</b>; ")
    htmlStringWriter.write("</td><td>")
    htmlStringWriter.write("From Dollar to Bulk: <b>" + str(dictResultStats["count-dollar-to-bulk"]) + "</b> ")
    htmlStringWriter.write("</td></tr></table>")
    htmlStringWriter.write(htmlStats(dfMergeCards))
    htmlStringWriter.write("<hr/>")
    htmlStringWriter.write("<h1>Report #1 - Trades</h1>")
    htmlStringWriter.write("<h2>Trades downgraded to Dollar</h2>" + htmlStats(dictResults["trades-to-dollar"]))
    htmlStringWriter.write(toHTMLDefaulter(dictResults["trades-to-dollar"]))
    htmlStringWriter.write("<h2>Trades downgraded to Dollar</h2>" + htmlStats(dictResults["trades-to-bulk"]))
    htmlStringWriter.write(toHTMLDefaulter(dictResults["trades-to-bulk"]))
    htmlStringWriter.write("<h1>Report #2 - Dollar</h1>")
    htmlStringWriter.write("<h2>Dollar upgrades to Trades</h2>" + htmlStats(dictResults["dollar-to-trades"]))
    htmlStringWriter.write(toHTMLDefaulter(dictResults["dollar-to-trades"]))
    htmlStringWriter.write("<h2>Dollar downgraded to Bulk</h2>" + htmlStats(dictResults["dollar-to-bulk"]))
    htmlStringWriter.write(toHTMLDefaulter(dictResults["dollar-to-bulk"]))
    htmlStringWriter.write("<h1>Report #3 - Bulk</h1>")
    htmlStringWriter.write("<h2>Bulk upgraded to Trades</h2>" + htmlStats(dictResults["bulk-to-trades"]))
    htmlStringWriter.write(toHTMLDefaulter(dictResults["bulk-to-trades"]))
    htmlStringWriter.write("<h2>Bulk upgraded to Dollar</h2>" + htmlStats(dictResults["bulk-to-dollar"]))
    htmlStringWriter.write(toHTMLDefaulter(dictResults["bulk-to-dollar"]))
    htmlStringWriter.write("</body></html>")
    htmlString = htmlStringWriter.getvalue()
    htmlStringWriter.close()
    return htmlString

#read in and return today's CSV as DF, determine appropriate old CSV as DF, and the old file name for use later
def buildCompareDFs(strTodayFileName):
    #get today's file
    dfTodaysCards = pandas.read_csv(DATA_DIR_NAME + strTodayFileName)
    dfTodaysCards = cleanCardDataFrame(dfTodaysCards)

    #getting older file is a bit trickier, check the run log, find the most recent run, find the old file used, get the next recent old file to compare with
    dictRunLog = readRunLog()

    strOldFileName = determineCompareFile(dictRunLog)
    print("ToCompareAgainst: " + strOldFileName)

    dfOldCards = pandas.read_csv(DATA_DIR_NAME + strOldFileName)
    dfOldCards = cleanCardDataFrame(dfOldCards)
    dfOldCards = dfOldCards.rename(index=str,columns={"Count":"OldCount","Price":"OldPrice"})

    return dfTodaysCards,dfOldCards,strOldFileName

MAGIC_CARD_JSON_URL = "https://mtgjson.com/json/AllCards.json.zip"
DATA_DIR_NAME = "data/"
RUN_LOG_FILE_NAME = DATA_DIR_NAME + "run-log.json"
CONFIG_FILE_NAME = "config.json"
COOKIE_FILE_NAME = "cookies.json"
TRADE_BOX_THRESHOLD = 2 #this might change, but it's $2 for now

print("Hello World!!!!")

dictConfig = configure()

strToday = dtScriptStart.strftime("%Y%m%d")
strTodayFileName = strToday+"-magic-cards.csv"
print("CSV that I want for today: " + strTodayFileName)

fetchAndWriteDeckboxLibrary(strTodayFileName)

print("OK cool, now I have a CSV of my library, a dictionary of every magic card ever that's up to date. Now I can check for price diffs")

dfTodaysCards,dfOldCards,strOldFileName = buildCompareDFs(strTodayFileName)
dfMergeCards = buildMergeDF(dfTodaysCards,dfOldCards) 
dictResults, dictResultStats = queryForReports(dfMergeCards)

#all the work is done, now just print the reports, first the changes from bulk
htmlString = buildHTMLReport(dfMergeCards,dictResults,dictResultStats)

with open(DATA_DIR_NAME + strToday + "-report.htm","w", encoding="utf-16") as file:
    file.write(htmlString)

sendMail(htmlString, dictConfig)

dtScriptEnd = datetime.datetime.now()
print("Total time elapsed: " + str(dtScriptEnd.timestamp()-dtScriptStart.timestamp()))

updateRunLog(strOldFileName,strTodayFileName,dtScriptStart, dtScriptEnd, dictResultStats)
