# Mess-around project to learn more python. 
# I organize my card collection according to price. Only $2 (although this might change) plus cards are kept in my trade box. 
# $1-2 cards are kept in a separate less accessed box. Anything under $1 is kept as a "bulk." Since prices change, it's a bit of a pain to check each card's value.
# This program will compare current prices to an older version of the inventory and produce a report reports:
# Report 1) Cards that dropped from trade box, $TRADE->$1; $TRADE->bulk w/ gross delta; by color, alphabatized
# Report 2) Cards that changed from dollar box, $1->$TRADE; $1->bulk w/gross delta; by color, alphabatized
# Report 3) Cards that increased from bulk box, bulk->$1; bulk-$TRADE w/gross delta; by color, alphabatized
# Overall change in value, any notes
#
# So I have to export from deckbox.org, then compare to old files
# Export doesn't have color, so I have to compare to a library fetched from mtgjson.org, if files aren't found then it probably means
# I need to update mtgjson.org (TODO:maybe check the filesize against header from https://mtgjson.com/json/AllCards.json.zip)
# 
# I'll schedule this to run every month or so
# Goals: learn csv with pandas, http stuff with requests, json stuff
#

import pandas
import datetime
import subprocess
import requests
import json
from pathlib import Path

#write a dictionary of http cookies to a local file
def makeCookies(cookies):
    with open("cookies.json","w") as file:
        json.dump(cookies,file)
    return

#read a dictionary of http cookies from a local file
def eatCookies():
    with open("cookies.json","r") as file:
        return json.load(file)

print("Hello World!!!!")

#first figure out today's date
today = datetime.datetime.now()
strToday = today.strftime("%Y%m%d")
print(strToday)

strTodayFileName = strToday+"-magic-cards.csv"
print(strTodayFileName)

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
if Path("data/" + strTodayFileName).exists():
    print("j'exist, donc il ne faut que je getter le file")
else:
    print("je n'exist pas, donc if faut que je getter le file")
    response = requests.get('https://deckbox.org/sets/export/1016639', headers=headers, params=params, cookies=cookies)
    #print(response.text)
    todayFile = open("data/" + strTodayFileName,"w")
    todayFile.write(response.text)
    todayFile.close()


