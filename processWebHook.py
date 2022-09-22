import flask 
import os
from urllib.request import Request, urlopen
import pandas as pd
from io import StringIO,BytesIO,TextIOWrapper
from zipfile import ZipFile
import re
import json
from flask import jsonify,request

app = flask.Flask(__name__)

@app.route('/')
@app.route('/home')
def home():
    return "hello World"

@app.route('/countmdrm')
def countrec():
    url = 'https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip'

    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    #resp = urllib.request.urlopen(url)

    zipfile = ZipFile(BytesIO(urlopen(req).read()))

    csvdata = TextIOWrapper(zipfile.open('MDRM_CSV.csv'),encoding='utf-8')

    data = pd.read_csv(csvdata,skiprows=1)

    count_row = data.shape[0]

    print(count_row)
    
    mdrmDataDict = data.to_dict('records')
    mdrmDataDictFilter = []
    count = 0
    for i in range(len(mdrmDataDict)):
        # print("ith row ,",i,"end date ,",mdrmDataDict[i]['End_Date'].split(' ')[0].replace('/', '-').split('-')[2])
        print('i ', i)
        enddate = mdrmDataDict[i]['End Date']
        # print(enddate)
        if enddate.split(' ')[0].replace('/', '-').split('-')[2] == '9999':
            # print('delete ',mdrmDataDict[i])
            mdrmDataDictFilter.append(mdrmDataDict[i])

    return jsonify(len(mdrmDataDictFilter))

@app.route('/countitemcode')
def countitemcode():
    url = 'https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip'

    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    # resp = urllib.request.urlopen(url)

    zipfile = ZipFile(BytesIO(urlopen(req).read()))

    csvdata = TextIOWrapper(zipfile.open('MDRM_CSV.csv'), encoding='utf-8')

    data = pd.read_csv(csvdata, skiprows=1)

    data.drop('Unnamed: 10', inplace=True, axis=1)
    data.drop('Mnemonic', inplace=True, axis=1)
    data.drop('Start Date', inplace=True, axis=1)
    data.drop('End Date', inplace=True, axis=1)
    data.drop('Confidentiality', inplace=True, axis=1)
    data.drop('SeriesGlossary', inplace=True, axis=1)
    data.drop('ItemType', inplace=True, axis=1)
    data.drop('Reporting Form', inplace=True, axis=1)

    # print(data)

    res = data.columns
    print(res)

    # dropping ALL duplicate values
    data.drop_duplicates(subset="Item Code",
                         keep='first', inplace=True)

    itemCodeDict = data.to_dict('records')
    print(itemCodeDict[1])
    print('len ', len(itemCodeDict))

    return jsonify(len(itemCodeDict))

@app.route('/itemcode')
def itemcode():
    startrange = request.args.get('startrange').split('.')[0]
    endrange = request.args.get('endrange').split('.')[0]
    print(startrange)
    print(endrange)

    url = 'https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip'

    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    # resp = urllib.request.urlopen(url)

    zipfile = ZipFile(BytesIO(urlopen(req).read()))

    csvdata = TextIOWrapper(zipfile.open('MDRM_CSV.csv'), encoding='utf-8')

    data = pd.read_csv(csvdata, skiprows=1)

    data.drop('Unnamed: 10', inplace=True, axis=1)
    data.drop('Mnemonic', inplace=True, axis=1)
    data.drop('Start Date', inplace=True, axis=1)
    data.drop('End Date', inplace=True, axis=1)
    data.drop('Confidentiality', inplace=True, axis=1)
    data.drop('SeriesGlossary', inplace=True, axis=1)
    data.drop('ItemType', inplace=True, axis=1)
    data.drop('Reporting Form', inplace=True, axis=1)

    # print(data)

    res = data.columns
    print(res)

    # dropping ALL duplicate values
    data.drop_duplicates(subset="Item Code",
                         keep='first', inplace=True)
    
    data.columns.values[0] = "Item_Code"
    data.columns.values[1] = "Item_Name"
    data.columns.values[2] = "Description"

    itemCodeDict = data.to_dict('records')
    print(itemCodeDict[1])
    print('len ',len(itemCodeDict))

    for itemcode in itemCodeDict:
        if type(itemcode['Description']) != str:
            itemcode['Description'] = 'Unavailable'
        else:
            itemcode['Description'] = itemcode['Description'].replace('<p style=\'white-space: pre-wrap;\'>','').replace('</p>','').replace('&#x0D;','').replace('&#x07;','')

    return json.dumps(itemCodeDict[int(startrange):int(endrange)], indent=2)

    

@app.route('/mdrmcsv')
def mdrmcsv():

    startrange = request.args.get('startrange').split('.')[0]
    endrange = request.args.get('endrange').split('.')[0]
    print(request.args.get('startrange'))
    print(request.args.get('endrange'))

    url = 'https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip'

    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    #resp = urllib.request.urlopen(url)

    zipfile = ZipFile(BytesIO(urlopen(req).read()))

    csvdata = TextIOWrapper(zipfile.open('MDRM_CSV.csv'),encoding='utf-8')

    data = pd.read_csv(csvdata,skiprows=1)

    data.drop('Unnamed: 10', inplace=True, axis=1)
    
    data.drop('Description', inplace=True, axis=1)

    data.drop('SeriesGlossary', inplace=True, axis=1)

    #print(data)

    data["Name"] = ""
    data["External_Id"] = ""

    res = data.columns
    print(res)

    data.columns.values[0] = "Mnemonic"
    data.columns.values[1] = "Item_Code"
    data.columns.values[2] = "Start_Date"
    data.columns.values[3] = "End_Date"
    data.columns.values[5] = "Confidentiality"
    data.columns.values[6] = "Item_Type"
    data.columns.values[7] = "Reporting_form"



    mdrmDataDict = data.to_dict('records')
    
    mdrmDataDictFilter = []

    for i in range(len(mdrmDataDict)):
        #print("ith row ,",i,"end date ,",mdrmDataDict[i]['End_Date'].split(' ')[0].replace('/', '-').split('-')[2])
        #print('i ',i)
        enddate = mdrmDataDict[i]['End_Date']
        #print(enddate)
        if enddate.split(' ')[0].replace('/', '-').split('-')[2] == '9999':
            #print('delete ',mdrmDataDict[i])
            mdrmDataDictFilter.append(mdrmDataDict[i])

    count=0

    # format the data
    for mdrm in mdrmDataDictFilter:
        count+=1
        if type(mdrm['Reporting_form']) != str:
            mdrm['Reporting_form'] = 'Unavailable'
        else:
            mdrm['Reporting_form'] = mdrm['Reporting_form'].strip()

        if mdrm['Name'] =='':
            # print('reporting form',type(mdrm['Reporting_form__c']))
            # print('Mnomonic',type(mdrm['Mnemonic__c']))
            # print('Item code',type(mdrm['Item_Code__c']))
            # print('Name',type(mdrm['Name']))
            mdrm['Name']= str(mdrm['Reporting_form'])+mdrm['Mnemonic']+str(mdrm['Item_Code'])
        
        if mdrm['External_Id'] == '':
            mdrm['External_Id'] = str(mdrm['Reporting_form']) + mdrm['Mnemonic'] + str(mdrm['Item_Code']) + str(count)
        
        if mdrm['Start_Date']:
            startdatesplit = mdrm['Start_Date'].split(' ')[0].replace('/','-').split('-')
            startdate = startdatesplit[2]+'-'+startdatesplit[0]+'-'+startdatesplit[1]
            mdrm['Start_Date'] = startdate

        if mdrm['End_Date']:
            enddatesplit = mdrm['End_Date'].split(' ')[0].replace('/','-').split('-')
            enddate = enddatesplit[2]+'-'+enddatesplit[0]+'-'+enddatesplit[1]
            mdrm['End_Date'] = enddate

    #print(type(d))

    print(count)
    print(mdrmDataDict[1])
    print(count)
    #mdrmcsvData = json.dumps(mdrmDataDict, indent=2)
    #return "success"
    return json.dumps(mdrmDataDictFilter[int(startrange):int(endrange)],indent=2)   
    #return jsonify(mdrmDataDict)
    
  

if __name__ == "__main__":
    app.debug = True
    app.run()
