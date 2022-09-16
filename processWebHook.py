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

@app.route('/mdrmcsv')
def mdrmcsv():

    startrange = request.args.get('startrange')
    endrange = request.args.get('endrange')
    print(request.args.get('startrange'))
    print(request.args.get('endrange'))

    url = 'https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip'

    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    #resp = urllib.request.urlopen(url)

    zipfile = ZipFile(BytesIO(urlopen(req).read()))

    csvdata = TextIOWrapper(zipfile.open('MDRM_CSV.csv'),encoding='utf-8')

    data = pd.read_csv(csvdata,skiprows=1)

    data.drop('Unnamed: 10', inplace=True, axis=1)

    #print(data)

    data["Name"] = ""

    res = data.columns
    print(res)

    data.columns.values[0] = "Mnemonic__c"
    data.columns.values[1] = "Item_Code__c"
    data.columns.values[2] = "Start_Date__c"
    data.columns.values[3] = "End_Date__c"
    data.columns.values[5] = "Confidentiality__c"
    data.columns.values[6] = "Item_Type__c"
    data.columns.values[7] = "Reporting_form__c"



    mdrmDataDict = data.to_dict('records')

    count=0

    # format the data
    for mdrm in mdrmDataDict:

        if mdrm['Name'] =='':
            # print('reporting form',type(mdrm['Reporting_form__c']))
            # print('Mnomonic',type(mdrm['Mnemonic__c']))
            # print('Item code',type(mdrm['Item_Code__c']))
            # print('Name',type(mdrm['Name']))
            mdrm['Name']= str(mdrm['Reporting_form__c'])+mdrm['Mnemonic__c']+str(mdrm['Item_Code__c'])

        if mdrm['Description']:
            mdrm['Description'] = re.sub('<[^<]+?>', '', str(mdrm['Description']))
            mdrm['Description'] = mdrm['Description'].replace('&#x0D;','')
            count += 1
            #print(mdrm['Description'])

        if mdrm['Start_Date__c']:
            mdrm['Start_Date__c'] = mdrm['Start_Date__c'].split(' ')[0]

        if mdrm['End_Date__c']:
            mdrm['End_Date__c'] = mdrm['End_Date__c'].split(' ')[0]

    #print(type(d))

    print(count)
    print(mdrmDataDict[1])
    #mdrmcsvData = json.dumps(mdrmDataDict, indent=2)
    #return "success"
    return jsonify(mdrmDataDict[int(startrange):int(endrange)])   

if __name__ == "__main__":
    app.debug = True
    app.run()