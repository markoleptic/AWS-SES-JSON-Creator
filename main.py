import numpy as np
import pandas as pd
import json

class BulkJsonFileCreator:
    def __init__(self, source, templateName, configSet, DefaultTemplateData):
        self.template = self.createSkeleton(source, templateName, configSet, DefaultTemplateData)
        self.newdf = pd.json_normalize(self.template)
        self.destinations = []

    def createSkeleton(self, source, templateName, configSet, DefaultTemplateData): 
        return {
            "Source": source,
            "Template": templateName,
            "ConfigurationSetName": configSet,
            "Destinations": [],
            "DefaultTemplateData":DefaultTemplateData
        }
    
    def fillFromCSV(self, csvFile):
        df = pd.read_csv(csvFile, header=0, index_col=False, low_memory=False)

        for index, row in df.iterrows():
            if pd.isnull(df.loc[index, "email"]):
                continue
            if not pd.isnull(df.loc[index, "sent"]):
                if (df.loc[index, "sent"] == 1.):
                    continue
            self.destinations.append({
                "Destination": {
                    "ToAddresses": [
                        row["email"]
                    ]
                },
                "ReplacementTemplateData": "{ \"name\":\"" + row["name"] + "\", \"activationkey\": \"" + row["key"] + "\" }"
            })
            
    def toJson(self):
        self.newdf.loc[0, "Destinations"] = self.destinations
        self.newdf.to_json("aws_ses_bulk_email.json", indent=2, orient='records')

myAWSemail = "jeff@aws.com"
myAWStemplateName = "SteamKeyTemplate"
myAWSconfigName = "app-config-set"
myAWSdefaultTemplateData = "{ \"name\":\"Streamer\", \"activationkey\":\"unknown\" }"

bulk = BulkJsonFileCreator(myAWSemail, myAWStemplateName, myAWSconfigName, myAWSdefaultTemplateData)
bulk.fillFromCSV("exampledata.csv")
bulk.toJson()