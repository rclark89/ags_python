# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 07:22:45 2022

@author: Ross

This is a script which aims to load AGS format data into a format usable by 
Python, and then divide each Group into data frames.

THIS IS A WORK IN PROGRESS!!!!!

The AGS data used here was downloaded from https://webapps.bgs.ac.uk/services/ngdc/accessions/index.html?titleDescription=Scaraway#item131407

Contains data supplied by Natural Environment Research Council.

Variable name explanation:
raw -> unedited from CSV
processed -> cleaned and formatted into a dataframe
proj/hole/geol etc -> variables outwith the functions
"""

import pandas as pd
import boto3

#S3 connection credentials.
s3 = boto3.resource(
    service_name='s3',
    region_name='XXXXXXXXXX',
    aws_access_key_id='XXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXX'
)

def AGS_raw(file_loc):
    raw_df = pd.read_csv(file_loc)
    #print(raw_df.head())
    return raw_df

def import_proj_data(import_df):
    
    #creates a new dataframe then selects rows 1 and 2 and colums up to 15 using iloc
    proj_raw = import_df.iloc[[0,2],0:15]
    
    #Raw dataframe headers are not suitable for proper analysis. 
    #Clean and recreate dataframe using correct values as headers.
    headers = proj_raw.iloc[0]
    proj_processed = pd.DataFrame(proj_raw.values[1:], columns=headers)
    return proj_processed

def import_hole_data(import_df):
    
    #Creates a new dataframe and then selects Rows 29-53 (HOLE section)
    hole_raw = import_df.iloc[29:53,:]
    
    #Clean and recreate dataframe
    headers = hole_raw.iloc[0]
    hole_processed = pd.DataFrame(hole_raw.values[1:], columns=headers)
    
    #Clean by dropping duplicates
    print(hole_processed.drop_duplicates(subset=['*HOLE_ID']))
    
    return hole_processed

def import_geol_data(import_df):
    
    #Creates a new dataframe and then selects rows containing GEOL info.
    geol_raw = import_df.iloc[293:469,0:4]
    
    #Clean and recreate dataframe
    headers = geol_raw.iloc[0]
    geol_processed = pd.DataFrame(geol_raw.values[1:], columns=headers)
    #geol_processed = geol_processed.drop([0])
    
    #Print out hole position information.
    print(geol_processed)
    
    return geol_processed

def convert_upload(df):
    #Converts output dataframes to CSV format and uploads to Python bucket.
    df.to_csv(r'C:\Users\Ross\Documents\AGS\upload\test.csv')
    
    s3.Bucket('ags-python-bucket').upload_file(
        r'C:\Users\Ross\Documents\AGS\upload\test.csv', 
        'new_file.csv'
        )

# Print out bucket name.
for bucket in s3.buckets.all():
    print("Current S3 bucket is",bucket.name)
    
#Check objects in bucket.
for obj in s3.Bucket('XXXXXXXXXXX').objects.all():
    print(obj)
    
#Download file and use locally.
s3.Bucket('XXXXXXXXXXXX').download_file(Key='1-CO106833.007 - 2019-07-04 1120 - Final - 1.csv', Filename=r'C:\Users\Ross\Documents\AGS\download.csv')
pd.read_csv(r'C:\Users\Ross\Documents\AGS\download.csv')

import_df = pd.DataFrame(AGS_raw(r'C:\Users\Ross\Documents\AGS\download.csv'))
proj = import_proj_data(import_df)
print("-------------------------------------------")
hole = import_hole_data(import_df)
print("-------------------------------------------")
geol = import_geol_data(import_df)
print("-------------------------------------------")

#test upload to S3 bucket.
convert_upload(proj)
