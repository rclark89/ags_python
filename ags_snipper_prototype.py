# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 07:22:45 2022

@author: Ross

This is a script which aims to load AGS format data into a format usable by 
Python, and then divide each Group into data frames.

The Scaraway Street AGS data used here was downloaded from https://webapps.bgs.ac.uk/services/ngdc/accessions/index.html?titleDescription=Scaraway#item131407
The 4 The Loanings Motherwell data was downloaded from https://webapps.bgs.ac.uk/services/ngdc/accessions/index.html?#item62653
Beith data downloaded from https://webapps.bgs.ac.uk/services/ngdc/accessions/index.html?#item62649

Contains data supplied by Natural Environment Research Council.

Future ideas:
    
1. define class (for later) - this will convert a raw AGS text file to a format
that Python can deal with, I'll probably stick with CSV.

"""

import pandas as pd
import boto3
import tkinter as tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure



#S3 connection credentials.
s3 = boto3.resource(
    service_name='s3',
    region_name='XXXXXXXXX',
    aws_access_key_id='XXXXXXXXXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXx'
)

def check_aws_bucket():
    # Print out bucket name.
    for bucket in s3.buckets.all():
        print("Current S3 bucket is",bucket.name)
    
    #Check objects in bucket.
    for obj in s3.Bucket('ags-python-bucket').objects.all():
        print(obj)
    
#Download file and use locally.
#s3.Bucket('ags-python-bucket').download_file(Key='1-CO106833.007 - 2019-07-04 1120 - Final - 1.csv', Filename=r'C:\Users\Ross\Documents\AGS\download.csv')
#pd.read_csv(r'C:\Users\Ross\Documents\AGS\download.csv')

#test


class CreateDataFrame: #This class creates raw dataframe objects for manipulation by other functions.
    def __init__(self,file_loc):
        self.file_loc = file_loc
    
    def AGS_raw(self):
        raw_df = pd.read_csv(self.file_loc)
        #print(raw_df.head()) - used for initial testing.
        return raw_df

    def imported_data(self):
        raw = CreateDataFrame(self.file_loc).AGS_raw() #Read csv and import.
        import_data = pd.DataFrame(raw) #Create dataframe from raw csv data.
        return import_data

class ProduceDataFrame: 
    def __init__(self,import_df):
        self.import_df = import_df

    def import_proj_data(self):
        pass

def import_proj_data(import_df):
    
    #creates a new dataframe then selects rows 1 and 2 and colums up to 15 using iloc
    proj_raw = import_df.iloc[[0,2],:]
    #print(proj_raw)

    #Raw dataframe headers are not suitable for proper analysis. 
    #Clean and recreate dataframe using correct values as headers.
    headers = proj_raw.iloc[0]
    proj_processed = pd.DataFrame(proj_raw.values[1:], columns=headers)
    print(proj_processed)
    
    return proj_processed

"""
def AGS_raw(file_loc):
    raw_df = pd.read_csv(file_loc)
    #print(raw_df.head())
    return raw_df
"""


def import_hole_data(import_df,row_start,row_end):
    
    #Creates a new dataframe and then selects Rows 29-53 (HOLE section)
    hole_raw = import_df.iloc[row_start:row_end,0:4]
    
    #Clean and recreate dataframe
    headers = hole_raw.iloc[0]
    hole_processed = pd.DataFrame(hole_raw.values[1:], columns=headers)
    
    #Print out hole position information.
    #print(hole_processed.iloc[[1,4,7,10,12,14,16,18,20],0:7])
    
    print(hole_processed.drop_duplicates())
    #subset=['*HOLE_ID'])
    return hole_processed

def import_geol_data(import_df,row_start,row_end):
    
     #Creates a new dataframe and then selects rows containing GEOL info.
    geol_raw = import_df.iloc[row_start:row_end,0:4]
    
    #Clean and recreate dataframe
    headers = geol_raw.iloc[0]
    geol_processed = pd.DataFrame(geol_raw.values[1:], columns=headers)
    #geol_processed = geol_processed.drop([0])
    
    #Print out hole position information.
    print(geol_processed)
    
    return geol_processed

def import_ispt_data(import_df,row_start,row_end):
    
    #Creates a new dataframe and then selects rows containing ISPT info.
    ispt_raw = import_df.iloc[row_start:row_end,0:7]
    
    #Clean and recreate dataframe
    headers = ispt_raw.iloc[0]
    ispt_processed = pd.DataFrame(ispt_raw.values[3:], columns=headers).dropna()
    
    #Print out hole position information.
    print(ispt_processed)
    
    return ispt_processed

def convert_upload(df):
    #Converts output dataframes to CSV format and uploads to Python bucket.
    df.to_csv(r'C:\Users\Ross\Documents\AGS\upload\test.csv')
    
    s3.Bucket('ags-python-bucket').upload_file(
        r'C:\Users\Ross\Documents\AGS\upload\test.csv', 
        'new_file.csv'
        )
    
def convert_only(df):
    #Converts output dataframes to CSV format.
    df.to_csv(r'C:\Users\Ross\Documents\AGS\upload\test.csv')
    
def spt_to_tk(input_df):
    
    local_df = input_df.sort_values(by='*ISPT_TOP', ascending=True)
    
    window = tk.Tk() #Create main Tk window
    window.title('SPT N Value v Depth') #window title
    window.geometry('750x750') #window geometry
    
    fig = Figure(figsize = (10,10), dpi = 100) #Create figure that contains plot
    plot1 = fig.add_subplot(111) #add subplot
    
    x = local_df['*ISPT_NVAL'].astype(str).astype(float) #Convert to a usable format.
    y = local_df['*ISPT_TOP'].astype(str).astype(float) #Convert to a usable format.
    
    plot1.scatter(x,y) #plot the graph
    #plot1.title('SPT vs Depth plot')
    plot1.set_xlabel('SPT N Value')
    plot1.set_ylabel('Depth')
    
    canvas = FigureCanvasTkAgg(fig, master = window) #Create the Tkinter canvas containing the matplotlib figure
    canvas.draw()
    canvas.get_tk_widget().pack()
    
    window.mainloop() #run the gui


#import_df = pd.DataFrame(AGS_raw(r'C:\Users\Ross\Documents\AGS\download.csv'))
#import_data = pd.DataFrame(AGS_raw(r'C:\Users\Ross\Documents\AGS\1807 TOTAL.csv'))
#print(import_df.head())

"""
#Scaraway Street
print("-------------------------------------------")
proj = import_proj_data(import_df)
print("-------------------------------------------")
hole = import_hole_data(import_df,29,53)
print("-------------------------------------------")
geol = import_geol_data(import_df,293,469)
print("-------------------------------------------")
"""

print('Beith')
print("-------------------------------------------")
import_data = CreateDataFrame(r'C:\Users\Ross\Documents\AGS\1807 TOTAL.csv').imported_data() #Create object comprising dataframe of entire CSV file.
import_proj_data(import_data)
print("-------------------------------------------")
#hole = import_hole_data(import_data,29,53)
print("-------------------------------------------")
#geol = import_geol_data(import_data,293,469)
print("-------------------------------------------")
ispt = import_ispt_data(import_data,1159,1239)

print("-------------------------------------------")
