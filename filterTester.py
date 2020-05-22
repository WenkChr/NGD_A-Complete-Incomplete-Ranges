#!/user/bin/env/python
# -*- coding: utf-8 -*-

import arcpy, os , sys, string
import pandas as pd
import numpy as np


arcpy.env.overwriteOutput = True

# Functions

def Addressfilter(AddressDataCSV, outDirectory, outGDB, outCSV_Name, out_name):
    # Street Name parser
    Suffix_List = [  'PL', 'ALLÉE', 'PLACE', 'ALLEY', 'FWY', 'PLAT', 'AUT', 'PLAZA', 'AVE', 'GDNS', 'PT', 'AV', 
     'POINTE', 'PVT', 'BEND', 'PROM', 'BLVD', 'GRNDS', 'QUAI', 'BOUL',  'HARBR','RAMP',
    'BYWAY', 'RANG',  'HTS', 'RG', 'CAPE', 'HGHLDS', 'CAR', 'HWY', 'RISE', 'CARREF', 'RD', 'CTR',  'RDPT', 'C', 'ÎLE', 'RTE', 
    'CERCLE', 'IMP', 'ROW',  'RUE', 'CH', 'RLE', 'CIR', 'KEY', 'RUN', 'CIRCT', 'SENT',  'SQ', 'LANE', 'ST',
    'CONC', 'LMTS', 'SUBDIV', 'CRNRS', 'LINE', 'TERR', 'CÔTE', 'LINK', 'TSSE', 'COUR', 'LKOUT', 'THICK', 'COURS', 'LOOP', 'CRT', 'MALL', 'TLINE', 
    'TRAIL', 'CRES', 'MAZE', 'TRNABT', 'CROIS', 'VALE', 'VIA', 'CDS', 'MONTÉE', 'DELL', 'MOUNT', 'VILLAS',
    'DIVERS', 'MTN',  'DOWNS', 'ORCH', 'VOIE', 'DR',  'ÉCH', 'PARC', 'WAY', 'END', 'PK', 'ESPL', 'PKY',  'PASS', 'WYND',
    'EXPY', 'PATH', 'EXTEN', 'PTWAY', 'CRCL', 'GT', 'PKWY', 'TRL', 'CRD', 'GRV', 'TER', 'LWN']
    # Removed: 'GLEN', MEADOW, CROSS, FRONT, 'CLOSE', 'BAY, 'WOOD',  'HEATH',  'RIDGE',  'HILL',  'INLET',  'MEWS', 'COVE', 'MANOR', 'ESTATE',  'ISLAND', 'BYPASS',
    # 'ABBEY', 'FARM', 'PINES', 'CAMPUS',  'TOWERS', 'PARADE', 'WALK', 'VISTA', 'GLADE', 'PORT', 'BEACH', 'DALE', 'MOOR', 'CHASE', 'ACRES', 'FIELD', 'GATE',
    ST_Directions = ['E', 'N', 'NE', 'NW', 'S', 'SE', 'SW', 'W', 'NO', 'SO', 'O']
    df = pd.read_csv(AddressDataCSV)
    # Removes all numeric charaters from the 
    df['NUMBER'] = df['NUMBER'].str.extract('(\d+)', expand = False)
    df['NUMBER_INT'] = pd.to_numeric(df['NUMBER'],errors='coerce')
    df['NUMBER_INT'] = df['NUMBER_INT'].astype(int, errors='ignore')
    
    df['Street_Name'] = ''
    for row in df.itertuples():
        Address_Split = row.STREET.split()
        del_indexes = []
        for item in Address_Split:
            if item.upper() in Suffix_List:
                df.at[row[0], 'Street_Suffix'] = Address_Split[Address_Split.index(item)]
                del_indexes.append(item)
            if item.upper() in ST_Directions:
                df.at[row[0], 'Street_Direction'] = Address_Split[Address_Split.index(item)]
                del_indexes.append(item)
        for i in del_indexes:
            Address_Split.remove(i)
        # From remaining segments constuct the street name
        AddressName = ''
        for item in Address_Split:
            outsegment = ''
            if len(AddressName) != 0:
                outsegment += ' '
            AddressName += outsegment + item
        df.at[row[0], 'Street_Name'] = AddressName
    
    print 'Exporting cleaned address data'
    df.drop(columns='OBJECTID')
    df.to_csv(os.path.join(outDirectory, outCSV_Name), index=False)  
    XY_Layer = arcpy.MakeXYEventLayer_management(os.path.join(outDirectory, outCSV_Name), 'LON', 'LAT', 'XY_Points')
    arcpy.FeatureClassToFeatureClass_conversion(XY_Layer, outGDB, out_name)
    return os.path.join(outGDB, out_name)      

# Constants
outDirectory = r'C:\Users\cwenkoff\Documents\CompletingRanges'
workingGDB = os.path.join(r'C:\Users\cwenkoff\Documents\CompletingRanges', 'workingGDB.gdb')
AddressIO_Database = r'C:\Users\cwenkoff\Documents\CompletingRanges\IO_Data.gdb'
TestArea = os.path.join(workingGDB, 'Toronto_CSD')
NGD_A = os.path.join(workingGDB, 'NGD_A')
Roads = os.path.join(workingGDB, 'TEST_AREA_NGD_AL_withStreets')
AddressPoints = os.path.join(AddressIO_Database, 'on')

# Function Calls
FilteredAddresses = Addressfilter(os.path.join(outDirectory, 'TorontoAddresses.csv'), outDirectory, workingGDB, 'Cleaned_Addresses.csv', 'Cleaned_Addresses')

print 'DONE!'
