#!/user/bin/env/python
# -*- coding: utf-8 -*-

import arcpy, os , sys
import pandas as pd
import numpy as np


arcpy.env.overwriteOutput = True
# Function Definitions

def df_uniqueValues(df, column):
    # returns a list of all unique values for the given column from the input dataframe
    #df = pd.read_csv(csv)
    uniques = df[column].dropna().unique().tolist()
    # If the length of the uniques is greater than 0 return it else return None
    if len(uniques)>= 1:
        return uniques
    else: 
        return None

def if_none_to_emtpy_list(var):
    # if input var's type is none then return it as an empty list
    if var is None:
        return []
    return var

def ArcPylockBreaker(Infile):
    # deletes a file if it exists
    if arcpy.Exists(Infile):
        arcpy.Delete_management(Infile)

def CSV_ToGDB_Table(table, workingGDB, outName):
    # converts a csv to a gdb table and adds oid's so the data can be projected is necessary minimizing errors
    # Required Pandas and numpy
    # SRC- https://my.usgs.gov/confluence/display/cdi/pandas.DataFrame+to+ArcGIS+Table
    df = pd.read_csv(table)
    x = np.array(np.rec.fromrecords(df.values))
    names = df.dtypes.index.tolist()
    x.dtype.names = tuple(names)
    OutFile = os.path.join(workingGDB, outName)
    # if outfile already exists delete it 
    ArcPylockBreaker(OutFile)
    arcpy.da.NumPyArrayToTable(x, OutFile)
    return OutFile

def Study_Area_Clipper(ClipAreaFile, FileToClip, outGDB, outName):
    # Clips an input fc to a study area to test the logic of the script
    # Takes the TestAreaFile and clips the FileToClip to its area
    print 'Clipping file to study area'
    OutFC = os.path.join(outGDB, outName)
    arcpy.Clip_analysis(FileToClip, ClipAreaFile, OutFC)
    return OutFC

def fc_to_csv_writer(fc, outDirectory, csv_name, field_list, null_value= -999):
    import unicodecsv as csv
    # Converts input fc or table into a csv
    # Gets all field names and puts them in a list
    fields = [f.name for f in arcpy.ListFields(fc)]
    for f in field_list:
        if f not in fields:
            print f + ' Not in the feature class'
            sys.exit()
    print 'Exporting fc to csv'
    with open(os.path.join(outDirectory, csv_name), 'wb') as f:
        writer = csv.writer(f, encoding= 'utf-8')
        writer.writerow(field_list)
        with arcpy.da.SearchCursor(fc, field_list) as cursor:
            for row in cursor:
                writer.writerow(row)
    return os.path.join(outDirectory, csv_name)

def fieldDeleter(inFile, KeepFieldList):
    #Deletes all fields not in the list of provided fields
    for f in arcpy.ListFields(inFile):
        fields = KeepFieldList
        if f.name in fields or f.required is True:
            continue
        else:
            arcpy.DeleteField_management(inFile, f.name)

def AddressCleaner(AddressDataCSV, outDirectory, outGDB, out_name, outCSV_Name = 'Cleaned_Addresses.csv', Address_Field='STREET'):
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
            # if item is a number change the number int fiekld to match this (some sources mark the unit number in NUMBER field)
            if item.isdigit():
                df.at[row[0], 'NUMBER_INT'] = Address_Split[Address_Split.index(item)]
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

def RangeMaker(NGD_A, AddressPoints, workingGDB, outTableName):
    print 'Joining NGD_A with addresses'
    AddWith_NGD_A = ''
    if arcpy.Exists(os.path.join(workingGDB, 'Address_NGD_A_sj')) is False:
        AddWith_NGD_A = arcpy.SpatialJoin_analysis(AddressPoints,  NGD_A, os.path.join(workingGDB, 'Address_NGD_A_sj'))
    AddWith_NGD_A = os.path.join(workingGDB, 'Address_NGD_A_sj')
    print 'Calculating Statistics'
    # finds the min and max for each 
    arcpy.Statistics_analysis(AddWith_NGD_A, os.path.join(workingGDB, outTableName), statistics_fields= [['NUMBER_INT', 'MIN'], ['NUMBER_INT', 'MAX']], 
                                            case_field= ['BB_UID','Street_Name'])
    return os.path.join(workingGDB, outTableName)

def Range_Flagger(NGD_AL_df, NGD_UID, NewAT_VAL, NGD_STR_UID, Direction):
    # If the NewAT_VAL is used on the street ID anywhere flag as manual_check
    # Find all rows where the NGD_STR_UID matches the input STR_UID and exclude the current NGD_UID 
    NGD_AL_df = NGD_AL_df[(NGD_AL_df['NGD_STR_UID_' + Direction] == NGD_STR_UID) & (NGD_AL_df['NGD_UID'] != NGD_UID)]

def BestNumberSelector(inputRow, NGD_UID, rowAF_VAL, NGD_STR_UID, BB_UID, Street_Name, AddressPointDF, NGD_AL_df, Direction):
    # Returns the best new AT number from the list of options in the BB
    # Select all points in the BB with the same street name
    AddressPoints_in_BB = AddressPointDF.loc[(AddressPointDF['BB_UID'] == BB_UID) & (AddressPointDF['Street_Name_Upper'] == Street_Name)]
    # Get all unique number values from the selected points that fall within the BB with the same street name
    AddressPoints_in_BB_StreetNumbers = df_uniqueValues(AddressPoints_in_BB, 'NUMBER_INT')
    # Get all road segments with the same NGD_STR_UID and the same BB_UID dependent on direction
    NGD_AL_SameStreet = NGD_AL_df.loc[(NGD_AL_df['NGD_STR_UID_' + Direction] == NGD_STR_UID) & (NGD_AL_df['NGD_UID'] != NGD_UID)]
    
    # Get all unique AF and AT values from the NGD_AL_SameStreet df
    Unique_AF_Vals = df_uniqueValues(NGD_AL_SameStreet, 'AF' + Direction + '_VAL') 
    Unique_AT_Vals = df_uniqueValues(NGD_AL_SameStreet, 'AT' + Direction + '_VAL')
    # If both lists are of None type then take the greatest value from the point values and return that
    if Unique_AF_Vals == None and Unique_AT_Vals == None:
        maxStreetNumber = max(AddressPoints_in_BB_StreetNumbers)
        return [maxStreetNumber, 'StreetAddressOnly']
    # if values are returned then use them to limit the range of potential point values
    else:    
        # Get only the unique vales from the AF and AT values lists
        Unique_AF_Vals = if_none_to_emtpy_list(Unique_AF_Vals)
        Unique_AT_Vals = if_none_to_emtpy_list(Unique_AT_Vals)
        
        AF_AL_Uniques = list(set(Unique_AF_Vals + Unique_AT_Vals))
        AF_AL_Uniques = [int(i) for i in AF_AL_Uniques]
        #print AF_AL_Uniques
        # return the min value from the AF_AT lists that is greater than the rows AF value
        AF_AT_Greater = [i for i in AF_AL_Uniques if i > rowAF_VAL]
        if len(AF_AT_Greater) >= 1:
            min_AF_AT_Greater = min(AF_AT_Greater)
            potential_new_AT_vals = [int(i) for i in AddressPoints_in_BB_StreetNumbers if int(i) < int(min_AF_AT_Greater)]
            if len(potential_new_AT_vals) == 0:
                return [int(rowAF_VAL), 'rowAF_VAl']
            else:
                new_AT_Val = max(potential_new_AT_vals)
                return [new_AT_Val, 'AF_AT reduced']
        else: 
            maxStreetNumber = max(AddressPoints_in_BB_StreetNumbers)
            return  [maxStreetNumber, 'StreetAddressOnly']

def IncompleteRangeFlagger(RangeData_csv, RoadData_csv, AllAddresses_csv, Direction, outDirectory):
    # Direction is L or R
    # Make Range dataframe and then select all rows that fall into the incomplete range format.
    AF_AT = ['AF' + Direction, 'AT' + Direction]
    # Set up range and road df's and all aaddresses df
    Range_df = pd.read_csv(RangeData_csv)
    Range_df['Street_Name_Upper'] = Range_df['Street_Name'].str.upper()
    
    NGD_AL_USE_COLS = ['NGD_UID', 'BB_UID_' + Direction, AF_AT[0] + '_VAL', AF_AT[1] + '_VAL', 'STR_NME', 'NGD_STR_UID_' + Direction]
    NGD_AL_df = pd.read_csv(RoadData_csv, usecols= NGD_AL_USE_COLS)    
    COL_IndexDict = {}
    # Get index numbers for all coulmns in the use cols list
    for col in NGD_AL_USE_COLS:
        COL_IndexDict[col] =  NGD_AL_df.columns.get_loc(col) 
    # Setup all addresses DF and create the Street_Name_Upper field
    AllAddresses_df = pd.read_csv(AllAddresses_csv)
    AllAddresses_df['Street_Name_Upper'] = AllAddresses_df['Street_Name'].str.upper()
    # Find all incomplete ranges in road df
    IncompleteRanges_df = NGD_AL_df[(NGD_AL_df[AF_AT[0] + '_VAL'].notna()) & (NGD_AL_df[AF_AT[1] + '_VAL'].isnull())]
    # Set up out df and set column names
    outDF = pd.DataFrame(columns=('NGD_UID', 'NGD_STR_UID', AF_AT[0] + '_VAL', 'New_' + AF_AT[1] + '_VAL', 'AT_Flag', 'NGD_STR_NME', 'IO_MIN', 'IO_MAX', 'IO_STR_NME', 
                                'Process_Flag'))
    # Iterate through incomplete ranges and determine if there is a match in the address ranges DF
    for row in IncompleteRanges_df.itertuples(index= False):
        Street_Name = row[COL_IndexDict['STR_NME']].upper()
        BB_STREET_Match = Range_df.loc[(Range_df['BB_UID'] == row[COL_IndexDict['BB_UID_' + Direction]]) & (Range_df['Street_Name_Upper'] == Street_Name )]
        if len(BB_STREET_Match.index) == 0: 
            continue
        # Run the BestNumberSelector function to determine the best new AT from the range of values in the block
        Best_AT = BestNumberSelector(row, row[COL_IndexDict['NGD_UID']], row[COL_IndexDict[AF_AT[0] + '_VAL']], row[COL_IndexDict['NGD_STR_UID_' + Direction]], 
                                    row[COL_IndexDict['BB_UID_' + Direction]], Street_Name, AllAddresses_df, NGD_AL_df, Direction)

        # Write new row to export DF
        new_index = len(outDF) + 1
        outDF.loc[new_index] = [row[COL_IndexDict['NGD_UID']], row[COL_IndexDict['NGD_STR_UID_' + Direction]], row[COL_IndexDict[ AF_AT[0] + '_VAL']], 
                                Best_AT[0], Best_AT[1], row[COL_IndexDict['STR_NME']], BB_STREET_Match['MIN_NUMBER_INT'].iloc[0], 
                                BB_STREET_Match['MAX_NUMBER_INT'].iloc[0], BB_STREET_Match['Street_Name'].iloc[0], 
                                Range_Flagger(NGD_AL_df, row[COL_IndexDict['NGD_UID']], BB_STREET_Match['MAX_NUMBER_INT'].iloc[0], COL_IndexDict['NGD_STR_UID_' + Direction],
                                            Direction)]
    #Removed range flagger call until AT assigner works
    # Range_Flagger(NGD_AL_df, row[COL_IndexDict['NGD_UID']], BB_STREET_Match['MAX_NUMBER_INT'].iloc[0], COL_IndexDict['NGD_STR_UID_' + Direction],
    #                             Direction)
    # Export outDF as csd
    outDF.to_csv(os.path.join(outDirectory, 'RangeCompleterTests.csv'), index= False)

# Constants
# Created before running tool: imported NGD files into workingGDB, joined the NGD_STREET table to the NGD_AL on the NGD_STREET_ID_L field
outDirectory = r'C:\Users\cwenkoff\Documents\CompletingRanges'
workingGDB = os.path.join(r'C:\Users\cwenkoff\Documents\CompletingRanges', 'workingGDB.gdb')
AddressIO_Database = r'C:\Users\cwenkoff\Documents\CompletingRanges\IO_Data.gdb'
TestArea = os.path.join(workingGDB, 'Toronto_CSD')
NGD_A = os.path.join(workingGDB, 'NGD_A')
Roads = os.path.join(workingGDB, 'NGD_AL_JoinedStreets')
AddressPoints = os.path.join(AddressIO_Database, 'on')

# Function Calls
# outputs of this process to be a flat file with flag on whether to change the AT(L or R) value or to have it checked manually or not to change 
# print 'Clipping Toronto Addresses'
# TorontoAddresses = Study_Area_Clipper(TestArea, AddressPoints, workingGDB, 'TorontoAddresses')
# TorontoAddresses = fc_to_csv_writer(TorontoAddresses, outDirectory, 'TorontoAddresses.csv', 
#                                     ['OBJECTID', 'LON', 'LAT', 'NUMBER', 'STREET', 'UNIT', 'CITY', 'SOURCE'])
# print 'Cleaning Toronto Addresses'
# Cleaned_Addresses = AddressCleaner(TorontoAddresses, outDirectory, workingGDB, 'TorontoCleanedAddresses')
# NGD_A_TOR = Study_Area_Clipper(TestArea, NGD_A, workingGDB, 'NGD_A_TOR')
# AddressRanges = RangeMaker(NGD_A_TOR, Cleaned_Addresses, workingGDB, 'torontoIO_MinMax')
# AddressRanges_CSV = fc_to_csv_writer(AddressRanges, outDirectory, 'torontoIO_MinMax.csv', ['BB_UID', 'Street_Name', 'MIN_NUMBER_INT', 'MAX_NUMBER_INT'])
# print 'Selecting incomplete ranges'
# NGD_AL_Toronto = Study_Area_Clipper(TestArea, Roads, workingGDB, 'NGD_AL_Toronto_w_Streets')

# print ' Creating NGD_AL_Toronto and Cleaned Addresses for target area csv'
# NGD_AL_All_CSV = fc_to_csv_writer(NGD_AL_Toronto, outDirectory, 'NGD_AL_TOR_all.csv', 
#                                                     ['NGD_UID', 'BB_UID_L', 'BB_UID_R', 'AFL_VAL', 'AFL_SRC', 'ATL_VAL', 'ATL_SRC', 'AFR_VAL', 'AFR_SRC', 'ATR_VAL', 'ATR_SRC', 
#                                                     'NGD_STR_UID_L', 'NGD_STR_UID_R', 'STR_NME', 'STR_LABEL_NME'])
# # This is created from a by product of the range maker function (the SJ output file)
# Cleaned_Area_Addresses_CSV = fc_to_csv_writer(os.path.join(workingGDB, 'Address_NGD_A_sj'), outDirectory, 'CleanedAreaAddresses.csv', 
#                                             ['OBJECTID', 'LON', 'LAT', 'NUMBER', 'STREET', 'UNIT', 'CITY', 'SOURCE', 'NUMBER_INT', 'Street_Name', 'Street_Suffix', 
#                                             'Street_Direction', 'BB_UID'])

NGD_AL_All_CSV = os.path.join(outDirectory, 'NGD_AL_TOR_all.csv')
AddressRanges_CSV = os.path.join(outDirectory, 'torontoIO_MinMax.csv')
Cleaned_Area_Addresses_CSV = os.path.join(outDirectory, 'CleanedAreaAddresses.csv')
IncompleteRangeFlagger(AddressRanges_CSV, NGD_AL_All_CSV, Cleaned_Area_Addresses_CSV, 'L', outDirectory)

print 'DONE!'
