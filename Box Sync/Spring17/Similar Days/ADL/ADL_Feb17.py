
# coding: utf-8

# In[1]:

# Author: Sreeta Gorripaty
# Date: 2/6/2017
import pandas as pd
import csv
import numpy as np
import time
import zipfile
import calendar
import os
from datetime import datetime, timedelta
from cStringIO import StringIO
from itertools import groupby
from operator import itemgetter


# In[2]:

def processDemand(file_location):
    """

    The data array must contain the following columns as show below:
    ['Airport(Origin for Deps/Destination for Arrs)', 'Day of month', 'Day of year',
    'Hour', 'Month', 'Year', 'Operation', 'Hourly demand'].

    This function takes in as input the path to the zip file that contains the ADL files for the entire day.

    This function outputs the 2 csv files, one containing the arrival demand and the other containing 
    departure demand information.

    """

    def outputFile(file_location):
        """

        This function takes as input the location of the zip file containing all the adl files for a day and
        outputs an array containing important information for further processing.

        """
        # Storing all the file names in a zip file corresponding to a day
        with zipfile.ZipFile(file_location, "r") as zfile: # Within the zip file
            file_list = zfile.namelist() # List of all file names
        # Getting the airport name for determining the GMT-local time relation
        global airport
        airport = file_list[0].split('_')[1].split('+')[0]
        EST = ['ewr', 'phl', 'jfk', 'lga'] # This should be updated to contain the list of all the airports with EST times
        PST = []
        CST = ['ord']
        MST = []

        if airport in EST: # Apply the demand processing meant for EST airports
            # Find the first filename after 4 AM EST time
            os.environ['TZ'] = 'US/Eastern' # defining the localtime environment
            time_local = [time.localtime(calendar.timegm
                                         (time.strptime(row.split('_')[2] + row.split('_')[3], 
                                                        "%Y%m%d%H%M%S"))) for row in file_list]
        if airport in PST: 

            os.environ['TZ'] = 'US/Pacific' # defining the localtime environment
            time_local = [time.localtime(calendar.timegm
                                         (time.strptime(row.split('_')[2] + row.split('_')[3], 
                                                        "%Y%m%d%H%M%S"))) for row in file_list]
        
        if airport in CST:
            os.environ['TZ'] = 'US/Central' # defining the localtime environment
            time_local = [time.localtime(calendar.timegm
                                         (time.strptime(row.split('_')[2] + row.split('_')[3], 
                                                        "%Y%m%d%H%M%S"))) for row in file_list]

        if airport in MST: 
            os.environ['TZ'] = 'US/Mountain' # defining the localtime environment
            time_local = [time.localtime(calendar.timegm
                                         (time.strptime(row.split('_')[2] + row.split('_')[3], 
                                                        "%Y%m%d%H%M%S"))) for row in file_list]

        # Creating an array with time and filename
        file_data = np.column_stack((np.arange(len(time_local)), time_local))
        index_sort = np.lexsort((file_data[:,5], file_data[:,4], file_data[:,3])) # Sorting by hour, and in case of conflict by minute
        hour_sorted = file_data[index_sort][:,4]
        # Sorting the filenames in the increasing order of their times
        filenames_sorted = np.array(file_list)[index_sort]

        # Getting the first index of 4 in the file_data - local hour
        filename_schDemand = filenames_sorted[list(hour_sorted).index(4)]
        print filename_schDemand
        
        # Searching all the zip files in the file_location and reading filename_schDemand
        adl_4am = []
        with zipfile.ZipFile(file_location) as zf:
            reader = csv.reader(StringIO(zf.read(filename_schDemand)))
            names = reader.next()
            for l in reader:
                adl_4am.append(l)

        return pd.DataFrame(adl_4am, columns=names)

    def process_ADL(adl_4am, zone):
        """

        This function takes in as input the inputted csv file data and processes it to give an array
        containing necessary information for further manipulation.

        """

        def TimeStruct(d):
            """
            Returning a local-time version for the given GMT time.
            """
            os.environ['TZ'] = zone # defining the localtime environment
            return time.localtime(calendar.timegm(time.strptime(d, "%m/%d/%Y %H:%M:%S")))

        def hourExtract(d): # Extract hour from time
            return float(TimeStruct(d).tm_hour)

        def mdayExtract(d): # Extract the day of month from time
            return float(TimeStruct(d).tm_mday)

        def ydayExtract(d): # Extract the day of the year from time
            return float(TimeStruct(d).tm_yday)

        def monExtract(d): # Extract month from time
            return float(TimeStruct(d).tm_mon)

        def yearExtract(d): # Extract year from time
            return float(TimeStruct(d).tm_year)

        # Prepare storage for features. 
        # Create object type list since we have both str and float in the array
        X = np.zeros((np.array(adl_4am).shape[0], 16)).astype(object)

        X[:,0] = [d for d in np.array(adl_4am)[:,2]] # Destination airport
        X[:,1] = [d for d in np.array(adl_4am)[:,4]] # Origin airport
        X[:,2] = [TimeStruct(d) for d in adl_4am["ETAtime"]] # ETA time in struct format
        X[:,3] = [hourExtract(d) for d in adl_4am["ETAtime"]] # ETA hour
        X[:,4] = [mdayExtract(d) for d in adl_4am["ETAtime"]] # ETA day of month
        X[:,5] = [ydayExtract(d) for d in adl_4am["ETAtime"]] # ETA day of year
        X[:,6] = [monExtract(d) for d in adl_4am["ETAtime"]] # ETA month of year
        X[:,7] = [yearExtract(d) for d in adl_4am["ETAtime"]] # ETA year
        X[:,8] = [TimeStruct(d) for d in adl_4am["ETDtime"]] # ETD time in struct format
        X[:,9] = [hourExtract(d) for d in adl_4am["ETDtime"]] # ETD hour
        X[:,10] = [mdayExtract(d) for d in adl_4am["ETDtime"]] # ETD day of month
        X[:,11] = [ydayExtract(d) for d in adl_4am["ETDtime"]] # ETD day of year
        X[:,12] = [monExtract(d) for d in adl_4am["ETDtime"]] # ETD month of year
        X[:,13] = [yearExtract(d) for d in adl_4am["ETDtime"]] # ETD year
        X[:,14] = [d for d in adl_4am["OpsType"]] # Operation type-Arrival/Departure at airport
        X[:,15] = np.ones(len(adl_4am)) # For easy counting on the demand

        return X

    def countArrData(adl_arr):
        """

        This function takes in as input an array containing  the processed flight information data and
        groups the arrival data to obtain counts and outputs an array containing the arrival demand.

        """
        # Grouping data based on Operation, ETAhour, ETAday_year, ETAmon, ETAyear
        grouper = itemgetter("ETAday_year", "ETAhour", "ETAmon", "ETAyear", "ETAday_month", "Operation", "Destination")
        arr_demand = []
        for key, grp in groupby(sorted(adl_arr, key = grouper), grouper):
            temp_dict = dict(zip(["ETAday_year", "ETAhour", "ETAmon", "ETAyear", "ETAday_month", "Operation", 
                                  "Destination"], key))
            temp_dict["count"] = sum(item["count"] for item in grp) # Finding the total # flights in each hour
            arr_demand.append(temp_dict) # Appending to a list
            # Converting the resulting list of dictionaries to arrays
        arr_demand = np.array(pd.DataFrame(arr_demand))

        return arr_demand


    def countDepData(adl_dep):
        """

        This function takes in as input an array containing  the processed flight information data and
        groups the departure data to obtain counts and outputs an array containing the departure demand.

        """
        # repeating it for departures to get departure demand    
        grouper = itemgetter("ETDday_year", "ETDhour", "ETDmon", "ETDyear", "ETDday_month", "Operation", "Origin")
        dep_demand = []
        for key, grp in groupby(sorted(adl_dep, key = grouper), grouper):
            temp_dict = dict(zip(["ETDday_year", "ETDhour", "ETDmon", "ETDyear", "ETDday_month", "Operation", 
                                  "Origin"], key))
            temp_dict["count"] = sum(item["count"] for item in grp)
            dep_demand.append(temp_dict)
        dep_demand = np.array(pd.DataFrame(dep_demand))

        # Rearranging the dep_demand array to match with the columns on arr_demand array
        dep_demand = np.hstack((np.hstack((dep_demand[:,6].reshape(len(dep_demand), 1), dep_demand[:,:6])),
                                dep_demand[:,7].reshape(len(dep_demand), 1)))

        return dep_demand


    def fullDay1(data, day): # We only expect from 3 to 23 hours in day1 
        # This is because the file contains one hour before and 36 hours beyond the time stamp (4AM local time here)
        """

        This function takes in as input the processed datafile containing the demand information and fills up
        missing rows of hours to keep a consistent format for the first day in the 4AM-4AM format.

        The output is a dataframe containing demand information from 3 to 23 hours in local time for the first day.

        """
        # Checking for presence of all required hours in day1
        data = data[data[:,2] == day]
        hours1 = data[data[:,2] == day][:,3]    
        flag_day1 = [int(not(i in hours1)) for i in range(3,24)] # Checking for missing hours
        # Basically filling up all the missing rows with a constant row (the first non-zero row) to begin with
        data_full_day1 = []
        counter = 0
        for i in range(len(flag_day1)):
            if flag_day1[i]:
                data_full_day1.append(data[0])
            else:
                data_full_day1.append(data[counter])
                counter += 1

        data_full_day1 = np.array(data_full_day1)
        # Correcting the hour and count information in the newly added rows
        data_full_day1[:,3] = range(3,24) # Correcting the hour column
        for i in range(21): #24-3 = 21
            if not flag_day1[i]:
                data_full_day1[i,7] = data_full_day1[i,7]
            else:
                data_full_day1[i,7] = 0 # 0 demand

        return data_full_day1

    def fullDay2(data, day): # We only expect from 0 to 13 hours in day2
        # This is because the file contains one hour before and ~36 hours beyond the time stamp (4AM local time here)

        """

        This function takes in as input the processed datafile containing the demand information and fills up
        missing rows of hours to keep a consistent format for the second day in the 4AM-4AM format.

        The output is a dataframe containing demand information from 0 to 13 hours in local time for the second day.

        """
        # If this part is empty (i.e., 0-4 hours have no demand), then just use the data from day1
        if len(data[data[:,2] == day]) == 0:
            default = data[data[:,2] == (day-1)][-1]
            next_date ='/'.join((str(int(default[4])), str(int(default[1])), str(int(default[5]))[2:]))
            next_date = datetime.date(datetime.strptime(next_date, "%m/%d/%y") + timedelta(days = 1))
            default[4] = next_date.month
            default[1] = next_date.day
            default[5] = next_date.year
            default[2] = next_date.timetuple().tm_yday

        data = data[data[:,2] == day]
        # Checking for presence of all required hours in day1
        hours2 = data[data[:,2] == day][:,3]
        flag_day2 = [int(not(i in hours2)) for i in range(0,14)] # Checking for missing hours
        # Basically filling up all the missing rows with a constant row (the first non-zero row) to begin with
        data_full_day2 = []
        counter = 0
        for i in range(len(flag_day2)):
            if flag_day2[i] and len(data):
                data_full_day2.append(data[0])
            elif flag_day2[i] and len(data) == 0:
                data_full_day2.append(default)
            else:
                data_full_day2.append(data[counter])
                counter += 1

        data_full_day2 = np.array(data_full_day2)
        # # Correcting the hour and count information in the newly added rows
        data_full_day2[:,3] = range(14) # Correcting the hour column
        for i in range(14):
            if not flag_day2[i]:
                data_full_day2[i,7] = data_full_day2[i,7]
            else:
                data_full_day2[i,7] = 0

        return data_full_day2

    def truncate(data):
        """

        This function takes in as input a consistent dataframe containing demand information in local time and
        trucates into 4 AM - 3 AM output.

        """
        return data[1:25,:] # Truncating 4 am - 3 am for one day demand data

    def writeCsv(data, operation):
        """

        This function writes the arrival and departure demand dataframes into a csv file based on which operation
        is represented by the data file. operation is represented as a string by 'arrival' or 'departure'

        """
        date = file_location.split('/')[-1].split('_')[-1].split('.')[0] # Getting the date from file name
        directory_save = '/' + "/".join(file_location.split('/')[1:-1])+'/Demand/'
        header = ['Airport', 'Operation', 'Hourly Demand', 'Year', 'Month', 'Day', 'Local Hour']
        # Outputting the arrival demand
        with open(directory_save+airport+'_' +operation + '_' + date + '.csv', 'w') as output:
            writer = csv.writer(output)
            writer.writerow(header)
            writer.writerows(data)

    #### Main code of this function #####
    adl_array = outputFile(file_location) # The array containing the information from relevant file in the zip file
    adl_array_process = process_ADL(adl_array, 'US/Eastern') # Getting a processed file of the ADL data with required fields
    # Creating a dictionary from the numpy array
    column_names = ['Destination', 'Origin', 'ETAtime', 'ETAhour', 'ETAday_month', 'ETAday_year', 'ETAmon',
                'ETAyear','ETDtime', 'ETDhour', 'ETDday_month', 'ETDday_year', 'ETDmon', 'ETDyear', 
                'Operation', 'count']
    adl_dict = [dict(zip(column_names, record)) for record in adl_array_process] 
    adl_arr = [record for record in adl_dict if record["Operation"] == "A"] # Filtering only for arrivals
    adl_dep = [record for record in adl_dict if record["Operation"] == "D"] # Filtering only for departures
    arr_demand = countArrData(adl_arr) # Deriving the counts for arrivals
    dep_demand = countDepData(adl_dep) # Deriving the counts for departures

    days = np.unique(arr_demand[:,2]) # Unique days existing in the dataframe. We expect 2 days.
    # If statement here showing that if the day1, day2 diff is >= 365, then day1 = days[1], day2 = days[0]
    # If statement also adds days with 0 demand in 0-4 hours
    if len(days) > 1:
        if abs(days[0] - days[1]) < 364:
            day1 = days[0]
            day2 = day1 + 1
        else:
            day1 = days[1]
            day2 = days[0]
    else:
        day1 = days[0]
        day2 = day1 + 1
    arr_demand_full = np.vstack((fullDay1(arr_demand, day1), fullDay2(arr_demand, day2))) # Deriving the local time demand
    arr_demand_day = truncate(arr_demand_full) # Deriving the truncated local time demand
    date = ['/'.join((str(int(row[4])), str(int(row[1])), str(int(row[5]))[2:])) for row in arr_demand_day]
    arr_demand_day = np.hstack((np.hstack((np.hstack((arr_demand_day[:,0].reshape(len(arr_demand_day), 1), 
                                           np.array(date).reshape(len(date), 1))), 
                                           arr_demand_day[:,3].reshape(len(arr_demand_day),1))), 
                               arr_demand_day[:,7].reshape(len(arr_demand_day),1)))
    # Creating a pandas dataframe
    arr_pd = pd.DataFrame(arr_demand_day, columns=['Airport', 'Local Date', 'Local Hour', 'Scheduled Arrival Demand'])

    dep_demand_full = np.vstack((fullDay1(dep_demand, day1), fullDay2(dep_demand, day2))) # Deriving the gmt time demand
    dep_demand_day = truncate(dep_demand_full) # Deriving the local time demand

    # Keeping only the variables required in the output
    date = ['/'.join((str(int(row[4])), str(int(row[1])), str(int(row[5]))[2:])) for row in dep_demand_day]
    dep_demand_day = np.hstack((np.hstack((np.hstack((dep_demand_day[:,0].reshape(len(dep_demand_day), 1), 
                                           np.array(date).reshape(len(date), 1))), 
                                           dep_demand_day[:,3].reshape(len(dep_demand_day),1))), 
                               dep_demand_day[:,7].reshape(len(dep_demand_day),1)))
    # Creating a pandas dataframe
    dep_pd = pd.DataFrame(dep_demand_day, columns=['Airport', 'Local Date', 'Local Hour', 'Scheduled Departure Demand'])

    # Merging the two arrays based on 'Airport', 'Local Date', 'Local Hour':
    demand_day_pd = pd.merge(arr_pd, dep_pd, on = ['Airport', 'Local Date', 'Local Hour'], how = 'outer')

    return np.array(demand_day_pd)            



# In[5]:

# pseudo code:
# In increasing order, iterate through folders (years); In each year, iterate through folders (months); 
# In each month, iterate through zip files (days)
# When iterating through zip files, append 24 lines into a list
# Write the list into a csv

header = ['Airport', 'Local Date', 'Local Hour', 'Scheduled Arrival Demand', 'Scheduled Departure Demand']
directory_save = "/Users/gorripaty/Box Sync/Fall16/Thesis/ADL/"
adldir = '/Volumes/Elements/ADL/ADL_LGA' # Stored on external hard-disk
output_file = 'LGA_demand_ADL.csv'
adl_list = []
# Iterate over year-folders
missing_count = 0
missing_days = []
avail_days = []
avail_count = 0
for subdir, dirs, files in os.walk(adldir):
    for file in files:
        try:
            adl_list.extend(processDemand(os.path.join(subdir, file)))#, day_count))
            avail_count += 1
            avail_days.append(file)
        except Exception as e: # Incase the zip file doesnt have hour 8!
#             print file
            print "Exception:   ", e
            missing_count += 1
            missing_days.append(file)
            continue
#         print os.path.join(subdir, file)
        
# Outputting the file to be merged with processed Kennis output file
with open(directory_save + output_file, 'w') as output:
    writer = csv.writer(output)
    writer.writerow(header)            
    writer.writerows(np.array(adl_list)) # This needs to be written in each zip file!


# In[6]:

print "Number of missing accounted days (errors in file):", missing_count
print "Number of processed complete days:", avail_count
# print "Number of missing un-accounted days (completely missing):", 1862-missing_count-avail_count
print "Number of missing un-accounted days (completely missing):", 1626-missing_count-avail_count


# In[ ]:




# In[ ]:



