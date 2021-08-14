'''
Author: Daphne Raskin
Purpose: This program is used to manipulate the raw rack data csv that the TMRW platform creates and extract more information.

What it does: The raw rack data csv stores ticket information in rows. This program outputs a new csv that stores all of the original ticket data with many more metrics.
    Inputted ticket data: RACK_ID, USAGE_END_TIME_UTC, DURATION_SECONDS, UNIT_ID, ORDER_ID
    Outputted ticket data: RACK_ID, DURATION_SECONDS, UNIT_ID, NORMALIZED_UNIT, ORDER_ID, DATE, TIME, HOUR, ORDERS_TODAY, ROLLING_HOUR_START, TRANSIT_TIME_LAST_HOUR, DURATIONS_LAST_HOUR_LIST, DAY_OF_WEEK, TOTAL_DURATION_TODAY, ALL_DURATION_SECONDS_TODAY_LIST, AVERAGE_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR, SD_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR, Z_SCORE_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR, SD_AVERAGE_DURATION_FOR_RACK, TIMED_OUT, Z_SCORE_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR, TOMORROW, TOMORROW_TOTAL_DURATION, TOMORROW_TRANSIT_TIME_LAST_HOUR, TOMORROW_TICKET_DURATION_LAST_HOUR_LIST, TOMORROW_DAY_OF_WEEK, TOMORROW_DOW_AVERAGE_TOTAL_DURATION, TOMORROW_DOW_SD_TOTAL_DURATION, TODAY_DOW_AVERAGE_TOTAL_DURATION, TODAY_DOW_SD_TOTAL_DURATION

How to run: 
You can run this script on your computer once you edit the file_path and output_path variables located below the rack_data class instantiation at the bottom.
    file_path : the exact path of the raw rack data csv on your computer
    output_path : the path where you want the program's outputted csv to be stored
'''


# load in libraries
from abc import ABC, abstractmethod 
import pandas as pd
import statistics
from statistics import mean
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import time
from pandas import Timestamp
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import pytz

class rack_data(ABC):
    
    def __init__(self):
        
        self.file_path = "" # set in open_csv
        self.path_to_store = "" # set in dict_to_df_to_csv
        self.rack_id = "RACK_ID"
        self.duration_seconds = "DURATION_SECONDS"
        self.unit_id = "UNIT_ID"
        self.normalized_unit = "NORMALIZED_UNIT"
        self.order_id = "ORDER_ID"
        self.date = "DATE"
        self.time = "TIME"
        self.hour = "HOUR"
        self.usage_end_time_utc = "USAGE_END_TIME_UTC"
        self.orders_today = "ORDERS_TODAY"
        self.rolling_hour_start = "ROLLING_HOUR_START"
        self.day_of_week = "DAY_OF_WEEK"
        self.timed_out = "TIMED_OUT"
        self.transit_time_last_hour = "TRANSIT_TIME_LAST_HOUR"
        self.total_duration_today = 'TOTAL_DURATION_TODAY'
        self.total_duration_today_list = 'ALL_DURATION_SECONDS_TODAY_LIST'
        self.list_tt_last_hour_frfh = "LIST_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR"
        self.average_tt_last_hour_frfh = "AVERAGE_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR"
        self.sd_tt_last_hour_frfh = "SD_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR"
        self.z_score_tt_last_hour_frfh = "Z_SCORE_TRANSIT_TIME_LAST_HOUR_FOR_RACK_FOR_HOUR"
        self.sd_average_duration_fr = "SD_AVERAGE_DURATION_FOR_RACK"
        self.z_score_average_duration_fr = "Z_SCORE_AVERAGE_DURATION_FOR_RACK"
        self.sd_tt_last_hour_fr = "SD_TRANSIT_TIME_LAST_HOUR_FOR_RACK"
        self.z_score_tt_last_hour_fr = "Z_SCORE_TRANSIT_TIME_LAST_HOUR_FOR_RACK"
        self.duration_list = "Duration List"
        self.transit_time_list = "Transit Time List"
        self.average = "Average"
        self.standard_deviation = "Standard Deviation"
        self.average_total_duration = "Average Total Duration"
        self.sum = "Sum"
        self.duration_average = "Duration Average"
        self.tomorrow = "TOMORROW"
        self.tomorrow_total_duration = "TOMORROW_TOTAL_DURATION"
        self.tomorrow_transit_time_last_hour = "TOMORROW_TRANSIT_TIME_LAST_HOUR"
        self.tomorrow_ticket_duration_last_hour_list = "TOMORROW_DURATIONS_LAST_HOUR_LIST"
        self.tomorrow_day_of_week = "TOMORROW_DAY_OF_WEEK"
        self.tomorrow_dow_average_total_duration = "TOMORROW_DOW_AVERAGE_TOTAL_DURATION"
        self.tomorrow_dow_sd_total_duration = "TOMORROW_DOW_SD_TOTAL_DURATION"
        self.today_dow_average_total_duration = "TODAY_DOW_AVERAGE_TOTAL_DURATION"
        self.today_dow_sd_total_duration = "TODAY_DOW_SD_TOTAL_DURATION"
        self.durations_last_hour = "DURATIONS_LAST_HOUR_LIST"
        
    def load_in_csv(self, path):
        
        # loads in csv, returns a python dictionary containing csv data
        self.path = path
        
        df = pd.read_csv(path)
        
        # load in all of the relevant data we have
        df[self.usage_end_time_utc] = pd.to_datetime(df[self.usage_end_time_utc])
        df[self.date] = [d.date() for d in df[self.usage_end_time_utc]]
        df[self.time] = [d.time() for d in df[self.usage_end_time_utc]]
        df[self.orders_today] = np.nan
        df[self.hour] = [d.hour for d in df[self.usage_end_time_utc]]
        
        # return python dictionary to manipulate the data more easily
        dataframe = df.set_index(self.usage_end_time_utc).T.to_dict('list')
        return dataframe
    
    def df_to_dict_for_manipulation(self, dataframe):
        
        # Make a list of all of the keys to each dictionary within the dataframe dictionary
        keys = [x for x in dataframe.keys()]
        
        # map to normalize unit_ids
        unit_normalized_map = {401 : 1, 590: 2, 968: 3, 11375: 4, 20926 : 5, 44113 : 6}
        
        # Parse the dataframe information and store in a new dictionary-of-dictionaries to manipulate the data
        rack_data_dict = {}
        for key in tqdm(keys):
            rack_data_dict[key] = {
                self.rack_id : dataframe[key][0],
                self.duration_seconds : dataframe[key][1],
                self.unit_id : dataframe[key][2],
                self.normalized_unit : unit_normalized_map[dataframe[key][2]],
                self.order_id : dataframe[key][3],
                self.date : dataframe[key][4],
                self.time : dataframe[key][5],
                self.hour : dataframe[key][-1]
            }
        return rack_data_dict
    
    def get_orders_today(self, rack_data_dict):
        '''Now, we have a dictionary containing all ticket orders, where the timestamp is the key, 
        and all of the attributes are identified as key:value pairs.
        Create a new attribute that loops through all keys and for each key, finds all of the other
        timestamps that occur on that day (and in the same rack) and stores them as a list under "ORDERS_TODAY"'''
        
        keys = rack_data_dict.keys()
        
        for key in tqdm(keys):
            ticket = rack_data_dict[key]
            tik_date = ticket[self.date]
            tik_rack_id = ticket[self.rack_id]
            orders_today = []

            # loop through comparison keys, storing orders that occurred on the same day, as well as  tomorrow total duration 
            for newkey in keys:
                test_ticket = rack_data_dict[newkey]
                test_rack = test_ticket[self.rack_id]
                test_date = test_ticket[self.date]

                if ((test_date == tik_date) & (test_rack == tik_rack_id)):
                    orders_today.append(newkey)
            ticket[self.orders_today] = orders_today
        return rack_data_dict
    
    def get_last_rolling_hour_info(self, rack_data_dict):
        ''' Now we need to figure out how many orders that occur today, occurred in the last rolling hour.
        This will tell us how much use the rack has gotten in the past hour.
        First, we need to determine the rolling hour start time that ends with the creation of the ticket.'''
        
        keys = rack_data_dict.keys()
        
        for key in tqdm(keys):
            ticket = rack_data_dict[key]
            tik_time = ticket[self.time]

            # make tik_time a datetime.date value instead of a datetime.time value & subtract
            tik_time = datetime.strptime(str(tik_time),'%H:%M:%S')
            delta = timedelta(minutes = 59)
            hour_start = (tik_time - delta).time() 

            # set rolling_hour_start time
            ticket[self.rolling_hour_start] = hour_start
            
        '''Now, we need to loop through the tickets saved in orders_today to determine if any occured in the last rolling hour
        Then, we will sum the durations of the tickets that occured in the last rolling hour, and save that info for each ticket

        step 1: loop through tickets
        step 2: for each ticket, get the ticket time and the rolling hour start time
        step 3: loop through the ticket's orders_today list
        step 4: determine if each ticket order ocurred after the rolling hour start time and before/at the original ticket's time
        step 5: sum those durations
        step 6: new attribute "TRANSIT_TIME_LAST_HOUR" set to sum
        '''
        i  = 0
        for key in tqdm(keys):
            i += 1
            ticket = rack_data_dict[key]
            tik_time, hour_start, orders_list = ticket[self.time], ticket[self.rolling_hour_start], ticket[self.orders_today]

            # to compare times, make all variables datetime.date values
            # make tik_time a datetime.date value instead of a datetime.time value 
            tik_time = datetime.strptime(str(tik_time),'%H:%M:%S')
            tik_time = pytz.utc.localize(tik_time).time()

            # make hour_start time a datetime.date value
            hour_start = datetime.strptime(str(ticket[self.rolling_hour_start]), '%H:%M:%S')
            hour_start = pytz.utc.localize(hour_start).time()

            # create a list where you will store all of the durations in the last rolling hour:
            durations_list = []

            # loop through orders and add the durations of the valid orders to the total
            transit_time = 0
            if type(orders_list) == list:
                for order in orders_list:

                    order_key = order            
                    order = order.to_pydatetime()
                    order = order.time()

                    if ((order >= hour_start) & (order <= tik_time)):

                        add_duration = rack_data_dict[order_key][self.duration_seconds]
                        durations_list.append(add_duration)
                        transit_time += add_duration

            # save the information for each ticket
            ticket[self.transit_time_last_hour] = transit_time
            ticket[self.durations_last_hour] = durations_list
        return rack_data_dict
    
    def get_todays_day_of_week(self, rack_data_dict):
    
        for key in tqdm(rack_data_dict.keys()):
            ticket = rack_data_dict[key]
            tik_date = ticket[self.date]
            day_of_week = tik_date.weekday()
            ticket[self.day_of_week] = day_of_week
           
        return rack_data_dict
            
    def get_todays_total_duration(self, rack_data_dict):
        # fill each ticket with the total duration for that day
        
        keys = rack_data_dict.keys()
        # loop through each ticket, and for each, find all tickets with the same date and rack and store their durations to sum
        for key in tqdm(keys):
            
            ticket = rack_data_dict[key]
            ticket_date, ticket_rack, orders_today = ticket[self.date], ticket[self.rack_id], ticket[self.orders_today]
            
            ticket_total_duration_today = 0
            total_duration_today_list = []
            
            # loop through comparison tickets -- Note to self: how to change this to save time? 
            for newkey in keys:
                test_ticket = rack_data_dict[newkey]
                test_ticket_rack, test_ticket_duration = test_ticket[self.rack_id], test_ticket[self.duration_seconds]
                
                if newkey in orders_today and test_ticket_rack == ticket_rack:
                    ticket_total_duration_today += test_ticket_duration
                    total_duration_today_list.append(test_ticket_duration)
                    
            ticket[self.total_duration_today] = ticket_total_duration_today
            ticket[self.total_duration_today_list] = total_duration_today_list
        return rack_data_dict
    
    def get_rack_average_transit_time_for_hour(self, rack_data_dict):
        '''For each ticket, find the average time that the ticket's rack is normally used in the past hour
        Then, determine the standard deviation of the ticket's total transit time from the average
        step 1: create a list of all of the rack numbers
        step 2: create a master rack dictionary
        step 3: loop through the racks 
        step 4: for each rack, create a dictionary with key:value pairs for each hour of the day
        step 5: for each rack, loop through all of its tickets 
        step 5: if the ticket was created in the rack, append that ticket's transit time to a list in the rack dictionary, 
        where the key is the hour at which the ticket was created
        '''
        
        keys = rack_data_dict.keys()

        # create a list of all rack ids
        racks = []
        for key in keys:
            ticket = rack_data_dict[key]
            if ticket[self.rack_id] not in racks:
                racks.append(ticket[self.rack_id])

        # dictionary to hold dictionaries for each rack of a list, for each hour, of the total transit times (rolling hour)
        rack_durations_per_hour = {}
        
        # loop through racks
        for rack in tqdm(racks):

            # create a dictionary of dictionaries, to hold a list of total durations for a rolling hour at each hour
            rack_durations_per_hour[rack] = {0:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 1: {self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 2:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 3:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 4:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 5:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 6:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 7:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 8:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 9:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 10:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 11:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 12:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 13:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 14:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 15:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 16:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 17:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 18:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 19:{self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 20: {self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 21: {self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 22: {self.duration_list: [], self.average: 0, self.standard_deviation: 0}, 23: {self.duration_list: [], self.average: 0, self.standard_deviation: 0}}
            # loop through all tickets 
            for key in keys:
                ticket = rack_data_dict[key]
                ticket_rack = ticket[self.rack_id]
                ticket_hour = ticket[self.hour]
                
                # if the ticket was called on the current rack, append that ticket's transit time last hour to the list of the hour it was called
                if ticket_rack == rack:
                    rack_durations_per_hour[rack][ticket_hour][self.duration_list].append(ticket[self.transit_time_last_hour]) 
        
            # for each rack, loop through and find the average and sd for each hourly transit time list
            for i in range(24):
                lst = rack_durations_per_hour[rack][i][self.duration_list]
                if len(lst) > 0:
                    average = sum(lst) / len(lst)
                    sd = statistics.pstdev(lst, average)
                    rack_durations_per_hour[rack][i][self.average] = average
                    rack_durations_per_hour[rack][i][self.standard_deviation] = sd
        
        # loop through keys, storing the average and standard deviation found above
        for key in keys:
            ticket = rack_data_dict[key]
            ticket_rack, ticket_hour = ticket[self.rack_id], ticket[self.hour]
            ticket[self.average_tt_last_hour_frfh] = rack_durations_per_hour[ticket_rack][ticket_hour][self.average]
            ticket[self.sd_tt_last_hour_frfh] = rack_durations_per_hour[ticket_rack][ticket_hour][self.standard_deviation]
        
        # Now you can calculate the z-score for each ticket's standard deviation for rack for ticket hour
        for key in tqdm(keys):
            
            ticket = rack_data_dict[key]
            transit_time_last_hour, average_transit_time_for_hour, sd_at_this_hour = ticket[self.transit_time_last_hour], ticket[self.average_tt_last_hour_frfh], ticket[self.sd_tt_last_hour_frfh]

            if sd_at_this_hour != 0:
                z_score = (transit_time_last_hour - average_transit_time_for_hour) / sd_at_this_hour
            else:
                z_score = 0.0

            ticket[self.z_score_tt_last_hour_frfh] = z_score

        return rack_data_dict
    
    def get_rack_average_duration(self, rack_data_dict):
        '''Once more data is collected, the average data for each hour for each rack will be more useful.
        Since we don't have a lot of data, we will find the variation between each ticket's duration and the average 
        duration (regardless of hour) for its rack.'''

        keys = rack_data_dict.keys()
        # create a list of all rack ids
        racks = []
        for key in keys:
            ticket = rack_data_dict[key]
            if ticket[self.rack_id] not in racks:
                racks.append(ticket[self.rack_id])
        
        # loop through racks and create dictionary to store lists of rack durations (regardless of hour)
        rack_duration_lists = {}
        for rack in racks:
            rack_duration_lists[rack] = []

        # loop through keys and store each duration for each rack 
        for key in keys:
            ticket = rack_data_dict[key]
            ticket_rack, ticket_duration = ticket[self.rack_id], ticket[self.duration_seconds]
            rack_duration_lists[ticket_rack].append(ticket_duration)

        # create a dictionary of rack average durations using the dictionary of duration lists
        rack_average_durations = {}
        for rack_id in racks:
            rack_average_durations[rack_id] = {}

        for rack in rack_duration_lists:

            duration_list = rack_duration_lists[rack]
            rack_average_dur = 0

            if len(duration_list) > 0:
                rack_average_dur = sum(duration_list) / len(duration_list)

            rack_average_durations[rack][self.duration_list] = duration_list
            rack_average_durations[rack][self.average] = rack_average_dur
        
        # find the standard deviation and z-score of each ticket's duration from the rack average
        for key in keys:
            
            ticket = rack_data_dict[key]
            ticket_rack, ticket_duration = ticket[self.rack_id], ticket[self.duration_seconds]

            duration_list = rack_average_durations[ticket_rack][self.duration_list]
            rack_average_duration = rack_average_durations[ticket_rack][self.average]

            # find rack standard deviation of average duration_seconds
            rack_sd = 0
            if len(duration_list) > 0:
                rack_sd = statistics.pstdev(duration_list, rack_average_duration)

            # find ticket's z_score based on standard deviation of rack's average duration seconds

            duration = ticket[self.duration_seconds]
            if rack_sd != 0:
                rack_avg_duration_z_score = (ticket_duration - rack_average_duration) / rack_sd
            else:
                rack_avg_duration_z_score = 0.0

            ticket[self.sd_average_duration_fr] = rack_sd
            ticket[self.sd_average_duration_fr] = rack_avg_duration_z_score
            
        return rack_data_dict
    
    def get_rack_average_transit_time(self, rack_data_dict):
        
        keys = rack_data_dict.keys()
        # create a list of all rack ids
        racks = []
        for key in keys:
            ticket = rack_data_dict[key]
            if ticket[self.rack_id] not in racks:
                racks.append(ticket[self.rack_id])
        
        # find the SD and z-score of each ticket's transit_time (last rolling hour) from rack avg, regardless of hour
        rack_average_rolling_transit_time = {}

        # Create a dict of all transit times in a rolling hour for each rack, with keys "List," "Average," and "SD"
        for rack in racks:
            rack_average_rolling_transit_time[rack] = {self.transit_time_list: [], self.average : 0, self.standard_deviation: 0}

        # Append all of those transit times in a rolling hour for each rack to the "list"
        for key in keys:

            ticket = rack_data_dict[key]
            ticket_rack, ticket_hour, ticket_transit_time_last_hour = ticket[self.rack_id], ticket[self.hour], ticket[self.transit_time_last_hour]
            rack_average_rolling_transit_time[ticket_rack][self.transit_time_list].append(ticket_transit_time_last_hour)


        # Find avg total transit time rolling hour & SD and save in each rack's "Average" and "SD" key
        for rack in rack_average_rolling_transit_time:

            lst = rack_average_rolling_transit_time[rack][self.transit_time_list]
            average = sum(lst) / len(lst)
            rack_average_rolling_transit_time[rack][self.average] = average
            sd = 0
            if len(lst) > 0:
                sd = statistics.pstdev(lst, average)
            rack_average_rolling_transit_time[rack][self.standard_deviation] = sd

        # Loop through each key and store its z_score for transit time rolling hour (rack specific, hour not-specific)
        # also add whether or not it times out
        for key in keys:

            ticket = rack_data_dict[key]
            ticket_rack, ticket_transit_time_rolling_hour = ticket[self.rack_id], ticket[self.transit_time_last_hour]

            sd = rack_average_rolling_transit_time[ticket_rack][self.standard_deviation]
            average = rack_average_rolling_transit_time[ticket_rack][self.average]

            z_score = 0
            if sd != 0:
                z_score = (ticket_transit_time_rolling_hour - average) / sd

            timedout_bool = False
            if ticket_transit_time_rolling_hour > 480: 
                timedout_bool = True

            ticket[self.timed_out] = timedout_bool
            ticket[self.z_score_tt_last_hour_fr] = z_score

        return rack_data_dict
    
    def get_tomorrows_transit_time_last_hour(self, rack_data_dict):
                        
        '''Strategy: In each ticket, save tomorrow's total transit time at today's ticket's hour (if there was any usage). 
        Step 1: Loop through tickets and save tomorrow's total transit time, and the ticket hour, and rolling hour start
        Step 1.5: Create an empty list of comparison tickets
        Step 2: If tomorrow's total transit time is > 0, loop through comparison tickets 
        Step 3: For each comparison ticket, if the comparison ticket's rack == ticket's rack and ticket's date == tomorrow:
        check to see at which hour the ticket was created
        Step 4: If the comparison ticket was created after the rolling hour start, but before the ticket creation, append its duration to a list
        Step 4.5: Sum the list
        Step 5: Loop through the comaprison ticket list, and 
        '''
        keys = rack_data_dict.keys()
        
        for key in tqdm(keys):
            ticket = rack_data_dict[key]
            ticket_date, ticket_rack = ticket[self.date], ticket[self.rack_id]
            tomorrow = ticket_date + timedelta(days=1)
            ticket[self.tomorrow] = tomorrow

            for newkey in keys:
                test_ticket = rack_data_dict[newkey]
                test_rack, test_date, test_total_duration_today = test_ticket[self.rack_id], test_ticket[self.date], test_ticket[self.total_duration_today]
                ticket[self.tomorrow_total_duration] = 0
                if (ticket_rack == test_rack) and (test_date == tomorrow):
                    ticket[self.tomorrow_total_duration] = test_total_duration_today
                    break
        
        for key in tqdm(keys):
            ticket = rack_data_dict[key]
            tomorrow, ticket_tomorrow_transit_time, ticket_rolling_hour_start, ticket_rack, ticket_time  = ticket[self.tomorrow], ticket[self.tomorrow_total_duration], ticket[self.rolling_hour_start], ticket[self.rack_id], ticket[self.time]

            comptickets_list = []
            if ticket_tomorrow_transit_time > 0:
                for newkey in rack_data_dict.keys():
                    test_ticket = rack_data_dict[newkey]
                    test_ticket_rack, test_ticket_date = test_ticket[self.rack_id], test_ticket[self.date]

                    if (test_ticket_rack == ticket_rack) and (test_ticket_date == tomorrow):
                        test_ticket_time = test_ticket[self.time]
                        if (test_ticket_time > ticket_rolling_hour_start) and (test_ticket_time < ticket_time):
                            comptickets_list.append(test_ticket[self.duration_seconds])

            ticket[self.tomorrow_transit_time_last_hour] = sum(comptickets_list)
            ticket[self.tomorrow_ticket_duration_last_hour_list] = comptickets_list
            
        return rack_data_dict
        
    def get_average_rack_usage_day_of_week(self, rack_data_dict):
        '''Find all total daily durations for each day the rack was used, for each day of the week 
        Step 1: create dictionary with each rack as a key, and each value a dictionary pairing a day of the week with 
        a dictionary, where the values are "Duration List," and "Average Total Duration"
        Step 2: Loop through all tickets, filling the appropriate information into the "Duration Lists" based on rack and day of week
        Step 3: Store Average Total Duration; Monday: 0, Sunday: 6'''
        
        keys = rack_data_dict.keys()
        
        # create a list of all rack ids
        racks = []
        for key in keys:
            ticket = rack_data_dict[key]
            if ticket[self.rack_id] not in racks:
                racks.append(ticket[self.rack_id])
        
        # for each rack, store the dates in which the rack was used, and fill a dictionary with emptry values for durations on that day, avg and SD
        dow_dict = {}
        rack_usage = {}
        for rack in racks:
            dow_dict[rack] = {0: {self.duration_list: [], self.average_total_duration: 0, self.standard_deviation : 0}, 1: {self.duration_list: [], self.average_total_duration: 0, self.standard_deviation : 0}, 2: {self.duration_list: [], self.average_total_duration: 0, self.standard_deviation : 0}, 3: {self.duration_list: [], self.average_total_duration: 0, self.standard_deviation : 0}, 4: {self.duration_list: [], self.average_total_duration: 0, self.standard_deviation : 0}, 5: {self.duration_list: [], self.average_total_duration: 0, self.standard_deviation : 0}, 6: {self.duration_list: [], self.average_total_duration: 0, self.standard_deviation : 0}}
            rack_usage[rack] = [] # list of days in which the rack was used
            
        # for each rack, store the total duration for the days when the rack was used(only store once per day of use):
        for key in keys:
            ticket = rack_data_dict[key]
            ticket_rack, ticket_date, ticket_dow, ticket_total_duration_today = ticket[self.rack_id], ticket[self.date], ticket[self.day_of_week], ticket[self.total_duration_today]
            if ticket_date not in rack_usage[ticket_rack]:
                dow_dict[ticket_rack][ticket_dow][self.duration_list].append(ticket_total_duration_today)
                rack_usage[ticket_rack].append(ticket_date)
          
        for key in dow_dict.keys():
            # for each day of the week, store the average total duration and SD from the duration lists
            for i in range(7):
                dow_durlist = dow_dict[key][i][self.duration_list]
                if len(dow_durlist) > 0:
                    average = sum(dow_durlist) / len(dow_durlist)
                    dow_dict[key][i][self.average_total_duration] = average
                    dow_dict[key][i][self.standard_deviation] = statistics.pstdev(dow_durlist, average)
                    
        # now, store tomorrow's day of the week for each ticket
        for key in rack_data_dict.keys():
            ticket = rack_data_dict[key]
            ticket_dow = ticket[self.day_of_week]
            if ticket_dow < 6:
                ticket[self.tomorrow_day_of_week] = ticket[self.day_of_week] + 1
            if ticket_dow == 6:
                ticket[self.tomorrow_day_of_week] = 0

        # now store tomorrow's average total duration for each ticket
        for key in rack_data_dict.keys():
            ticket = rack_data_dict[key]
            ticket_date, tomorrow_dow, ticket_rack = ticket[self.day_of_week], ticket[self.tomorrow_day_of_week], ticket[self.rack_id]
            
            ticket[self.tomorrow_dow_average_total_duration] = dow_dict[ticket_rack][tomorrow_dow][self.average_total_duration]
            ticket[self.tomorrow_dow_sd_total_duration] = dow_dict[ticket_rack][tomorrow_dow][self.standard_deviation]
            ticket[self.today_dow_average_total_duration] = dow_dict[ticket_rack][ticket_date][self.average_total_duration]
            ticket[self.today_dow_sd_total_duration] = dow_dict[ticket_rack][ticket_date][self.standard_deviation]

        return rack_data_dict
    
    def dict_to_df_to_csv(self, rack_data_dict, path_to_store):
        
        self.path_to_store = path_to_store
        
        # create a new dataframe from rack_data_dict 
        # orient= 'index', so that each element of the dictionary is a row
        df = pd.DataFrame.from_dict(rack_data_dict, orient='index')
        
        # save to csv
        df.to_csv(path_to_store)

    def run(self, file_path, path_to_store):
        dataframe = self.load_in_csv(file_path)
        rack_data_dict = self.df_to_dict_for_manipulation(dataframe)
        rack_data_dict = self.get_orders_today(rack_data_dict)
        rack_data_dict = self.get_last_rolling_hour_info(rack_data_dict)
        rack_data_dict = self.get_todays_day_of_week(rack_data_dict)
        rack_data_dict = self.get_todays_total_duration(rack_data_dict)
        rack_data_dict = self.get_rack_average_transit_time_for_hour(rack_data_dict)
        rack_data_dict = self.get_rack_average_duration(rack_data_dict)
        rack_data_dict = self.get_rack_average_transit_time(rack_data_dict)
        rack_data_dict = self.get_tomorrows_transit_time_last_hour(rack_data_dict)
        rack_data_dict = self.get_average_rack_usage_day_of_week(rack_data_dict)
        self.dict_to_df_to_csv(rack_data_dict, path_to_store)

rack_data = rack_data()

file_path = 'C:\\Users\\draskin\\Downloads\\results-20210802-141221.csv'
output_path = 'C:\\Users\\draskin\\Downloads\\results-20210715-153634_TEST.csv'

# YOU MUST SET FILE_PATH AND OUTPUT_PATH!!!
# file_path is the path of the raw rack data csv on your computer
# output_path is the path of the new csv that the program will output. 
rack_data.run(file_path, output_path)
