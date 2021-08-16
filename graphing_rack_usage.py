'''
Taken from a jupyter file. These are some of the cells I wrote to help Michael visualize and interpret rack usage. 
Jupyter notebook cells provide a better format for these methods, and the full file is on my computer.
'''

file_path = 'C:\\Users\\draskin\\Downloads\\results-20210802-141221.csv'
df_attempt = pd.read_csv(file_path)

# make string of all rack dictionarys where you can store all rack ticket information to be used for individual rack data manipulation

racks = np.unique(np.array(df_attempt["RACK_ID"].tolist()))

rack_dicts_string = ""
for r in racks:
    string = "rack_{rack} = ".format(rack = r) + "{}\n"
    rack_dicts_string += string
print(rack_dicts_string)

# below, copy and paste the printed string from above here and instantiate all of those dicts:
rack_402 = {}
rack_403 = {}
rack_404 = {}
rack_405 = {}
rack_406 = {}
rack_407 = {}
rack_408 = {}
rack_409 = {}
rack_410 = {}
rack_411 = {}
rack_412 = {}
rack_413 = {}
rack_414 = {}
rack_415 = {}
rack_416 = {}
rack_417 = {}
rack_418 = {}
rack_591 = {}
rack_592 = {}
rack_593 = {}
rack_594 = {}
rack_595 = {}
rack_596 = {}
rack_597 = {}
rack_598 = {}
rack_599 = {}
rack_600 = {}
rack_601 = {}
rack_602 = {}
rack_603 = {}
rack_604 = {}
rack_605 = {}
rack_606 = {}
rack_607 = {}
rack_969 = {}
rack_970 = {}
rack_971 = {}
rack_972 = {}
rack_973 = {}
rack_974 = {}
rack_975 = {}
rack_976 = {}
rack_977 = {}
rack_978 = {}
rack_979 = {}
rack_980 = {}
rack_981 = {}
rack_982 = {}
rack_983 = {}
rack_984 = {}
rack_985 = {}
rack_11388 = {}
rack_11389 = {}
rack_11390 = {}
rack_11391 = {}
rack_11392 = {}
rack_20927 = {}
rack_20928 = {}
rack_20929 = {}
rack_20930 = {}
rack_20931 = {}
rack_20932 = {}
rack_20933 = {}
rack_20934 = {}
rack_20935 = {}
rack_20936 = {}
rack_20937 = {}
rack_20938 = {}
rack_20939 = {}
rack_20940 = {}
rack_20941 = {}
rack_20942 = {}
rack_20943 = {}
rack_44126 = {}
rack_44127 = {}
rack_44128 = {}
rack_44129 = {}
rack_44130 = {}

# fill rack dicts VERY IMPORTANT CELL!!!
keys = df_attempt_dict.keys()
for k in tqdm(keys):
    ticket = df_attempt_dict[k]
    ticket_rack = ticket["RACK_ID"]
    rack_string = "rack_{r}".format(r = ticket_rack)
    eval(rack_string)[k] = ticket
    
def fill_rack_dicts(df):
    keys = df.keys()
    
    for k in keys:
        ticket = df[k]
        ticket_rack = ticket["RACK_ID"]
        if ticket_rack == rack_id:
            string = "rack_{rack}".format(rack = ticket_rack)
            eval(string)[k] = ticket
    return string

def get_unit_rack_mapping():
    # make units dictionary, with unit_id keys paired with empty lists
    units = {}
    unit_ids = np.unique(np.array(df_attempt["UNIT_ID"].tolist()))
    for i in unit_ids:
        units[i] = []

    # make list of racks and store in units dictionary
    racks = []
    df_attempt_keys = df_attempt_dict.keys()

    for key in df_attempt_keys:
        ticket = df_attempt_dict[key]
        ticket_unit = ticket["UNIT_ID"]
        ticket_rack = ticket["RACK_ID"] 

        unit_list = units[ticket_unit]

        if ticket_rack not in unit_list:
            units[ticket_unit].append(ticket_rack)
    return units

# method to calculate ema and return array of dates to plot
def calculate_ema(transit_times, days, smoothing = 2):
    ema = []
    for i in range(days-1):
        ema.append(np.nan)
    ema.append(sum(transit_times[:days]) / days)
    for transit_time in transit_times[days:]:
        ema.append((transit_time * (smoothing / (1 + days))) + ema[-1] * (1 - (smoothing / (1 + days))))
    return ema

def map_unit_id_to_location(unit_id):
    if unit_id == 401:
        return "Vios"
    if unit_id == 590:
        return "Conceptions"
    if unit_id == 779:
        return "SDFC"
    if unit_id == 968:
        return "SDFC"
    if unit_id == 20926:
        return "BIVF"
    if unit_id == 44113:
        return "CCRM"
    if unit_id == 44500:
        return "CCRM"
    else:
        return "Location Unknown"
    
def map_location_to_unit_id(location):
    if location.lower() == "vios":
        return [401]
    if location.lower() == "conceptions":
        return [590]
    if location.lower() == "sdfc":
        return [779, 968]
    if location.lower() == "bivf":
        return [20926]
    if location.lower() == "ccrm":
        return [44113, 44500]
    
def get_timed_out_information(rack_dict):
    
    rack_id = 0
    total_timeouts = 0
    i = 0
    

    list_tix_before_timeout = []
    for key in rack_dict.keys():
        
        ticket = rack_dict[key]
        ticket_transit_time = ticket["TRANSIT_TIME_LAST_HOUR"]
        ticket_date = ticket["DATE"]
        
        if i != 1:
            rack_id = ticket["RACK_ID"]
            i = 1
            
        july_seventh = date(2021, 7, 7)
        after_july_seventh_bool = (ticket_date > july_seventh)
        
        if ticket_transit_time >= 480 and after_july_seventh_bool:

            total_timeouts += 1
            
            time = ticket["TIME"]
            
            durations_list = ticket["DURATIONS_LAST_HOUR"]
            
            num_tix = len(durations_list) / 2.0
            list_tix_before_timeout.append(num_tix)
            
            durations_string = str(durations_list).replace('[', '').replace(']', '').replace(',', ' +')            
        
            print("Rack {} timed out on {} at {} from pulls of {} seconds during the last hour after {} tickets.".format(rack_id, ticket_date, time, durations_string, num_tix))
    
    if len(list_tix_before_timeout) > 0: 
        print("There were {} total timeouts on rack {} after July 7th. The following ticket counts resulted in timeouts: {}".format(total_timeouts, rack_id, list_tix_before_timeout))
    else:
        print("There were {} total timeouts on rack {} after July 7th.".format(total_timeouts, rack_id))

#graph a rack from a specific start date
def graph_rack_from_start_date(rack_dict, startdate):
    x_values = []
    y_values = []
    rack_id = 0
    unit_id = 0
    
    # fill x, and also grab rack id, unit id, start_date, end_date, and map unit_id to location, 
    i = 0
    for key in rack_dict.keys():
        ticket_date = rack_dict[key]["DATE"]
        if i < 1:
            rack_id = rack_dict[key]["RACK_ID"]
            unit_id = rack_dict[key]["UNIT_ID"]
        if i == len(x_values) - 1:
            end_date = rack_dict[key]["DATE"]
        i = 1
        if ticket_date >= startdate:
            x_values.append(key)
            y_values.append(rack_dict[key]["TRANSIT_TIME_LAST_HOUR"])
    print(len(x_values))
    print(len(y_values))
    
    location = map_unit_id_to_location(unit_id)
            
    y2 = plt.gca().get_ylim()

    plt.scatter(x_values, y_values, s = 3, color = 'k')
    
    # plot buffer rule & 7 minute threshold lines
    plt.axhline(y = 435, color = 'blue', linestyle = '-', label = "7.25 minute warning")
    plt.axhline(y = 480, color = 'purple', linestyle = '-', label = "8. minute buffer rule")
    
    # calculate ema if there are more than 10 pieces of data for that rack
    transit_times = []
    for key in rack_dict:
        ticket = rack_dict[key]
        ticket_date_transit = rack_dict[key]["DATE"]
        if ticket_date_transit >= startdate:
            transit_time = ticket["TRANSIT_TIME_LAST_HOUR"]
            transit_times.append(transit_time)
        
    if len(transit_times) >= 10:
        ema = calculate_ema(transit_times, 10)
        # plot ema
        plt.plot(x_values, ema, color = 'orange', label = 'ema')
    
    # plot axis & title
    plt.xlabel("Date")
    plt.ylabel("Total Transit Time, Last Rolling Hour (sec)")
    title = "Use of Rack {rack_id}, Located in Unit {unit_id} at {location} ({start_date} thru {end_date})".format(rack_id = rack_id, unit_id = unit_id, location = location, start_date = startdate, end_date= end_date)
    plt.title(title, size=15)

    # plot ticks
    N = 500
    tl = plt.gca().get_xticklabels()
    maxsize = max([t.get_window_extent().width for t in tl])
    m = .5 # inch margin
    s = maxsize/plt.gcf().dpi*N+2*m
    margin = m/plt.gcf().get_size_inches()[0]

    plt.gcf().subplots_adjust(left=margin, right=10.-margin)
    plt.gcf().set_size_inches(s, plt.gcf().get_size_inches()[1])

    plt.legend(loc=(.8,.75))
    plt.show()
    
    # print timed-out information
    get_timed_out_information(rack_dict)
    
def graph_rack(rack_dict):
    x = rack_dict.keys()
    y = []
    rack_id = 0
    unit_id = 0
    
    # fill x, and also grab rack id, unit id, start_date, end_date, and map unit_id to location, 
    i = 0
    for key in x:
        if i < 1:
            rack_id = rack_dict[key]["RACK_ID"]
            unit_id = rack_dict[key]["UNIT_ID"]
            start_date = rack_dict[key]["DATE"]
        if i == len(x) - 1:
            end_date = rack_dict[key]["DATE"]
        i+= 1
        y.append(rack_dict[key]["TRANSIT_TIME_LAST_HOUR"])
    
    location = map_unit_id_to_location(unit_id)
            
    y2 = plt.gca().get_ylim()

    plt.scatter(x, y, s = 3, color = 'k')
    
    # plot buffer rule & 7 minute threshold lines
    plt.axhline(y = 435, color = 'blue', linestyle = '-', label = "7.25 minute warning")
    plt.axhline(y = 480, color = 'purple', linestyle = '-', label = "8. minute buffer rule")
    
    # calculate ema if there are more than 10 pieces of data for that rack
    transit_times = []
    for key in rack_dict:
        ticket = rack_dict[key]
        transit_time = ticket["TRANSIT_TIME_LAST_HOUR"]
        transit_times.append(transit_time)
        
    if len(transit_times) >= 10:
        ema = calculate_ema(transit_times, 10)
        # plot ema
        plt.plot(x, ema, color = 'orange', label = 'ema')
    
    # plot axis & title
    plt.xlabel("Date")
    plt.ylabel("Total Transit Time, Last Rolling Hour (sec)")
    title = "Use of Rack {rack_id}, Located in Unit {unit_id} at {location} ({start_date} thru {end_date})".format(rack_id = rack_id, unit_id = unit_id, location = location, start_date = start_date, end_date= end_date)
    plt.title(title, size=15)

    # plot ticks
    N = 500
    tl = plt.gca().get_xticklabels()
    maxsize = max([t.get_window_extent().width for t in tl])
    m = .5 # inch margin
    s = maxsize/plt.gcf().dpi*N+2*m
    margin = m/plt.gcf().get_size_inches()[0]

    plt.gcf().subplots_adjust(left=margin, right=10.-margin)
    plt.gcf().set_size_inches(s, plt.gcf().get_size_inches()[1])

    plt.legend()
    plt.show()
    
    # print timed-out information
    get_timed_out_information(rack_dict)
    
# method to graph list of rack dictionaries
def graph_list_of_racks(list_of_rack_dicts):
    for i in list_of_rack_dicts:
        graph_rack(i)
        
# method to graph all racks from one unit
def graph_racks_from_unit(unit_id):
    
    # first, store all rack_ids of that unit in a list
    units = get_unit_rack_mapping()
    #print(units)
    racks = units[unit_id]
    for r in racks:
        graph_rack(eval("rack_{r}".format(r = r)))
        
def get_ticket_counts_before_timeout(rack_dict):
    
    list_tix_for_timeout = []
    
    for key in rack_dict.keys():
        
        ticket = rack_dict[key]
        total_tix = 0
        ticket_timeout_bool = ticket["TIMED_OUT"]
        ticket_date = ticket["DATE"]
        
        july_seventh = date(2021, 7, 7)
        after_july_seventh_bool = (ticket_date > july_seventh)

        if ticket_timeout_bool and after_july_seventh_bool:
            
            ticket_durations_last_hour = ticket["DURATIONS_LAST_HOUR"]
            print(ticket_durations_last_hour)
            total_tix = len(ticket_durations_last_hour) / 2.
            list_tix_for_timeout.append(total_tix)
    return list_tix_for_timeout

# returns the total average ticket counts of the list of unit(s) that are passed
def get_ticket_counts_for_units(unit_ids):
    
    total_tix_count = []
    
    for unit_id in unit_ids:
        
        unit_tix_count = []
        
        # first, store all rack_ids of that unit in a list
        units = get_unit_rack_mapping()
        racks = units[unit_id]
        
        # loop through each rack, storing each rack's ticket counts when the rack has timed out
        
        for r in racks:
            rack_tix_count = get_ticket_counts_before_timeout(eval("rack_{rack}".format(rack = r)))
            
            unit_tix_count.append(rack_tix_count)
            
        # then, append those lists to the unit list, and append the unit lists to the total list!
        
        total_tix_count.append(unit_tix_count)
    
    # then, unpack all values from the total and return a list of those values
    all_ticket_counts = []
    for i in range(len(total_tix_count)):
        unit = total_tix_count[i]
        for y in range(len(unit)):
            rack = unit[y]
            for z in range(len(rack)):
                val = rack[z]
                all_ticket_counts.append(val)
    print(all_ticket_counts)
    return sum(all_ticket_counts) / len(all_ticket_counts)                                                             

# returns the total average transit time that results in timeout, of the list of unit(s) that are passed 
def get_total_timeout_average_time(unit_ids):
    total_timeouts = []
    
    for unit_id in unit_ids:
        
        unit_total_timeouts = []
        
        # first, store all rack_ids of that unit in a list
        units = get_unit_rack_mapping()
        racks = units[unit_id]
        # loop through each rack, storing each rack's ticket counts when the rack has timed out
        
        for r in racks:
    
            rack = eval("rack_{r}".format(r = r))
            rack_total_timeouts = []
            for key in rack.keys():
                ticket = rack[key]
                timedout_bool = ticket["TIMED_OUT"]
                ticket_date = ticket["DATE"]
                
                july_seventh = date(2021, 7, 7)
                after_july_seventh_bool = (ticket_date > july_seventh)
                if timedout_bool and after_july_seventh_bool: 
                    ticket_total_transit_time = ticket["TRANSIT_TIME_LAST_HOUR"]
                    rack_total_timeouts.append(ticket_total_transit_time)
                    
            unit_total_timeouts.append(rack_total_timeouts)
            
        # then, append those lists to the unit list, and append the unit lists to the total list!
        total_timeouts.append(unit_total_timeouts)
    
    # then, unpack all values from the total and return a list of those values
    all_timeouts = []
    for i in range(len(total_timeouts)):
        unit = total_timeouts[i]
        for y in range(len(unit)):
            rack = unit[y]
            for z in range(len(rack)):
                val = rack[z]
                all_timeouts.append(val)
    print("TOTAL")
    print(all_timeouts)
    return sum(all_timeouts) / len(all_timeouts)                                                         

'''
Strategy: For each ticket that resulted in a timeout (after July 7th), determine the average time per pull
'''

# returns the average pull time of timeout pulls, of the list of unit(s) that are passed 
def get_average_time_per_pull_during_timeout(unit_ids):
    all_pulls_per_timeout = []
    
    for unit_id in unit_ids:
        
        unit_pulls_per_timeout = []
        
        # first, store all rack_ids of that unit in a list
        units = get_unit_rack_mapping()
        racks = units[unit_id]
        
        # loop through each rack, storing each rack's ticket counts when the rack has timed out
        for r in racks:
    
            rack = eval("rack_{r}".format(r = r))
            rack_pulls_per_timeout = []
            
            for key in rack.keys():
                ticket = rack[key]
                timedout_bool = ticket["TIMED_OUT"]
                ticket_date = ticket["DATE"]
                july_seventh = date(2021, 7, 7)
                after_july_seventh_bool = (ticket_date > july_seventh)
                
                if timedout_bool and after_july_seventh_bool: 
                    
                    ticket_durations_last_hour = ticket["DURATIONS_LAST_HOUR"]
                    for pull in ticket_durations_last_hour:
                        rack_pulls_per_timeout.append(pull)
            
            unit_pulls_per_timeout.append(rack_pulls_per_timeout)
            
        # then, append those lists to the unit list, and append the unit lists to the total list!
        all_pulls_per_timeout.append(unit_pulls_per_timeout)
    
    # then, unpack all values from the total and return a list of those values
    all_pulls = []
    for i in range(len(all_pulls_per_timeout)):
        unit = all_pulls_per_timeout[i]
        for y in range(len(unit)):
            rack = unit[y]
            for z in range(len(rack)):
                val = rack[z]
                all_pulls.append(val)

    average_pull_time = sum(all_pulls) / len(all_pulls)
    sd_pull_time = statistics.pstdev(all_pulls, average_pull_time)
    return all_pulls, average_pull_time, sd_pull_time                                                    
