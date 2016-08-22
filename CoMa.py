#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pickle as pc
import time

""""
Minimal script to store logs
"""

def add_data_entry_to_pandas(data_entry, panda_frame):
    
    column_data = [data_entry[0], data_entry[1], 
        float(data_entry[19])*1e-3,
        float(data_entry[23])*1e-3,
        float(data_entry[20])*1e-3,
        float(data_entry[24])*1e-3,
        float(data_entry[22])*1e-3,
        float(data_entry[26])*1e-3,
        float(data_entry[21])*1e-3,
        float(data_entry[25])*1e-3,
        (str(data_entry[51:56]))]
    
    column_names = ["Ku-nummer", "Transponder-nummer", # TODO: Get the names from the existing set
        "Idag-1", 
        "Idag-2", 
        "Igår-1", 
        "Igår-2", 
        "Tilgjengelig-1", 
        "Tilgjengelig-2", 
        "Total-1", 
        "Total-2",
        "Dato"]

    tmp_frame = pd.DataFrame([column_data], columns = column_names)

    return pd.DataFrame.append(tmp_frame, panda_frame)

def add_log_core_log(parsed_logs, b):


    for i in range(0, len(parsed_logs)):

        data_entry = parsed_logs[i]

        b = add_data_entry_to_pandas(data_entry, b)

    return b

def initialize_panda():
    
    column_names = ["Ku-nummer", "Transponder-nummer",
    "Idag-1", 
    "Idag-2", 
    "Igår-1", 
    "Igår-2", 
    "Tilgjengelig-1", 
    "Tilgjengelig-2", 
    "Total-1", 
    "Total-2",
    "Dato"]
    
    column_data = [0, 0, 
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0]

    return pd.DataFrame([column_data], columns = column_names)

def load_parsed_log(data_path):

    parsed_logs = []

    with open(data_path) as f:

        tmp_lines = f.readlines()

    for i in tmp_lines:

        tmp_parsed = i.split()
        parsed_logs.append(tmp_parsed)
                
    return parsed_logs

def check_for_new(master_data):

    parsed_log = load_parsed_log(path_to_dta_file)
    old_log = pc.load(open(path_to_current_parsed_log,"rb"))

    if parsed_log == old_log:

        print "Equal, will not update"

        print "Current core log has"
        print len(master_data)
        print "entries"

    else:

        print "Not equal, will update"

        master_data = add_log_core_log(parsed_log, master_data)
        master_data = master_data.reset_index(drop = True)
        pc.dump(parsed_log, open(path_to_current_parsed_log, "wb"))
        pc.dump(master_data, open(path_to_core_log, "wb"))

        "Core log saved"
        
        print "Current core log has"
        print len(master_data)
        print "entries"

path_to_dta_file = "cows.dta"
path_to_current_parsed_log = "current_log.p"
path_to_core_log = "core.p"
is_run_zero = False
time_res_in_min = 5

# Things to do before activity

if is_run_zero:

    master_data_set = initialize_panda()
    print "Zero-dataframe initialized"
    init_log = load_parsed_log(path_to_dta_file)
    print "Initial log loaded"
    pc.dump(init_log, open(path_to_current_parsed_log, "wb"))
    print "Inital log dumped"
    master_data_set = add_log_core_log(init_log, master_data_set)
    print "Initial log added"
    pc.dump(master_data_set, open(path_to_core_log, "wb"))
    print "Core log saved"
    master_data_set.reset_index(drop = True)

else:
    
    master_data_set = pc.load(open(path_to_core_log, "rb"))


while True:
    
    check_for_new(master_data_set)
    time.sleep(60*time_res_in_min)


