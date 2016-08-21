#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pickle as pc

def add_data_entry_to_pandas(data_entry, panda_frame):
    
    column_data = [data_entry[0], data_entry[1], 
        float(data_entry[19])*1e-3,
        float(data_entry[23])*1e-3,
        float(data_entry[20])*1e-3,
        float(data_entry[24])*1e-3,
        float(data_entry[22])*1e-3,
        float(data_entry[26])*1e-3,
        float(data_entry[21])*1e-3,
        float(data_entry[25])*1e-3,]
    
    column_names = ["Ku-nummer", "Transponder-nummer", # TODO: Get the names from the existing set
        "Idag-1", 
        "Idag-2", 
        "Igår-1", 
        "Igår-2", 
        "Tilgjengelig-1", 
        "Tilgjengelig-2", 
        "Total-1", 
        "Total-2"]

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
    "Total-2"]
    
    column_data = [0, 0, 
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,]

    return pd.DataFrame([column_data], columns = column_names)

def load_parsed_log(data_path):

    parsed_logs = []

    with open(data_path) as f:

        tmp_lines = f.readlines()

    for i in tmp_lines:

        tmp_parsed = i.split()
        parsed_logs.append(tmp_parsed)
                
    return parsed_logs


path_to_dta_file = "cows.dta"
path_to_current_parsed_log = "current_log.p"
path_to_core_log = "core.p"

foo = pc.load(open(path_to_core_log, "rb"))

foo.reset_index(inplace = True)

print foo

b = pc.load(open(path_to_core_log, "rb"))
parsed_logs = load_parsed_log(path_to_dta_file)
parsed_old = pc.load(open(path_to_current_parsed_log, "rb"))

b.to_csv("foo.csv")

if parsed_logs ==  parsed_old:

    b = add_log_core_log(parsed_logs, b)

#    pc.dump(parsed_logs, open(path_to_current_parsed_log, "wb"))
#    pc.dump(b, open(path_to_core_log, "wb"))
#
#else:
#    
#    print "Not needed"
