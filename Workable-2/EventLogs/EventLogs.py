#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  5 16:26:28 2019

@author: aliakbarmehdizadeh
"""

import json
import os
import time
from os import listdir
from os.path import isfile, join
from os import chdir, getcwd
import pandas as pd
import numpy as np
import io

try:
    to_unicode = unicode
except NameError:
    to_unicode = str

working_directory = '/Users/alimehdizadeh/Dropbox/CafeBazaar/Workable'
# directory of cnadidates information
read_dir = '/Users/alimehdizadeh/Desktop/Workable_Data'
# set reading direcotry
chdir(read_dir)

# Load All candidate Info
ListOfCandidates = []

EventLog = pd.DataFrame(
    columns=['CaseId', 'ActionType', 'ActionID', 'ActionTime', 'ActionActorID', 'ActionActorName', 'ActionBody'])

for CandidateFileName in os.listdir(read_dir):
    with open(CandidateFileName, "r", encoding='utf8') as Candidate:

        try:
            CandidateInfo = json.load(Candidate)
            print(CandidateInfo['candidate']['name'])
            ListOfCandidates.append(CandidateInfo)

        except:
            print('an error in loading')

        # Make Event Log
for Candidate in ListOfCandidates:

    try:
        CaseId = Candidate['candidate']['id']
    except:
        continue

    for Action in Candidate['activities']:

        ActionType = Action['action']
        ActionID = Action['id']
        ActionTime = Action['created_at']

        try:
            ActionActorID = Action['member']['id']
        except:
            ActionActorID = np.NaN
        try:
            ActionActorName = Action['member']['name']
        except:
            ActionActorName = 'CafeBazzar'
        try:
            ActionBody = Action['body']
        except:
            ActionBody = np.NaN

        EventLog = EventLog.append(
            pd.Series([CaseId, ActionType, ActionID, ActionTime, ActionActorID, ActionActorName, ActionBody],
                      index=EventLog.columns),
            ignore_index=True)

EventLog = EventLog.sort_values('ActionTime')
EventLog = EventLog.drop(['ActionID', 'ActionActorID', 'ActionBody'], axis=1)
EventLog['ActionTime'] = EventLog['ActionTime'].str.replace('T', ' ')
EventLog = EventLog.dropna()
EventLog_CSV = EventLog.to_csv(working_directory + '/EventLog_CSV.csv', index=None, header=True, encoding="utf-8")
