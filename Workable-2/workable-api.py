#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 11:15:54 2019

@author: aliakbarmehdizadeh
"""
# curl -H "Authorization:Bearer <ACCESS TOKEN>"       https://<cafebazaar>.workable.com/spi/v3/jobs

import json
import os
import time
from os import listdir
from os.path import isfile, join
import io
import sys
import os
import datetime
from datetime import date
from datetime import datetime
import pandas as pd
import sqlite3
from datetime import timedelta

working_directory = "/Users/alimehdizadeh/Dropbox/HR-CORE/Codes/Workable"
jobs_data_path = "/Users/alimehdizadeh/Dropbox/HR-CORE/Data/Workable"

##################################################        
#         stablish a conneciton with workable  
##################################################  

os.chdir(working_directory)
from Workable import Workable

# Workable API TOKEN
api_token = os.getenv('API_TOKEN')
# api_url_base = 'https://cafebazaar.workable.com/spi/v3/{}'
workable = Workable(account='cafebazaar', apikey=api_token)

##################################################        
#       loading the database for last update
##################################################        

os.chdir(jobs_data_path)
# create a database connection to a disk file based database
conn = sqlite3.connect("workable_database.db")
# obtain a cursor object
cursor = conn.cursor()

try:
    cursor.execute("SELECT * FROM update_times ORDER BY run_time DESC LIMIT 1")
    result = cursor.fetchone()
    last_run_time = datetime.strptime(result[-1], '%Y-%m-%d')
except:
    last_run_time = datetime.today().date() - timedelta(days=301)

##################################################        

# to get data after this point of time: 30 days before the last update
i_date = last_run_time - timedelta(days=1)

# when update starts
run_time = datetime.now().date()

# list of all jobs: state possible values (draft, published, archived & closed).
all_jobs = workable.job_list(state='published')

# list of all members
members = workable.active_members()

# workable info as JSON:
workable_updated_candidates = {}

for each_job in all_jobs:
    print(all_jobs[each_job]['title'])

    # candidate list for Each Job
    updated_after_candidate_list = workable.candidate_list(job=all_jobs[each_job]["shortcode"], updated_after=i_date)

    for each_candidate in updated_after_candidate_list:

        while True:
            B = True
            try:

                # candidate general Information
                each_candidate_details = updated_after_candidate_list[each_candidate]
                # candiadte process Information
                each_candidate_activities = workable.single_candidate_activities(
                    candidate_id=each_candidate_details["id"])
                # candidate events Inforamtion
                each_candidate_events = workable.single_candidate_events(candidate_id=each_candidate_details["id"], )

                # saving the data of candidate
                workable_updated_candidates[updated_after_candidate_list[each_candidate]['id']] = {
                    'info': each_candidate_details,
                    'actions': each_candidate_activities,
                    'events': each_candidate_events,
                    'updated_at': each_candidate_details['updated_at'],
                    'job_shortcode': each_candidate_details['job']['shortcode'],
                    'stage': each_candidate_details['stage'],
                }
                print('\t' + each_candidate_details['name'])

            except:
                # sleep for api limit_time and reiterate if error happened
                print('sleep')
                B = False
                time.sleep(10)
                continue
            if B:
                break

            # when update ends
f_date = datetime.now().date()

##########################################################
#      connecting to /creating the database for update
##########################################################

# updating the databbase
os.chdir(jobs_data_path)
# create a database connection to a disk file based database
conn = sqlite3.connect("workable_database.db")

# obtain a cursor object
cursor = conn.cursor()

##################################
#           TABLES
##################################

# create a table in the disk file based database
createTable = "CREATE TABLE IF NOT EXISTS workable_by_candidate(id TEXT, job_shortcode TEXT, updated_at TEXT, stage TEXT, events TEXT ,info TEXT, UNIQUE(id))"
cursor.execute(createTable)

# create a table for jobs info
createTable = "CREATE TABLE IF NOT EXISTS workable_jobs(shortcode TEXT, state TEXT, title TEXT, UNIQUE(shortcode))"
cursor.execute(createTable)

# create a table for members info
createTable = "CREATE TABLE IF NOT EXISTS members_info(id TEXT, name TEXT, email TEXT, UNIQUE(id))"
cursor.execute(createTable)

# create a table for events info:
createTable = "CREATE TABLE IF NOT EXISTS events_info(id TEXT, types TEXT, starts_at TEXT, ends_at TEXT, job_id TEXT ,members TEXT,candidate_id TEXT , UNIQUE(id))"
cursor.execute(createTable)

# create a table for updates info
createTable = "CREATE TABLE IF NOT EXISTS update_times(i_date TEXT, f_date TEXT, run_time TEXT )"
cursor.execute(createTable)

####################################
#         INSERTING VALUES
####################################

# insert runnig information to update_times
insertValues = """INSERT OR REPLACE INTO update_times(i_date, f_date, run_time ) VALUES (?,?,?);"""
cursor.execute(insertValues, (str(i_date), str(f_date), str(run_time)))

# insert intp the jobs table:
insertValues = """INSERT OR REPLACE INTO workable_jobs(shortcode, state, title) VALUES (?,?,?);"""
for job in all_jobs:
    cursor.execute(insertValues, (all_jobs[job]['shortcode'],
                                  all_jobs[job]['state'],
                                  all_jobs[job]['title']
                                  )
                   )

# insert into the reports workable candidate/event table
insertValues = """INSERT OR REPLACE INTO workable_by_candidate(id, job_shortcode, updated_at, stage, events, info) VALUES (?,?,?,?,?,?);"""

for candidate in workable_updated_candidates:
    cursor.execute(insertValues, (candidate,
                                  workable_updated_candidates[candidate]['job_shortcode'],
                                  workable_updated_candidates[candidate]['updated_at'],
                                  workable_updated_candidates[candidate]['stage'],
                                  json.dumps(workable_updated_candidates[candidate]['events'], sort_keys=True,
                                             separators=(',', ': '), ensure_ascii=False),
                                  json.dumps(workable_updated_candidates[candidate], sort_keys=True,
                                             separators=(',', ': '), ensure_ascii=False)
                                  )
                   )

insertValues_events = """INSERT OR REPLACE INTO events_info(id, types, starts_at, ends_at, job_id, members,candidate_id)  VALUES (?,?,?,?,?,?,?);"""
for candidate in workable_updated_candidates:
    for event in workable_updated_candidates[candidate]['events']:
        cursor.execute(insertValues_events, (workable_updated_candidates[candidate]['events'][event]['id'],
                                             workable_updated_candidates[candidate]['events'][event]['type'],
                                             workable_updated_candidates[candidate]['events'][event]['starts_at'],
                                             workable_updated_candidates[candidate]['events'][event]['ends_at'],
                                             workable_updated_candidates[candidate]['events'][event]['job'][
                                                 'shortcode'],
                                             json.dumps(
                                                 workable_updated_candidates[candidate]['events'][event]['members'],
                                                 sort_keys=True, separators=(',', ': '), ensure_ascii=False),
                                             workable_updated_candidates[candidate]['events'][event]['candidate']['id'])
                       )

# insert into the members table
insertValues = """INSERT OR REPLACE INTO members_info(id, name, email) VALUES (?,?,?);"""
for member in members:
    cursor.execute(insertValues, (members[member]['id'],
                                  members[member]['name'],
                                  members[member]['email']
                                  )
                   )

# closing databbase connection
conn.commit()
conn.close()

#####################################
#         Evaluation Process
#####################################
import memeber_performance_evaluation

memeber_performance_evaluation.evaluation()
