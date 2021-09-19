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
from datetime import datetime
from datetime import timedelta
import os
from datetime import date
from dateutil.relativedelta import relativedelta
import sqlite3
import pytz
import ast


def evaluation():
    working_directory = "/Users/alimehdizadeh/Dropbox/HR-CORE/Codes/Workable"
    data_path = "/Users/alimehdizadeh/Dropbox/HR-CORE/Data/Workable"

    ##################################################        
    #              loading the database 
    ##################################################        

    os.chdir(data_path)
    # create a database connection to a disk file based database
    conn = sqlite3.connect("workable_database.db")
    # tables names
    table_names = pd.read_sql_query("SELECT name FROM sqlite_master WHERE  type='table';", conn)

    # select prefreed time period to be loaded:
    today_str = str(datetime.today().date() + timedelta(days=14))
    last_month_str = str(datetime.today().date() - timedelta(days=30))

    # converting data to pandas dataframe
    data_frame = {}
    for index, table_name in table_names.iterrows():

        if table_name[-1] == 'workable_by_candidate':
            data_frame[table_name[-1]] = pd.read_sql_query('SELECT * FROM ' + table_name[
                -1] + ' WHERE updated_at BETWEEN "' + last_month_str + '" AND "' + today_str + '"', conn,
                                                           parse_dates=['updated_at', 'starts_at', 'ends_at', 'i_date',
                                                                        'f_date', 'run_time'])

        elif table_name[-1] == 'events_info':
            data_frame[table_name[-1]] = pd.read_sql_query('SELECT * FROM ' + table_name[
                -1] + ' WHERE starts_at BETWEEN "' + last_month_str + '" AND "' + today_str + '"', conn,
                                                           parse_dates=['updated_at', 'starts_at', 'ends_at', 'i_date',
                                                                        'f_date', 'run_time'])

        else:
            data_frame[table_name[-1]] = pd.read_sql_query('SELECT * FROM ' + table_name[-1], conn,
                                                           parse_dates=['updated_at', 'starts_at', 'ends_at', 'i_date',
                                                                        'f_date', 'run_time'])

    ##################################################    
    #           initilization of data
    ##################################################

    today = datetime.utcnow()
    today = today.replace(tzinfo=pytz.utc)

    data = pd.DataFrame(
        columns=['name', 'num interviews', 'num meetings', 'num calls', 'tot time [h]', 'tot meeting time [h]',
                 'tot call time [h]', 'positions', 'next two weeks interveiws', 'avg words/interview', 'num moved'])
    data_positions = pd.DataFrame(
        columns=['title', 'state', 'shortcode', 'num interviews', 'next two weeks interviews'])

    # initilization
    data['name'] = data_frame['members_info']['name']
    data['num interviews'] = 0
    data['num meetings'] = 0
    data['num calls'] = 0
    data['tot time [h]'] = today - today
    data['tot meeting time [h]'] = today - today
    data['tot call time [h]'] = today - today
    data['positions'] = ''
    data['next two weeks interveiws'] = 0
    data['avg words/interview'] = 0
    data['num moved'] = 0
    data['num disqualified'] = 0

    data_positions['title'] = data_frame['workable_jobs']['title']
    data_positions['state'] = data_frame['workable_jobs']['state']
    data_positions['shortcode'] = data_frame['workable_jobs']['shortcode']
    data_positions['num interviews'] = 0
    data_positions['next two weeks interviews'] = 0
    data_positions['num applied'] = 0
    data_positions['num disqualified'] = 0
    # stages
    data_positions['Applied'] = 0
    data_positions['Phone Interview / Task'] = 0
    data_positions['Interview 1'] = 0
    data_positions['Interview 2'] = 0
    data_positions['Interview 3'] = 0
    data_positions['Offer'] = 0
    data_positions['Shortlisted'] = 0
    data_positions['Hired'] = 0
    data_positions['Sourced'] = 0
    data_positions['Archived'] = 0

    ##################################################    
    #           analysis by event
    ##################################################

    for index, event in data_frame['events_info'].iterrows():

        event_type = event['types']
        event_duration = event['ends_at'] - event['starts_at']

        job_shortcode = event['job_id']
        job_name = \
        data_frame['workable_jobs']['title'].loc[data_frame['workable_jobs']['shortcode'] == job_shortcode].values[0]
        job_index = data_positions[data_positions['title'] == job_name].index[0]

        members = ast.literal_eval(event['members'])

        for member in members:

            member_name = member['name']
            member_index = data[data['name'] == member_name].index[0]

            if event['starts_at'] < today:

                data_positions.at[job_index, 'num interviews'] += 1

                data.at[member_index, 'num interviews'] += 1
                data.at[member_index, 'tot time [h]'] += event_duration

                if event_type == 'MeetingEvent':
                    data.at[member_index, 'num meetings'] += 1
                    data.at[member_index, 'tot meeting time [h]'] += event_duration

                else:
                    data.at[member_index, 'num calls'] += 1
                    data.at[member_index, 'tot call time [h]'] += event_duration

                if not (job_name in data.at[member_index, 'positions']):
                    data.at[member_index, 'positions'] += job_name + ', '
            else:
                data.at[member_index, 'next two weeks interveiws'] += 1
                data_positions.at[job_index, 'next two weeks interviews'] += 1

    ##################################################    
    #           analysis by actions/candidates
    ##################################################

    for index, candidate in data_frame['workable_by_candidate'].iterrows():

        candidate_info = json.loads(candidate['info'])
        candidate_stage = candidate_info['stage']
        candidate_actions = candidate_info['actions']

        job_shortcode = candidate_info['job_shortcode']
        job_name = \
        data_frame['workable_jobs']['title'].loc[data_frame['workable_jobs']['shortcode'] == job_shortcode].values[0]
        job_index = data_positions[data_positions['title'] == job_name].index[0]

        data_positions.at[job_index, candidate_stage] += 1

        for action in candidate_actions:

            action_time = datetime.strptime(candidate_actions[action]['created_at'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
            action_time = action_time.replace(tzinfo=pytz.utc)

            if action_time < today - timedelta(days=30):
                continue
            else:
                if candidate_actions[action]['action'] == 'comment':

                    actor_name = candidate_actions[action]['member']['name']
                    involved_members = []

                    for event in candidate_info['events']:

                        members = candidate_info['events'][event]['members']

                        for member in members:
                            involved_members.append(member['name'])

                    if actor_name in involved_members:
                        try:
                            actor_index = data[data['name'] == actor_name].index[0]
                            words = len(candidate_actions[action]['body'].split(' '))
                            data.at[actor_index, 'avg words/interview'] = ((data.at[
                                                                                actor_index, 'avg words/interview'] *
                                                                            data.at[
                                                                                actor_index, 'num interviews']) + words) / \
                                                                          data.at[actor_index, 'num interviews']
                        except:
                            data.at[actor_index, 'avg words/interview'] = 0

                elif candidate_actions[action]['action'] == 'moved':

                    actor_name = candidate_actions[action]['member']['name']
                    actor_index = data[data['name'] == actor_name].index[0]
                    data.at[actor_index, 'num moved'] += 1

                elif candidate_actions[action]['action'] == 'applied':

                    data_positions.at[job_index, 'num applied'] += 1

                elif candidate_actions[action]['action'] == 'disqualified':

                    actor_name = candidate_actions[action]['member']['name']
                    actor_index = data[data['name'] == actor_name].index[0]

                    data_positions.at[job_index, 'num disqualified'] += 1
                    data.at[actor_index, 'num disqualified'] += 1

    ##################################################    
    #           creating the output
    ##################################################

    # get the time of last updates
    to_time = data_frame['update_times']['run_time'].iloc[-1].date()
    since_time = data_frame['update_times']['i_date'].iloc[-1].date()

    data.to_csv(data_path + '/members/since ' + str(since_time) + ' to ' + str(to_time) + '.csv', index=None,
                header=True, encoding="utf-8")
    data_positions.to_csv(data_path + '/positions/since ' + str(since_time) + ' to ' + str(to_time) + '.csv',
                          index=None, header=True, encoding="utf-8")

    return print('done')
