# -*- coding: utf-8 -*-
"""

This module provides and easy interface for working with the Workable API
from your Python application.

Example Usage:

        from Workable import Workable

        workable = Workable(account='youraccount',apikey='yourapikey')

Section breaks are created by resuming unindented text. Section breaks
are also implicitly created anytime a new section starts.
"""

import requests
from ratelimit import limits, sleep_and_retry
import json
import time
import datetime
from datetime import date
 

delay_per_request = 1

class Workable(object):

    def __init__(self, account, apikey):
        self.account = str(account).lower()
        self.apikey = str(apikey)

        # Authorization headers. Content-Type is not necessary,
        # but should workable start providing alternate
        # content types such as XML, this won't break
        self.request_headers = {
            'Content-Type': 'application/json',
            'authorization': 'Bearer ' + self.apikey
        }
        # Base URL Endpoint for all API requests
        self.api_base = 'https://cafebazaar.workable.com/spi/v3/'

        # API Endpoints for all jobs, a single job, gettig a list of stages and account members
        self.endpoints = {
            'jobs':        self.api_base + 'jobs',
            'job':         self.api_base + 'jobs/',
            'stages':      self.api_base + 'stages/',
            'members':     self.api_base + 'members',
            'candidates':  self.api_base + 'candidates/',
            'recruiters':  self.api_base + 'recruiters/',
            'events':      self.api_base + 'events/',

        }
        # Increase default limit for downloading lists from 50 to 100,
        # so we need to make fewer requests
        self.default_limit = 100

#############################################################################################
#   Functions
#############################################################################################
  
    def workable_depaginate(self, url, key):
        """
        Returns one object based on a given key for a workable API endpoint.

        Arguments:
        url -- the API endpoint that returns paginated data
        key -- the key that contains all the data
        """
        list = { key: []}
        paging = True
        while paging == True:
            
            if '?' in url : 
                url = url + '&limit=' + str(self.default_limit)
            else:
                url = url + '?limit=' + str(self.default_limit)
                
            request = requests.get(url, headers=self.request_headers)
            time.sleep(delay_per_request)
            response_json = request.json()
            list[key].extend(response_json[key])
            try:
                url = response_json['paging']['next'] + '?limit=' + str(self.default_limit)
            except KeyError:
                paging = False
            else:
                paging = True
                
        return { key +' '+str(i) : list[key][i] for i in range(0, len(list[key]) ) }
    
    def candidate_list(self, job, updated_after):
        """
        Download and return the basic list of all candidates for a given job
        """
        #if ( created_after != '' )
        job_candidates_url = self.endpoints['job'] + job + '/candidates'+'?updated_after='+str(updated_after)
        candidate_list = self.workable_depaginate(job_candidates_url, 'candidates')
        return candidate_list

    def candidate_details(self, candidate_list, job):
        """
        Download and return Details for all candidates in a candidate_list
        """
        candidates = []

        for candidate in candidate_list:
            detail = self.single_candidate_detail(candidate['id'], job)
            candidates.append(detail['candidate'])
        return candidates

    def single_candidate_detail(self, candidate_id, job):
        """
        Returns the candidate's detail information, for a given candidate identified by ID
        """
        url = self.endpoints['job'] + job + '/candidates/' + candidate_id
        request = requests.get(url, headers=self.request_headers)
        time.sleep(delay_per_request)

        response = request.json()
        return response

    def single_candidate_activities(self, candidate_id):
        """
        Returns the candidate's detail avctivities (comments, interviews, mentions, ... ), for a given candidate identified by ID
        """
        url = self.endpoints['candidates'] + candidate_id + '/activities/'
        actions = self.workable_depaginate(url, 'activities')
        return actions  

    def workable_write_json(self, object, filename):
        """
        Save the output from workable to a file. Existing files will be overwritten without warning!
        :param object: result from calling the workable API, JSON format
        :param filename: name the file should be saved as, without .json extension
        """
        full_name = filename + '.json'
        open(full_name, 'w').close()
        file = open(full_name, 'a', encoding='utf-8')
        file.write(json.dumps(object, indent=2))
        return

    def job_list(self, state):
        """
        Returns a list of all jobs matching the given state
        :param state: one of the following: draft, published, archived, closed
        :return: Job List
        """
        jobs = []
        if state != '':
            url = self.endpoints['jobs'] + '?state='+ str(state)
        else:
            url = self.endpoints['jobs'] 
            
        jobs = self.workable_depaginate(url, 'jobs')
        return jobs

    def job_detail(self, job):
        """
        Returns detail info for a given job
        :param job: Job Shortcode
        :return: Job Info
        """
        url = self.endpoints['job'] + job
        request = requests.get(url, headers=self.request_headers)
        time.sleep(delay_per_request)

        job = request.json()
        return job
    
    def single_candidate_events(self, candidate_id):
        """
        Returns events detail
        """   
        url = self.endpoints['events'] + '?candidate_id=' + str(candidate_id)
        events = self.workable_depaginate(url, 'events')
        
        return events
    
    def active_members(self):
        """
        returen active members in our domain
        """
        members = []
        url = self.endpoints['members']
        members = self.workable_depaginate(url, 'members')
        
        return members
    
    def member_events(self, member_id = '', start_date= '', end_date = ''):
        """
        return events of a member
        """
        
        url = self.endpoints['events'] + '?member_id=' + str(member_id) + '&start_date=' + str(start_date)+ '&end_date=' + str(end_date)
        memberevents = self.workable_depaginate(url, 'events')
        
        return memberevents
        


