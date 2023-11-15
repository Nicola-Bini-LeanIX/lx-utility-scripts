import logging
import numpy as np
import pandas as pd
import json
import requests
import html
import os
import sys

# For timestamp in backup names
import datetime

# Regular expression operations
import re

class LeanIX:

    # init of LeanIX class to create instance
    def __init__(self):
        self.__load_env_vars()
        self.headers = self.__getAuthHeader()

    # Get access token per request
    def __getAuthHeader(self): 
        access_token = None
        auth_url = 'https://' + self.base_url+ '/services/mtm/v1/oauth2/token' 
        try:
            response = requests.post(auth_url, auth=('apitoken', self.api_token), data={'grant_type': 'client_credentials'})
            response.raise_for_status()
            access_token = response.json()['access_token']
            headers = {'Authorization': 'Bearer ' +
                       access_token, 'Content-Type': 'application/json'}
            return headers
        except Exception as ex:
            print('Error: Obtaining access token failed. ' + str(ex))

    def __load_env_vars(self):
        real_path = os.path.realpath(__file__)
        dir_path = os.path.dirname(real_path)

        file_path = dir_path + '/.env'
        print('file_path')
        print(file_path)

        data = {}
        with open(file_path) as file:
            data = json.load(file)

            file.close()

        self.api_token = data['api_token']
        self.base_url = data['base_url']

     # Function to execute GraphQL queries. The function is defined to be reusable by passing any query (will consider possible patches in future to alsp reuse them for mutation requests)
    def __execute_graphql(self, query):
        request_url = 'https://' + self.base_url + '/services/pathfinder/v1/'
        json_data = json.dumps(query)
        # print('######### json_data ##########')
        # print(json_data)
        
        response = requests.post(url=request_url + 'graphql', headers=self.headers, data=json_data)
        response.raise_for_status()

        return response

    def get_fs_data(self, fs_type, fields=[], categories=[]):
        '''
        Retrieve apps data 
        fs_type: Factsheet type
        fields: Fields to retrieve 
        categories: Empty list for all categories
        '''

        facet_filters = ''
        if len(categories) > 0 :
            facet_filters = '{facetFilters: [{facetKey: "FactSheetTypes", keys: ["%s"]}, {facetKey: "category", keys: %s}]}' % (fs_type, json.dumps(categories))
        else:
            facet_filters = '{facetFilters: [{facetKey: "FactSheetTypes", keys: ["%s"]}]}' % (fs_type)
            

        query_body = R''' 
            {
              allFactSheets(filter: %s) {
                totalCount
                edges {
                  node {
                    ... on %s {
                      id
                      name
                      externalId{externalId}
                      %s
                    }
                  }
                }
              }
            }''' % (facet_filters, fs_type, ' '.join(fields))

        fields = fields + ['id', 'name', 'externalId']

        query = {
            'query' : query_body
        }

        print(query)

        response = self.__execute_graphql(query)

        if response.status_code == 200:

            if 'errors' in response:
                raise ConnectionError(f'A GraphQL error occured while retrieving {fs_type} data:\n{response["errors"]}')

            # If no errors occurred
            else:
                df_fs_data = pd.DataFrame(columns=fields)

                for node in response.json()['data']['allFactSheets']['edges']:
                    if node['node']['externalId']:
                        node['node']['externalId'] = node['node']['externalId']['externalId']
                    df_fs_data = pd.concat([df_fs_data, pd.DataFrame([node['node']], columns=fields)], ignore_index=True)
        else:
            raise ConnectionError('Could not connect to GraphQL server')

        return df_fs_data

    def delete_fs(self, fs_id):

        delete_url = 'https://' + self.base_url + '/services/pathfinder/v1/factSheets/' + fs_id
        data = {'id', }
        response = requests.delete(delete_url, headers=self.headers)

        if response.status_code == 204:
            return 1
        else:
            return 0

    def delete_list_fs(self, fs_ids, fs_type):
        assert(isinstance(fs_ids, list))

        #print('fs_ids')
        #print(fs_ids)
        current_fs_list_to_delete = []
        counter = 0
        continue_run = True

        # Create list of ids to include in the next run
        for fs_id in fs_ids:
            if counter < 50:
                try:
                    current_fs_list_to_delete.append(fs_id)
                    fs_ids.remove(fs_id)
                    counter += 1
                except Exception as e:
                    continue_run = False
                    print('Error: ' + str(e))
                    print('End of list reached')
                    break
            else:
                break


        if len(current_fs_list_to_delete) > 0:
            queries = ""
            mutation_counter = 1
            patches_list=[]
            parameters = ''

            for fs_id in current_fs_list_to_delete:  # iterating over all factSheets
                # add new variablename to parameters
                patches = []
                patchName = "patches"+str(mutation_counter)
                patches.append({
                    "op": "replace",
                    "path": "/status",
                    "value": "ARCHIVED"
                })
                # add the created patches to the variables per factSheet

                patches_list.append({patchName: patches})
                if parameters != "":
                    parameters = parameters + ","
                parameters = parameters + "$"+patchName+": [Patch]!"
                # create mutation incl. scores per Factsheet
                mutation = R"""fs%s: updateFactSheet(id: "%s", 
                        patches: $patches%s, validateOnly: false, comment: "Deleted by script made by LeanIX Team" ) {
                            factSheet {
                                ... on %s {
                                    id
                                    status
                                }
                            }
                        }""" % (mutation_counter, fs_id, mutation_counter, fs_type)
                #print(mutation)
                mutation_counter += 1
                if queries != "":
                    queries = queries + ","
                queries = queries+mutation

            # create final query to be sent via GraphQL REST API
            graphql_query = """mutation (%s) {%s}""" % (parameters, queries)
            post_request_body = {
                "query": graphql_query,
                "variables": {
                }
            }


            for patch in patches_list:
                for key in patch:
                    post_request_body["variables"][key] = patch.get(key)

            #TODO: Add this to logging files
            #print('### post_request_body ###')
            #print(post_request_body['variables'])

            query_response = self.__execute_graphql(post_request_body)  # call mutation endpoint

            if query_response.status_code == 401 or 'errors' in query_response:
                print('ERROR - GraphQL executed with errors ' + str(query_response['errors']))
            else:
                pass
                #print('Successfully loaded data -- 200\nresponse: ' + str(query_response.json()))
            if continue_run:
                self.delete_list_fs(fs_ids, fs_type)

if __name__ == '__main__':

    confirmation_word = 'popx'
    print(sys.argv)
    if len(sys.argv) >= 2:
        mode = sys.argv[1]
    else:
        mode = False
    if mode!='--da':
        confirmation = input(f'Type {confirmation_word} to delete all the FS in the workspace:')
    fs_types_to_delete = ['Application'] 
    categories = {
            #'Process': ['organizationalProcess', 'systemProcess']
            }
    counter = 0 

    if (confirmation == confirmation_word or mode=='--da'):
        test = LeanIX()
        
        for fs_type in fs_types_to_delete:
            if mode!='--da':
                print(f'deleting all {fs_type} in the workspace, press enter to confirm')
                input()
            
            df_fs = test.get_fs_data(fs_type=fs_type, categories=(categories.get(fs_type) if categories.get(fs_type) else []))

            print(f'{len(df_fs.index)}# will be deleted')

#TODO: count FS deleted
            print(df_fs['id'].to_list())
            test.delete_list_fs(fs_ids = df_fs['id'].to_list(), fs_type=fs_type)

        #print(f'{counter} factsheets have been deleted')

