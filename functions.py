import datetime
import json
import time
import warnings
from typing import Dict, List , Union 
import numpy as np

import bs4
import pandas as pd
import requests
from copy import deepcopy
from bs4 import BeautifulSoup
from mysql.connector.constants import ClientFlag
from requests.auth import HTTPBasicAuth
from sqlalchemy import false
from sqlalchemy import create_engine
from tqdm import tqdm
import xml.etree.ElementTree as ET
import csv
import tempfile
import datetime as dt
import time
from datetime import datetime, timedelta
import re
# ----------------------------------------------------------------------------------------------------------------------------------------
class DownloadDetails:
    @staticmethod
    def data_formatting(surveyIDs):
            '''
            This function performs data cleaning operations on extracted subject ids stored in a list.
            Gets response text from API pull and removes unwanted punctuations. 
            
            Argument:
                surveyIDs (list)
            
            Returns:
                list: An expected list format
            '''
            surveyIDs = surveyIDs.replace('[', '')
            surveyIDs = surveyIDs.replace(']', '')
            surveyIDs = surveyIDs.replace('\r', '')
            surveyIDs = surveyIDs.replace('\n', '')
            surveyIDs = surveyIDs.strip()
            surveyIDs = surveyIDs.replace(' ', '')
            surveyIDs = surveyIDs.split(',')
            return surveyIDs

# ------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_store_id(api_key, username, password, survey_id, prime_date):
        """
        This function extracts the survey ids given a specific survey id and date periods.
        The data cleaning function is then called on the extracted ids.

        Arguments:
            prime_date (iterator variable from list)
            survey_id (str)

        Returns:
            list: List containing subject ids for the prime date
        """
        start_date = end_date = prime_date
        print(start_date, end_date, sep='\n')

        # Download features
        url = f'http://api.dooblo.net/newapi/SurveyInterviewIDs?surveyIDs={survey_id}&dateStart={start_date}T00%3a00%3a00.0000000%2b00%3a00&dateEnd={end_date}T23%3a59%3a59.9990000%2b00%3a00&dateType=Upload'
        payload = {}
        headers = {
            'Cookie': 'ASP.NET_SessionId=fqtuuiimuc0ij43ejti02ktu'
        }
        response = requests.request("GET", url, headers=headers, data=payload, auth=HTTPBasicAuth(
            f"{api_key}/{username}", f"{password}"))
        print('HTTP Request response completed')
        surveyIDs = response.text
        backbone = DownloadDetails()
        list_subjects = backbone.data_formatting(surveyIDs)
        print(f"Number of subject ids: {len(list_subjects)}") # store extracted subject_ids in a list
        print("-"*40)
        return list_subjects  

# ------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def download_xml(subj_id, survey_id, api_key, username, password):
        """
        This function downloads retail audit data for a given subject id. Makes API call to STG servers.
        Displays information containing outlet name, auditor, audited items.
        
        Arguments:
            subj_id (str): a single subject id from the list of subject ids
            survey_id (str): ID for survey
            api_key (str): API Key
            username (str): User name
            password (str): User password
        
        Returns:
            response (xml formatted data)
        """    
        url = f"http://api.dooblo.net/newapi/SurveyInterviewData?subjectIDs={subj_id}&surveyID={survey_id}&onlyHeaders=false&includeNulls=false"
        payload = {}
        headers = {
            'Cookie': 'ASP.NET_SessionId=fqtuuiimuc0ij43ejti02ktu',
            'Accept': 'text/xml',
            'accept-encoding': 'UTF-8'
        }
        response = requests.request("GET", url, headers=headers, data=payload, auth=HTTPBasicAuth(
            f"{api_key}/{username}", f"{password}"))
        return response

# ------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def xml_to_list(response):
        """
        This function parses the xml formatted data into a list format.
        
        Argument:
            response(xml formatted data)
        
        Returns:
            data(list)
        """
        # Initialize lists to store data
        data = []

        # Parse the XML content directly from the response text
        root = ET.fromstring(response.text)

        # Extract data from XML tree
        for element in root.iter():
            if element.text and element.text.strip():
                data.append([element.tag, "text", element.text])
            for attribute, value in element.attrib.items():
                data.append([element.tag, attribute, value])
        return data

# ------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------------
class RecruitmentDownload:
    @staticmethod
    def transform_recruitment_items(subj_id, survey_id, api_key, username, password):
        """
        This function creates and stores the data expected for new items in a dataframe.
        The variables that define new items are selected and manipulated in order to arrive
        at a desired dataframe format. Responses from the FullVariable, QuestionAnswer sections
        are selected.
        
        Arguments:
            subj_id (str)
            survey_id (str)
            api_key (str)
            username (str)
            password (str)
        
        Returns:
            df_items_store_details (pd.DataFrame)
        """
        try:
            backbone = DownloadDetails()
            response = backbone.download_xml(subj_id, survey_id, api_key, username, password)
            data_list = backbone.xml_to_list(response)
            df = pd.DataFrame(data_list, columns=["Element", "Attribute", "Value"])
            df_extract = df[df['Element'].isin(['FullVariable','QuestionAnswer','SubjectNum','Upload'])] # the loaded csv file has columns Element, Attribute and Value. In the element column, for items, we pick the listed variables of interest ie. fullvariable, questionanswer,...
            df_result_extract = df_extract[df_extract['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']] # picks selected data points with Attribute as 'Text' only, resets the index to start from 0 and then drops other columns leaving Element and Value

            # Pick Subject number, Upload date for now...any other special column can come here
            store_info = df_result_extract.iloc[-2:].transpose().reset_index(drop=True) # picks subject number and upload date
            store_info = store_info.rename(columns=store_info.iloc[0]).drop(store_info.index[0]) # remove headers
            store_info.reset_index()

            # Recruitment items
            # Extracting the Outlet code
            # Had to create two instances for how api returns outlet code and outlet name
            df_one = pd.DataFrame()
            if not df_result_extract.empty:
                try:
                    df_one_main = df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'Outlet_Code_'][0]:df_result_extract.index[df_result_extract['Value'] == 'OutletName'][0], :]
                except IndexError:
                    print("Subsetting 1 failed!")
                    try:
                        df_one_annex = df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'Outlet_Name'][0]:df_result_extract.index[df_result_extract['Value'] == 'Outlet_Code'][0]+1, :]
                    except IndexError:
                        print("Subsetting 2 failed!")
                    else:
                        if not df_one_annex.empty:
                            df_one = df_one_annex.reset_index(drop=True)
                            df_one = df_one.iloc[[2,3],:]
                else:
                    if not df_one_main.empty:
                        df_one = df_one_main.reset_index(drop=True)
                        df_one = df_one.iloc[[0,1],:]
                    else:
                        print("Subsetting 1 assignment failed!")
            else:
                print("The dataframe is empty")
            
            store_id = df_one['Value'].to_frame().reset_index(drop=True)
            store_id = store_id.rename(columns=store_id.iloc[0]).drop(store_id.index[0]) # remove headers
            store_id.reset_index()

            # Extracting info on items in the store
            df_two = df_result_extract['Value'].to_frame()
            df_items= df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'I_1_Export_Category'][0]:, :] # select from export_category to Export_price
            df_items_use = df_items.iloc[:-8]['Value'].to_frame().reset_index(drop=True)

            # Merge outlet code with subject number and upload date for use
            df_store_details = pd.concat([store_info, store_id], axis=1)

            ## Sorting out item details, rearranging them to all exist in one column per variable type
            transformed_data = {} # # Instantiate empty dict

            # # Loop through the DataFrame in steps of 2
            for i in range(0, len(df_items_use)-1, 2):
                # Use the first value as the header and the second value as the value beneath the header
                header = df_items_use.iloc[i, 0]
                value = df_items_use.iloc[i+1, 0]
                # Add the header and value to the dictionary
                transformed_data[header] = value
            df_sample = pd.DataFrame([transformed_data])

            #Manipulating resulting dataframe
            df_sample.columns = df_sample.columns.str.replace(r'I_\d+_', '', regex=True) # change made here
            melted_df = df_sample.melt()

            # # Create a new column to group the rows by
            melted_df['Group'] = (melted_df['variable'] == 'Export_Category').cumsum()

            # Pivot the DataFrame to transform the "variable" column into columns
            melted_df = melted_df.pivot_table(index='Group', columns='variable', values='value', aggfunc=sum) # aggfunc here is to deal with entry columns with duplicates, can't tell where duplicate even is ie. specific column!!! serious attention
            # Reset the index to make "Group" a regular column
            melted_df.reset_index(inplace=True)
            melted_df_edit = melted_df.drop(columns=['Group'], axis=0)
            # Re-order columns
            melted_df_final = melted_df_edit.iloc[:,[4,3,6,1,12,11,8,2,14,9,7,15,13,5,0,10]]

            # Finally, put all details together
            df_items_store_details = pd.concat([df_store_details, melted_df_final], axis=1)

            ## Fill in remaining rows of missing values
            if 'Outlet_Code_' in df_items_store_details.columns:
                df_items_store_details['Outlet_Code_'] = df_items_store_details['Outlet_Code_'].fillna(df_items_store_details['Outlet_Code_'].iloc[0])
            elif 'Outlet_Code' in df_items_store_details.columns:
                df_items_store_details['Outlet_Code'] = df_items_store_details['Outlet_Code'].fillna(df_items_store_details['Outlet_Code'].iloc[0])
            df_items_store_details['Upload'] = df_items_store_details['Upload'].fillna(df_items_store_details['Upload'].iloc[0])
            df_items_store_details['SubjectNum'] = df_items_store_details['SubjectNum'].fillna(df_items_store_details['SubjectNum'].iloc[0])
        except KeyError:
            print("Null")
        else:
            return df_items_store_details

# ------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def transform_recruitment_profile(subj_id, survey_id, api_key, username, password):
        """
        Extract the values you need
        The parsed xml data is grouped into chapters. The target chapters list is used to select the desired chapters.
        The chapters are stored in a dataframe from which the necessary variables are selected.
        """
        try:
            backbone = DownloadDetails()
            response = backbone.download_xml(subj_id, survey_id, api_key, username, password)
            data_list = backbone.xml_to_list(response)
            df = pd.DataFrame(data_list, columns=["Element", "Attribute", "Value"])
            
            target_chapters = ['Outlet Profile', 'Outlet Location', 'Contacts', 'Add New Items', 'Chapter 13', 'Observations']

            # Initialize variables
            selected_dataframes = {}
            current_chapter = None
            current_rows = []

            for index, row in df.iterrows():
                if row['Element'] == 'ChapterName':
                    if row['Value'] in target_chapters:
                        if current_chapter:
                            selected_dataframes[current_chapter] = pd.DataFrame(current_rows)
                        current_chapter = row['Value']
                        current_rows = [row]
                else:
                    current_rows.append(row)

            # Add the last chapter DataFrame after processing all rows
            if current_chapter:
                selected_dataframes[current_chapter] = pd.DataFrame(current_rows)
            # Save the dataframes with names based on the list names
            for chapter, chapter_df in selected_dataframes.items():
                df_name = f"{chapter.replace(' ', '_').lower()}_df"
                globals()[df_name] = chapter_df

            # Select the rows from the last chapter to the end
            last_chapter = target_chapters[-1]
            last_chapter_df = selected_dataframes.get(last_chapter, pd.DataFrame())

            # Outlet Profile
            outlet_profile_df.columns = ["Element", "Attribute", "Value"]
            df_profile_extract = outlet_profile_df[outlet_profile_df['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']]
            selected_rows_1 = df_profile_extract.loc[(df_profile_extract['Element'] == 'FullVariable') | (df_profile_extract['Element'] == 'QuestionAnswer')]

            # Outlet Location
            outlet_location_df.columns = ["Element", "Attribute", "Value"]
            df_profile_extract = outlet_location_df[outlet_location_df['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']]
            selected_rows_2 = df_profile_extract.loc[(df_profile_extract['Element'] == 'FullVariable') | (df_profile_extract['Element'] == 'QuestionAnswer')]

            # Add new items
            add_new_items_df.columns = ["Element", "Attribute", "Value"]
            df_profile_extract = add_new_items_df[add_new_items_df['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']]
            selected_rows_3 = df_profile_extract.loc[(df_profile_extract['Element'] == 'FullVariable') | (df_profile_extract['Element'] == 'QuestionAnswer')]

            # Observations
            observations_df.columns = ["Element", "Attribute", "Value"]
            df_profile_extract = observations_df[observations_df['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']]
            selected_rows_4 = df_profile_extract.loc[(df_profile_extract['Element'] == 'FullVariable') | (df_profile_extract['Element'] == 'QuestionAnswer')]
            selected_rows_4 = selected_rows_4.drop(selected_rows_4.loc[selected_rows_4['Value'] == 'Items_Added'].index) # Drop Items added variable

            # Special variables that do not appear in FullVariable-QuestionAnswer pairing ## extracted from observations_df due to sequential code running
            spare_df = df_profile_extract.loc[(df_profile_extract['Element'] == 'SubjectNum') | (df_profile_extract['Element'] == 'VisitStart')|\
                                                    (df_profile_extract['Element'] == 'VisitEnd') | (df_profile_extract['Element'] == 'ClientDuration')|\
                                                    (df_profile_extract['Element'] == 'Upload')|(df_profile_extract['Element'] == 'Duration')|\
                                                        (df_profile_extract['Element'] == 'Date')|(df_profile_extract['Element'] == 'SurveyorName')]

            spare_df = spare_df.reset_index(drop=True)
            spare_df_clean = spare_df.rename(columns=spare_df.iloc[0]).drop(spare_df.index[0]).dropna(axis=0) # remove headers    
            result = spare_df_clean.transpose(copy=True).reset_index()

            # Rename the columns and rows
            result.columns = result.iloc[0]
            result = result.iloc[1:]
            result = result.dropna().reset_index(drop=True)

            # Combine them
            combined_df = pd.concat([selected_rows_1, selected_rows_2, selected_rows_3, selected_rows_4]).reset_index(drop=True).drop(["Element"], axis=1)
            
            ## Sorting out item details, rearranging them to all exist in one column per variable type
            transformed_data = {} # # Instantiate empty dict

            # # Loop through the DataFrame in steps of 2
            for i in range(0, len(combined_df)-1, 2):
                # Use the first value as the header and the second value as the value beneath the header
                header = combined_df.iloc[i, 0]
                value = combined_df.iloc[i+1, 0]
                # Add the header and value to the dictionary
                transformed_data[header] = value
            df_sample = pd.DataFrame([transformed_data])
            
            # Combine dataframes and drop N/As
            new_combined_df = pd.concat([df_sample, result], axis=1)
            new_combined_df = new_combined_df.dropna(how='all', axis=0)
            # Reorder and Rename columns
            new_combined_df = new_combined_df.iloc[:,[23,24,25,26,27,30,28,29,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21]]
            new_combined_df_renamed = new_combined_df.rename(columns={"ClientDuration":"NetDuration"})
        except NameError:
            print("Null")
        except KeyError:
            print("Null")
        else:
            return new_combined_df_renamed

# ------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------------
class AuditCaptureDetails:
    @staticmethod
    def transform_new_items(subj_id, survey_id, api_key, username, password):
        """
        This function creates and stores the data expected for new items in a dataframe.
        The variables that define new items are selected and manipulated in order to arrive
        at a desired dataframe format. Responses from the FullVariable, QuestionAnswer sections
        are selected.
        
        Arguments:
            subj_id (str)
            survey_id (str)
            api_key (str)
            username (str)
            password (str)
        
        Returns:
            df_items_store_details (pd.DataFrame)
        """
        try:
            backbone = DownloadDetails()
            response = backbone.download_xml(subj_id, survey_id, api_key, username, password)
            data_list = backbone.xml_to_list(response)
            df = pd.DataFrame(data_list, columns=["Element", "Attribute", "Value"])
            df_extract = df[df['Element'].isin(['FullVariable','QuestionAnswer','SubjectNum','Upload'])] # the loaded csv file has columns Element, Attribute and Value. In the element column, for items, we pick the listed variables of interest ie. fullvariable, questionanswer,...
            df_result_extract = df_extract[df_extract['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']] # picks selected data points with Attribute as 'Text' only, resets the index to start from 0 and then drops other columns leaving Element and Value

            # Pick Subject number, Upload date for now...any other special column can come here
            store_info = df_result_extract.iloc[-2:].transpose().reset_index(drop=True) # picks subject number and upload date
            store_info = store_info.rename(columns=store_info.iloc[0]).drop(store_info.index[0]) # remove headers
            store_info.reset_index()
        
            # New items
            # Extracting the Outlet code
            # Had to create two instances for how api returns outlet code and outlet name
            df_one = pd.DataFrame()
            if not df_result_extract.empty:
                try:
                    df_one_main = df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'Outlet_Code_'][0]:df_result_extract.index[df_result_extract['Value'] == 'OutletName'][0], :]
                except IndexError:
                    print("Subsetting 1 failed!")
                    try:
                        df_one_annex = df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'Outlet_Name'][0]:df_result_extract.index[df_result_extract['Value'] == 'Outlet_Code'][0]+1, :]
                    except IndexError:
                        print("Subsetting 2 failed!")
                    else:
                        if not df_one_annex.empty:
                            df_one = df_one_annex.reset_index(drop=True)
                            df_one = df_one.iloc[[2,3],:]
                else:
                    if not df_one_main.empty:
                        df_one = df_one_main.reset_index(drop=True)
                        df_one = df_one.iloc[[0,1],:]
                    else:
                        print("Subsetting 1 assignment failed!")
            else:
                print("The dataframe is empty")
            
            store_id = df_one['Value'].to_frame().reset_index(drop=True)
            store_id = store_id.rename(columns=store_id.iloc[0]).drop(store_id.index[0]) # remove headers
            store_id.reset_index()

            # Extracting info on items in the store
            df_two = df_result_extract['Value'].to_frame()
            df_items= df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'I_1_Export_Category'][0]:, :] # select from export_category to Export_price
            df_items_use = df_items.iloc[:-8]['Value'].to_frame().reset_index(drop=True)

            # Merge outlet code with subject number and upload date for use
            df_store_details = pd.concat([store_info, store_id], axis=1)

            ## Sorting out item details, rearranging them to all exist in one column per variable type
            transformed_data = {} # Instantiate empty dict

            # # Loop through the DataFrame in steps of 2
            for i in range(0, len(df_items_use), 2):
                # Use the first value as the header and the second value as the value beneath the header
                header = df_items_use.iloc[i, 0]
                value = df_items_use.iloc[i+1, 0]
                # Add the header and value to the dictionary
                transformed_data[header] = value
            df_sample = pd.DataFrame([transformed_data])

            #Manipulating resulting dataframe
            df_sample.columns = df_sample.columns.str.replace(r'I_\d+_', '', regex=True) # change made here
            melted_df = df_sample.melt()

            # # Create a new column to group the rows by
            melted_df['Group'] = (melted_df['variable'] == 'Export_Category').cumsum()

            # Pivot the DataFrame to transform the "variable" column into columns
            melted_df = melted_df.pivot_table(index='Group', columns='variable', values='value', aggfunc=sum) # aggfunc here is to deal with entry columns with duplicates, can't tell where duplicate even is ie. specific column!!! serious attention
            # Reset the index to make "Group" a regular column
            melted_df.reset_index(inplace=True)
            melted_df_edit = melted_df.drop(columns=['Group'], axis=0)
            # Re-order columns
            melted_df_final = melted_df_edit.iloc[:,[4,3,6,1,12,11,8,2,14,9,7,15,13,5,0,10]]

            # Finally, put all details together
            df_items_store_details = pd.concat([df_store_details, melted_df_final], axis=1)

            ## Fill in remaining rows of missing values
            # Select which outlet code to use
            if 'Outlet_Code_' in df_items_store_details.columns: 
                df_items_store_details['Outlet_Code_'] = df_items_store_details['Outlet_Code_'].fillna(df_items_store_details['Outlet_Code_'].iloc[0])
            elif 'Outlet_Code' in df_items_store_details.columns:
                df_items_store_details['Outlet_Code'] = df_items_store_details['Outlet_Code'].fillna(df_items_store_details['Outlet_Code'].iloc[0])
            df_items_store_details['Upload'] = df_items_store_details['Upload'].fillna(df_items_store_details['Upload'].iloc[0])
            df_items_store_details['SubjectNum'] = df_items_store_details['SubjectNum'].fillna(df_items_store_details['SubjectNum'].iloc[0])
        except IndexError:
            print("Null")
        else:
            return df_items_store_details
    
# ------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def transform_audit_capture_profile(subj_id, survey_id, api_key, username, password):
        """
        Extract the values you need
        The parsed xml data is grouped into chapters. The target chapters list is used to select the desired chapters.
        The chapters are stored in a dataframe from which the necessary variables are selected.
        """
        try:
            backbone = DownloadDetails()
            response = backbone.download_xml(subj_id, survey_id, api_key, username, password)
            data_list = backbone.xml_to_list(response)
            df = pd.DataFrame(data_list, columns=["Element", "Attribute", "Value"])
            
            target_chapters = ['Outlet Details', 'Audit Caputre', 'Info Display', 'Auto-code Audit', 'Name']

            # Initialize variables
            selected_dataframes = {}
            current_chapter = None
            current_rows = []

            for index, row in df.iterrows():
                if row['Element'] == 'ChapterName':
                    if row['Value'] in target_chapters:
                        if current_chapter:
                            selected_dataframes[current_chapter] = pd.DataFrame(current_rows)
                        current_chapter = row['Value']
                        current_rows = [row]
                else:
                    current_rows.append(row)

            # Add the last chapter DataFrame after processing all rows
            if current_chapter:
                selected_dataframes[current_chapter] = pd.DataFrame(current_rows)
            # Save the dataframes with names based on the list names
            for chapter, chapter_df in selected_dataframes.items():
                df_name = f"{chapter.replace(' ', '_').lower()}_df"
                globals()[df_name] = chapter_df

            # Select the rows from the last chapter to the end
            last_chapter = target_chapters[-1]
            last_chapter_df = selected_dataframes.get(last_chapter, pd.DataFrame())

            # Info display # Pick old_items_count
            info_display_df.columns = ["Element", "Attribute", "Value"]
            df_profile_extract = info_display_df[info_display_df['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']]
            selected_rows_1 = df_profile_extract.loc[(df_profile_extract['Element'] == 'FullVariable') | (df_profile_extract['Element'] == 'QuestionAnswer')]
            #print(selected_rows_1)
            
            # Outlet Details
            outlet_details_df.columns = ["Element", "Attribute", "Value"]
            df_profile_extract = outlet_details_df[outlet_details_df['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']]
            selected_rows_2 = df_profile_extract.loc[(df_profile_extract['Element'] == 'FullVariable') | (df_profile_extract['Element'] == 'QuestionAnswer')|\
                                                    (df_profile_extract['Element'] == 'Text') | (df_profile_extract['Element'] == 'TopicAnswer')]
            df_remove = selected_rows_2.loc[(selected_rows_2['Element'] == 'Text') & (selected_rows_2['Value'].isin(['Auditor\'s Name', 'Outlet Code', 'Outlet Name',\
                                                                                                                    'No', 'Previous Item Count', 'DAYS BETWEEN LAST AUDIT',\
                                                                                                                    'Opened', 'Is this shop open or closed during time of visit.']))]
            selected_rows_2 = selected_rows_2.drop(df_remove.index)
            df_remove_other = selected_rows_2.loc[(selected_rows_2['Element'] == 'FullVariable') & (selected_rows_2['Value'].isin(['Profile_Details']))]
            selected_rows_2 = selected_rows_2.drop(df_remove_other.index)
            df_remove_again = selected_rows_2.loc[(selected_rows_2['Element'] == 'Text') & (selected_rows_2['Value'].isin(['Temporarily Closed']))]
            selected_rows_2 = selected_rows_2.drop(df_remove_again.index)
            df_remove_permanent = selected_rows_2.loc[(selected_rows_2['Element'] == 'Text') & (selected_rows_2['Value'].isin(['Permanetly Closed']))]
            selected_rows_2 = selected_rows_2.drop(df_remove_permanent.index)
            selected_rows_2 = selected_rows_2.drop(index=116)
            #print(selected_rows_2)

            # Name # Pick item count and observations
            name_df.columns = ["Element", "Attribute", "Value"]
            df_profile_extract = name_df[name_df['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']]
            selected_rows_3 = df_profile_extract.loc[(df_profile_extract['Element'] == 'FullVariable') | (df_profile_extract['Element'] == 'QuestionAnswer')]
            
            # Remove reason part from the selected_rows_3
            remove_reason_index = selected_rows_3.loc[(selected_rows_3['Element'] == 'FullVariable') & (selected_rows_3['Value'] == 'Reason')].index.tolist()

            if len(remove_reason_index) > 1:
                selected_rows_3 = selected_rows_3.drop(remove_reason_index)

            # Assuming you have extracted the data into separate DataFrames info_display_df, outlet_details_df, and name_df
            # Extract 'Reason' and its corresponding 'QuestionAnswer' response
            reason_df = name_df[(name_df['Element'] == 'FullVariable') & (name_df['Value'] == 'Reason')]

            # Create a new DataFrame with the FullVariable row
            new_reason_df = reason_df.copy()

            # Find the index of the FullVariable row
            full_variable_index = reason_df[reason_df['Element'] == 'FullVariable'].index

            # Find the index of the QuestionAnswer row that follows the FullVariable row
            question_answer_index = full_variable_index + 3

            # Add the QuestionAnswer row to the new DataFrame
            new_reason_df = new_reason_df.append(name_df.loc[question_answer_index])
            new_reason_df = new_reason_df.drop('Attribute', axis=1)
            
            # Special variables that do not appear in FullVariable-QuestionAnswer pairing
            spare_df = df_profile_extract.loc[(df_profile_extract['Element'] == 'SubjectNum') | (df_profile_extract['Element'] == 'VisitStart')|\
                                                    (df_profile_extract['Element'] == 'VisitEnd') | (df_profile_extract['Element'] == 'ClientDuration')|\
                                                    (df_profile_extract['Element'] == 'Upload')|(df_profile_extract['Element'] == 'Duration')|\
                                                        (df_profile_extract['Element'] == 'Date')]

            spare_df = spare_df.reset_index(drop=True)
            spare_df_clean = spare_df.rename(columns=spare_df.iloc[0]).drop(spare_df.index[0]).dropna(axis=0) # remove headers    
            result = spare_df_clean.transpose(copy=True).reset_index()

            # Rename the columns and rows
            result.columns = result.iloc[0]
            result = result.iloc[1:]
            result = result.dropna().reset_index(drop=True)

            # # Combine them
            combined_df = pd.concat([selected_rows_1, selected_rows_2, selected_rows_3, reason_df]).reset_index(drop=True).drop(["Element"], axis=1)
            
            ## Sorting out item details, rearranging them to all exist in one column per variable type
            transformed_data = {} # # Instantiate empty dict

            # # Loop through the DataFrame in steps of 2
            for i in range(0, len(combined_df)-1, 2):
                # Use the first value as the header and the second value as the value beneath the header
                header = combined_df.iloc[i, 0]
                value = combined_df.iloc[i+1, 0]
                # Add the header and value to the dictionary
                transformed_data[header] = value
            df_sample = pd.DataFrame([transformed_data])
            
            # Combine dataframes and drop N/As
            new_combined_df = pd.concat([df_sample, result], axis=1)
            # Rename columns
            new_combined_df_renamed = new_combined_df.rename(columns={"Outlet Type Description":"Outlet Type", "Name of owner":"Outlet Owner",
                                                                    "Selling Area":"Outlet Size", "ClientDuration":"NetDuration", 
                                                                    "Q_105":"Submit", "Q_128":"Submit"})
            # Reorder columns
            # Initialize the DataFrames
            df_with_reason = None
            df_without_reason = None

            if 'Reason' in new_combined_df_renamed.columns:
                df_with_reason = new_combined_df_renamed.loc[:,['SubjectNum','Date','Surveyor','Duration','NetDuration','Upload','VisitStart','VisitEnd','Outlet_Code_','OutletName',\
                                                        'CurrDate','Previous Date','Country','City','Outlet Type','Outlet Type Code','Outlet Owner','Outlet Size','Street Name',\
                                                            'Landmark','Locality','Previous Outlet Status','Channel','Cell_Name','Prev_Item_Count',	'days','GPS',\
                                                            'Submit','TC_PL','Reason']]
            else:
                df_without_reason = new_combined_df_renamed.loc[:,['SubjectNum','Date','Surveyor','Duration','NetDuration','Upload','VisitStart','VisitEnd','Outlet_Code_','OutletName',\
                                                        'CurrDate','Previous Date','Country','City','Outlet Type','Outlet Type Code','Outlet Owner','Outlet Size','Street Name',\
                                                            'Landmark','Locality','Previous Outlet Status','Channel','Cell_Name','Prev_Item_Count',	'days','GPS','old_items_count',\
                                                            'ItemCount','Observations','Submit','TC_PL']]
            # Concatenate the two DataFrames
            final_combined_renamed_df = pd.concat([df_with_reason, df_without_reason], ignore_index=True)
        except NameError:
            print("Empty columns")
        except KeyError:
            print("Too many missing columns")
        else:
            return final_combined_renamed_df
# ------------------------------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def transform_old_items(subj_id, survey_id, api_key, username, password):
        """
        Extract the values you need
        Load csv file into dataframe and then select responses in the FullVariable, QuestionAnswer sections
        Create a DataFrame from the extracted data
        """
        try:
            backbone = DownloadDetails()
            response = backbone.download_xml(subj_id, survey_id, api_key, username, password)
            data_list = backbone.xml_to_list(response)
            df = pd.DataFrame(data_list, columns=["Element", "Attribute", "Value"])
            df_extract_1 = df[df['Element'].isin(['FullVariable','QuestionAnswer'])] # the loaded csv file has columns Element, Attribute and Value. In the element column, for items, we pick the listed variables of interest ie. fullvariable, questionanswer,...
            df_extract_1_new = df_extract_1.loc[df_extract_1.index[df_extract_1['Value'] == 'I_1_Purch_Item_Name'][0]:'I_1_Export_Category', :]
            df_extract_1_final = df_extract_1_new.drop(df_extract_1_new[df_extract_1_new['Value'].str.match(r'I_\d+_Purch_Item_Details')].index)
            df_extract_1_final = df_extract_1_final.reset_index(drop=True)
            df_extract_1_final['Value'] = df_extract_1_final['Value'].str.replace(r'^I_\d+_', '', regex=True)
            vanish_var= df_extract_1_final.loc[df_extract_1_final.index[df_extract_1_final['Value'] == 'Export_Category'][0]:, :]
            df_extract_1_final = df_extract_1_final.drop(vanish_var.index)

            catch_list = ["Purch_Item_Name", "Purch_Prev_Foward_Stock",	"Purch_Prev_Back_Stock", "FowStock",\
                        "BackStock", "Purch_Barcode", "Purch_Country", "Previous_Price", "Current_Price",\
                        "Prev_Doc_Purch", "Prev_Oral_Purch", "Doc_Purch", "Oral_Purch", "Opening_Stock",\
                        "Closing_Stock", "Final_Price",	"Prev_Purchases", "Current_Purchases", "Prev_Sales",\
                        "Sales", "Sales_Reason", "Item_Observation"]
            
            nouveau_data = {name: [] for name in catch_list}

            # Initialize the current name
            present_name = None

            # Iterate through df_extract_2_new and populate the new dataframe dictionary
            for indexe, rowe in df_extract_1_final.iterrows():
                if rowe['Element'] == 'FullVariable' and rowe['Value'] in catch_list:
                    present_name = rowe['Value']
                elif rowe['Element'] == 'QuestionAnswer' and present_name is not None:
                    nouveau_data[present_name].append(rowe['Value'])

            # Fill in any missing values with 'NA'
            max_lengthe = max(len(nouveau_data[name]) for name in catch_list)
            for name in catch_list:
                nouveau_data[name] += ['NA'] * (max_lengthe - len(nouveau_data[name]))

            # Create the new dataframe
            df_nice = pd.DataFrame(nouveau_data)
            df_nice['Purch_Item_Name'] = df_nice['Purch_Item_Name'].str.replace(r'\s*\([^)]*\)', '', regex=True) # remove text in bracket

            # -------------------------------------------------------------------------------------------------------------------------------------
            # Pick country of origin
            # Create an empty list to store the extracted data 
            origin_holder = []

            # Iterate through each row of the DataFrame
            for index_3, row_3 in df.iterrows():
                if row_3['Element'] == 'FullVariable':
                    full_variable = row_3['Value']
                    if re.match(r'I_\d+_Country_Origin', full_variable):
                        variable = None
                        country_origin = None
                        for i in range(index_3+1, len(df)):
                            if df.iloc[i]['Element'] == 'Variable':
                                variable = df.iloc[i]['Value']
                            elif df.iloc[i]['Element'] == 'QuestionAnswer':
                                country_origin = df.iloc[i]['Value']
                                origin_holder.append({'Country_Origin': country_origin})
                                break

            # Create a new DataFrame from the extracted data
            df_extract_country = pd.DataFrame(origin_holder)

            # Add up first two final dfs
            df_old_items_part_1 = pd.concat([df_nice, df_extract_country], axis=1)

        # -------------------------------------------------------------------------------------------------------------------------------------------------
            df_extract_2 = df[df['Element'].isin(['Text','TopicAnswer'])]
            df_extract_2_new = df_extract_2.loc[df_extract_2.index[df_extract_2['Value'] == 'ITEM_NAME'][0]:'FORWARD STOCK', :]
            df_extract_2_new = df_extract_2_new.reset_index(drop=True)
            df_remove = df_extract_2_new.loc[(df_extract_2_new['Element'] == 'Text') & (df_extract_2_new['Value'].isin(['Barcode', 'Country of Origin', 'Forward stock (Shelf)',\
                                                                                                                    'Backward stock (Store Room)', 'Sales Price per single items(2 decimal place)',\
                                                                                                                    'Item/Product Name/Description','Scan Barcode', 'Category Code', 'Previous Sales Price',\
                                                                                                                    'Item Name', 'Item Barcode', 'Segment Code', 'Manufacturer', 'Brand Name',\
                                                                                                                    'Flavour/Variant', 'Item Code', 'Item weight/ volume', 'old items count',\
                                                                                                                    'Reason for a 50% increase or decrease<br>', 'Previous Forward Stock', 'Previous Backward Stock',\
                                                                                                                    'Previous Documented Purchases', 'Previous Oral Purchases', 'Documented Purchases', 'Oral Purchases',\
                                                                                                                    'Open Stock', 'Closing Stock', 'Final Price', 'Previous Sales', 'Sales',\
                                                                                                                    'Item/Product Name', 'Number of New Items recorded', 'Field observations and Notes/Comments from the Auditor',\
                                                                                                                    'Would you like to submit this survey?', 'Yes', 'Ex_Price_Reason', 'Reason for a 50% increase or decrease',\
                                                                                                                    'Item/Product Name/ Description', 'Item unit', 'Forward stock (Shelf) {0}',\
                                                                                                                    'Backward stock (Store Room) {0}', 'Item/Product Name&nbsp;/ Description' ]))]

            df_extract_2_new = df_extract_2_new.drop(df_remove.index)
            df_remove_other = df_extract_2_new.loc[(df_extract_2_new['Element'] == 'Text') & (df_extract_2_new['Value'].isin(['Category']))]
            df_extract_2_new = df_extract_2_new.drop(df_remove_other.index)
            df_remove_again = df_extract_2_new.loc[(df_extract_2_new['Element'] == 'Text') & (df_extract_2_new['Value'].isin(['Segment']))]
            df_extract_2_new = df_extract_2_new.drop(df_remove_again.index)
            df_remove_permanent = df_extract_2_new.loc[(df_extract_2_new['Element'] == 'Text') & (df_extract_2_new['Value'].isin(['Packaging']))]
            df_extract_2_new = df_extract_2_new.drop(df_remove_permanent.index)
            df_remove_question = df_extract_2_new.loc[(df_extract_2_new['Element'] == 'Text') & (df_extract_2_new['Value'].isin(['Amount']))]
            df_extract_2_new = df_extract_2_new.drop(df_remove_question.index)

            # ----------------- ----------- ---------------------

            # Define the list of target names
            target_names = ['ITEM_NAME', 'ITEM_NO', 'BARCODE', 'CATEGORY', 'CATEGORY_CODE', 'SEGMENT', 'SEGMENTS_CODE', 'MANUFACTURER', 'BRAND_NAME', 'VARIANT', 'ITEM_CODE',\
                                'ITEM_WEIGHT', 'ITEM_UNIT', 'FORWARD_STOCK', 'BACKWARD_STOCK', 'SALES PRICE']
            
            new_data = {name: [] for name in target_names}

            # Iterate through df_extract_2_new and populate the new dataframe dictionary
            for index, row in df_extract_2_new.iterrows():
                if row['Element'] == 'Text' and row['Value'] in target_names:
                    current_name = row['Value']
                elif row['Element'] == 'TopicAnswer' and current_name is not None:
                    new_data[current_name].append(row['Value'])

            # Fill in any missing values with 'NA'
            max_length = max(len(new_data[name]) for name in target_names)
            for name in target_names:
                new_data[name] += ['NA'] * (max_length - len(new_data[name]))

            # Create the new dataframe
            df_new = pd.DataFrame(new_data)

            # Creating final final
            # Merge the DataFrames based on the common column with different names
            final_old_items_df = pd.merge(df_new, df_old_items_part_1, left_on='ITEM_NAME', right_on='Purch_Item_Name', how='left')
            final_old_items_df = final_old_items_df.loc[:,['Purch_Item_Name','ITEM_NAME','ITEM_NO','BARCODE','CATEGORY','CATEGORY_CODE','SEGMENT',\
                                                            'SEGMENTS_CODE','MANUFACTURER','BRAND_NAME','VARIANT','ITEM_CODE','ITEM_WEIGHT', 'FORWARD_STOCK',\
                                                            'ITEM_UNIT','Purch_Prev_Foward_Stock','Purch_Prev_Back_Stock','FowStock','BackStock','BACKWARD_STOCK', 'Purch_Barcode',\
                                                            'Purch_Country','Previous_Price','Current_Price','Prev_Doc_Purch','Prev_Oral_Purch','Doc_Purch',\
                                                            'Oral_Purch','Opening_Stock','Closing_Stock','Final_Price', 'Prev_Purchases', 'Current_Purchases','Prev_Sales','Sales',\
                                                            'SALES PRICE', 'Sales_Reason', 'Country_Origin', 'Item_Observation']]
    
        except IndexError:
            print("Missing")
        else:
            return final_old_items_df
