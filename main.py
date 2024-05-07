from functions import DownloadDetails, RecruitmentDownload, AuditCaptureDetails
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
import time

def main():    
    # Begin timer
    start = time.time()

    # Define lists for use
    new_final_df = [] # store processed info of all stores to produce a bigger df of outlets

    # Begin iteration
    api_key = 'f803b1f2-486e-4de7-9e6c-faa45366bb28'
    username = input("Enter your username: ")
    password = input("Enter your password: ")
    #print("NB. Select a single day when using audit capture profile!\n")
    print("Select one of the following file types:\n 1. audit capture profile\n 2. new items\n 3. old items\n 4. recruitment profile\n 5. recruitment items\n")
    file_type = input("What file do you want to download?: ")
    survey_id = input('Enter survey id: ') # input survey id
    first_day_str = input("Enter start date (YYYY-MM-DD): ")
    last_day_str = input("Enter end date (YYYY-MM-DD): ")
    
    # ------------------------------------------------------------

    if file_type == "audit capture profile":
        first_day = datetime.strptime(first_day_str, "%Y-%m-%d")
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d")
        
        date_list = [first_day + timedelta(days=x) for x in range((last_day - first_day).days + 1)]
        date_list_formatted = [date.strftime("%Y-%m-%d") for date in date_list]
        #print(date_list_formatted)
            
        for found_date in date_list_formatted:
            try:
                general = DownloadDetails()
                audit = AuditCaptureDetails()
                new_list = general.get_store_id(api_key, username, password, survey_id, found_date) # get store ids for given period, returns a list of store ids
                for outlet in new_list: # iterate through ids, transform their data and append them to a big list
                    dataframe_return = audit.transform_audit_capture_profile(outlet, survey_id, api_key, username, password)
                    new_final_df.append(dataframe_return)           
                # Save processed stores data
                merged_df = pd.concat(new_final_df, axis=0)
                merged_df.insert(0, 'Period', dt.datetime.today().replace(day=1).date().strftime('%Y-%m-%d')) # Add 'period' column
                merged_df.to_excel(f'audit_capture_profile_{first_day_str}___{last_day_str}.xlsx', index=False)
                #merged_df.to_sql(con=my_conn, name='audit_capture_profile', if_exists='replace', index=False)
                print("="*40)
                print("Finished process")
            except ValueError:
                print("No subject ids for the day!")
                print("="*40)
    # ------------------------------------------------------------
    elif file_type == "new items":
        first_day = datetime.strptime(first_day_str, "%Y-%m-%d")
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d")
        
        date_list = [first_day + timedelta(days=x) for x in range((last_day - first_day).days + 1)]
        date_list_formatted = [date.strftime("%Y-%m-%d") for date in date_list]
        #print(date_list_formatted)
            
        for found_date in date_list_formatted:
            try:
                general = DownloadDetails()
                audit = AuditCaptureDetails()
                new_list = general.get_store_id(api_key, username, password, survey_id, found_date) # get store ids for given period, returns a list of store ids
                for outlet in new_list: # iterate through ids, transform their data and append them to a big list
                    dataframe_return = audit.transform_new_items(outlet, survey_id, api_key, username, password)
                    new_final_df.append(dataframe_return)           
                # Save processed stores data
                merged_df = pd.concat(new_final_df, axis=0)
                merged_df.insert(0, 'Period', dt.datetime.today().replace(day=1).date().strftime('%Y-%m-%d')) # Add 'period' column
                merged_df.to_excel(f'new_items_{first_day_str}___{last_day_str}.xlsx', index=False)
                #merged_df.to_sql(con=my_conn, name='new_items', if_exists='replace', index=False)
                print("="*40)
                print("Finished process")
            except NameError:
                print("Null")
            except KeyError:
                print("No subject ids for the day!")
                print("="*40)
            except ValueError:
                print("No subject ids for the day!")
                print("="*40)
    # ------------------------------------------------------------
    elif file_type == "old items":
        first_day = datetime.strptime(first_day_str, "%Y-%m-%d")
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d")
        
        date_list = [first_day + timedelta(days=x) for x in range((last_day - first_day).days + 1)]
        date_list_formatted = [date.strftime("%Y-%m-%d") for date in date_list]
        #print(date_list_formatted)
            
        for found_date in date_list_formatted:
            try:
                general = DownloadDetails()
                audit = AuditCaptureDetails()
                new_list = general.get_store_id(api_key, username, password, survey_id, found_date) # get store ids for given period, returns a list of store ids
                for outlet in new_list: # iterate through ids, transform their data and append them to a big list
                    dataframe_return = audit.transform_old_items(outlet, survey_id, api_key, username, password) # not finished
                    new_final_df.append(dataframe_return)           
                # Save processed stores data
                merged_df = pd.concat(new_final_df, axis=0)
                merged_df.insert(0, 'Period', dt.datetime.today().replace(day=1).date().strftime('%Y-%m-%d')) # Add 'period' column
                merged_df.to_excel(f'old_items_{first_day_str}___{last_day_str}.xlsx', index=False)
                #merged_df.to_sql(con=my_conn, name='old_items', if_exists='replace', index=False)
                print("="*40)
                print("Finished process")
            except ValueError:
                print("No subject ids for the day!")
                print("="*40)
    # # ------------------------------------------------------------
    elif file_type == "recruitment items":
        first_day = datetime.strptime(first_day_str, "%Y-%m-%d")
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d")
        
        date_list = [first_day + timedelta(days=x) for x in range((last_day - first_day).days + 1)]
        date_list_formatted = [date.strftime("%Y-%m-%d") for date in date_list]
        #print(date_list_formatted)
            
        for found_date in date_list_formatted:
            try:
                general = DownloadDetails()
                recruit = RecruitmentDownload()
                new_list = general.get_store_id(api_key, username, password, survey_id, found_date) # get store ids for given period, returns a list of store ids
                for outlet in new_list: # iterate through ids, transform their data and append them to a big list
                    dataframe_return = recruit.transform_recruitment_items(outlet, survey_id, api_key, username, password)
                    new_final_df.append(dataframe_return)           
                # Save processed stores data
                merged_df = pd.concat(new_final_df, axis=0)
                merged_df.insert(0, 'Period', dt.datetime.today().replace(day=1).date().strftime('%Y-%m-%d')) # Add 'period' column
                merged_df.to_excel(f'recruitment_items_{first_day_str}___{last_day_str}.xlsx', index=False)
                #merged_df.to_sql(con=my_conn, name='recruitment_items', if_exists='replace', index=False)
                print("="*40)
                print("Finished process")
            except ValueError:
                print("No subject ids for the day!")
                print("="*40)
    # ------------------------------------------------------------
    elif file_type == "recruitment profile":
        first_day = datetime.strptime(first_day_str, "%Y-%m-%d")
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d")
        
        date_list = [first_day + timedelta(days=x) for x in range((last_day - first_day).days + 1)]
        date_list_formatted = [date.strftime("%Y-%m-%d") for date in date_list]
        #print(date_list_formatted)
            
        for found_date in date_list_formatted:
            try:
                general = DownloadDetails()
                recruit = RecruitmentDownload()
                new_list = general.get_store_id(api_key, username, password, survey_id, found_date) # get store ids for given period, returns a list of store ids
                for outlet in new_list: # iterate through ids, transform their data and append them to a big list
                    dataframe_return = recruit.transform_recruitment_profile(outlet, survey_id, api_key, username, password)
                    new_final_df.append(dataframe_return)           
                # Save processed stores data
                merged_df = pd.concat(new_final_df, axis=0)
                merged_df.insert(0, 'Period', dt.datetime.today().replace(day=1).date().strftime('%Y-%m-%d')) # Add 'period' column
                merged_df.to_excel(f'recruitment_profile_{first_day_str}___{last_day_str}.xlsx', index=False)
                #merged_df.to_sql(con=my_conn, name='recruitment_profile', if_exists='replace', index=False)
                print("="*40)
                print("Finished process")
            except ValueError:
                print("No subject ids for the day!")
                print("="*40) 
    else:
        print("Your input is not valid!")
    # ------------------------------------------------------------
    
    # End timer
    end = time.time()
    print("-"*40)
    print(f"Program run successfully. It took {round((end - start)/60, 2)} minutes to run.")


if __name__ =="__main__":
    main()