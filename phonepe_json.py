import os
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer,Float,Numeric,Double,BIGINT,VARCHAR
from sqlalchemy.exc import IntegrityError
import mysql.connector
import plotly.express as px


class json_file_df:
    def df_source_files_agg_transaction(self):

        # Define the base path where the JSON files are located
        pathAT1 = "C:/Users/Nagaarjun.G/Desktop/Phonepe/pulse/data/aggregated/transaction/country/india/state/"

        # List all directories (states) in the specified path
        aggr_tran_list = os.listdir(pathAT1)

        # Define a dictionary to store extracted data
        at_data = {
            "States": [],
            "Years": [],
            "Quarter": [],
            "Transaction_type": [],
            "Transaction_count": [],
            "Transaction_amount": []
        }

        # Loop through each state directory
        for state in aggr_tran_list:
            # Form the path for the current state directory
            present_state = os.path.join(pathAT1, state)
            
            # List all directories (years) within the current state directory
            aggr_year_list = os.listdir(present_state)

            # Loop through each year directory within the current state directory
            for year in aggr_year_list:
                # Form the path for the current year directory
                present_year = os.path.join(present_state, year)
                
                # List all JSON files within the current year directory
                aggr_file_list = os.listdir(present_year)

                # Loop through each JSON file in the current year directory
                for file in aggr_file_list:
                    # Form the path for the current JSON file
                    present_file = os.path.join(present_year, file)

                    # Open and read JSON data from the file
                    with open(present_file, "r") as f:
                        try:
                            # Attempt to parse JSON data from the file
                            AT_json_data = json.load(f)

                            # Extract necessary data from the parsed JSON
                            for i in AT_json_data["data"]["transactionData"]:
                                name = i["name"]
                                count = i["paymentInstruments"][0]["count"]
                                amount = i["paymentInstruments"][0]["amount"]


                                at_data["Transaction_type"].append(name)
                                at_data["Transaction_count"].append(count)
                                at_data["Transaction_amount"].append(amount)
                                at_data["States"].append(state)
                                at_data["Years"].append(int(year))
                                at_data["Quarter"].append(int(file.strip(".json")))
                                
                            

                        except json.JSONDecodeError as e:
                            # Handle JSON decoding errors
                            print(f"Error decoding JSON in file '{present_file}': {e}")

                        except (KeyError, IndexError) as e:
                            # Handle key errors or index errors in accessing JSON data
                            print(f"Error accessing required data in file '{present_file}': {e}")

                        except ValueError as e:
                            # Handle value errors (e.g., conversion to integer)
                            print(f"Value error in file '{present_file}': {e}")

        if at_data:
            return at_data
        else:
            return None    


        

        # Change the States Names and Set Title # columns1
        

    def df_source_files_agg_user(self):

        # Define the base path where the JSON files are located
        pathAU2 = "C:/Users/Nagaarjun.G/Desktop/Phonepe/pulse/data/aggregated/user/country/india/state/"

        # List all directories (states) in the specified path
        aggr_user_list = os.listdir(pathAU2)

        # Define a dictionary to store extracted data
        au_data = {
            "States": [],
            "Years": [],
            "Quarter": [],
            "Brands": [],
            "User_count": [],
            "Percentage": []
        }

        # Loop through each state directory
        for state in aggr_user_list:
            # Form the path for the current state directory
            present_state = os.path.join(pathAU2, state)
            
            # List all directories (years) within the current state directory
            aggr_year_list = os.listdir(present_state)

            # Loop through each year directory within the current state directory
            for year in aggr_year_list:
                # Form the path for the current year directory
                present_year = os.path.join(present_state, year)
                
                # List all JSON files within the current year directory
                aggr_file_list = os.listdir(present_year)

                # Loop through each JSON file in the current year directory
                for file in aggr_file_list:
                    # Form the path for the current JSON file
                    present_file = os.path.join(present_year, file)

                    # Open and read JSON data from the file
                    with open(present_file, "r") as f:
                        try:
                            # Attempt to parse JSON data from the file
                            AU_json_data = json.load(f)

                            # Extract necessary data from the parsed JSON

                            try:
                                for i in AU_json_data["data"]["usersByDevice"]:
                                    brand = i["brand"]
                                    count = i["count"]
                                    percentage = i["percentage"]

                                    au_data["Brands"].append(brand)
                                    au_data["User_count"].append(count)
                                    au_data["Percentage"].append(percentage)
                                    au_data["States"].append(state)
                                    au_data["Years"].append(int(year))
                                    au_data["Quarter"].append(int(file.strip(".json")))
                                    
       
                                    
                            except:
                                pass      

                        except json.JSONDecodeError as e:
                            # Handle JSON decoding errors
                            print(f"Error decoding JSON in file '{present_file}': {e}")

                        except (KeyError, IndexError) as e:
                            # Handle key errors or index errors in accessing JSON data
                            print(f"Error accessing required data in file '{present_file}': {e}")

                        except ValueError as e:
                            # Handle value errors (e.g., conversion to integer)
                            print(f"Value error in file '{present_file}': {e}")

        if au_data:
            return au_data
        else:
            return None            




    def df_source_files_map_transaction(self):
        # Define the base path where the JSON files are located
        pathMT1 = "C:/Users/Nagaarjun.G/Desktop/Phonepe/pulse/data/map/transaction/hover/country/india/state/"

        # List all directories (states) in the specified path
        map_tran_list = os.listdir(pathMT1)

        # Define a dictionary to store extracted data
        mt_data = {
            "States": [],
            "Years": [],
            "Quarter": [],
            "Districts": [],
            "Transaction_count": [],
            "Transaction_amount": []
        }

        # Loop through each state directory
        for state in map_tran_list:
            # Form the path for the current state directory
            present_state = os.path.join(pathMT1, state)
            
            # List all directories (years) within the current state directory
            aggr_year_list = os.listdir(present_state)

            # Loop through each year directory within the current state directory
            for year in aggr_year_list:
                # Form the path for the current year directory
                present_year = os.path.join(present_state, year)
                
                # List all JSON files within the current year directory
                aggr_file_list = os.listdir(present_year)

                # Loop through each JSON file in the current year directory
                for file in aggr_file_list:
                    # Form the path for the current JSON file
                    present_file = os.path.join(present_year, file)

                    try:
                        # Open and read JSON data from the file
                        with open(present_file, "r") as f:
                            # Attempt to parse JSON data from the file
                            MT_json_data = json.load(f)
                            
                            # Extract relevant data from JSON structure
                            for data in MT_json_data["data"]["hoverDataList"]:
                                name = data["name"]
                                count = data["metric"][0]["count"]
                                amount = data["metric"][0]["amount"]  # Assuming this is correct
                               
                                
                                # Append extracted data to the columns dictionary
                                mt_data["Districts"].append(name)
                                mt_data["Transaction_count"].append(count)
                                mt_data["Transaction_amount"].append(amount)
                                mt_data["States"].append(state)
                                mt_data["Years"].append(int(year))                                
                                mt_data["Quarter"].append(int(file.strip(".json")))
                                
                           
                    except json.JSONDecodeError as e:
                            # Handle JSON decoding errors
                            print(f"Error decoding JSON in file '{present_file}': {e}")

                    except (KeyError, IndexError) as e:
                            # Handle key errors or index errors in accessing JSON data
                            print(f"Error accessing required data in file '{present_file}': {e}")

                    except ValueError as e:
                            # Handle value errors (e.g., conversion to integer)
                            print(f"Value error in file '{present_file}': {e}")

        if mt_data:
            return mt_data
        else:
            return None
                                

                        


    def df_source_files_map_user(self):


        # Define the base path where the JSON files are located
        pathMU2 = "C:/Users/Nagaarjun.G/Desktop/Phonepe/pulse/data/map/user/hover/country/india/state/"

        # List all directories (states) in the specified path
        map_user_list = os.listdir(pathMU2)

        # Define a dictionary to store extracted data
        mu_data = {
            "States": [],
            "Years": [],
            "Quarter": [],
            "Districts": [],
            "RegisteredUsers": [],
            "AppOpens": []
        }

        # Loop through each state directory
        for state in map_user_list:
            present_state = os.path.join(pathMU2, state)
            
            # List all directories (years) within the current state directory
            aggr_year_list = os.listdir(present_state)

            # Loop through each year directory within the current state directory
            for year in aggr_year_list:
                present_year = os.path.join(present_state, year)
                
                # List all JSON files within the current year directory
                aggr_file_list = os.listdir(present_year)

                # Loop through each JSON file in the current year directory
                for file in aggr_file_list:
                    present_file = os.path.join(present_year, file)

                    try:
                        # Open and read JSON data from the file
                        with open(present_file, "r") as f:
                            MU_json_data = json.load(f)

                            # Assuming 'MU_json_data' contains your JSON data
                            for district, data in MU_json_data["data"]["hoverData"].items():
                                registered_users = data.get("registeredUsers")
                                app_opens = data.get("appOpens")

                                # Append extracted data to the mu_data dictionary
                                mu_data["Districts"].append(district)
                                mu_data["RegisteredUsers"].append(registered_users)
                                mu_data["AppOpens"].append(app_opens)
                                mu_data["States"].append(state)
                                mu_data["Years"].append(int(year))
                                
                                # Extract quarter from filename assuming file name is in format "1.json"
                                quarter = int(os.path.splitext(file)[0])
                                mu_data["Quarter"].append(quarter)
                                
                            
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in file '{present_file}': {e}")

                    except (KeyError, IndexError) as e:
                        print(f"Error accessing required data in file '{present_file}': {e}")

                    except ValueError as e:
                        print(f"Value error in file '{present_file}': {e}")

        if mu_data:
            return mu_data
        else:
            return None


    
    def df_source_files_top_transaction(self):
        # Define the base path where the JSON files are located
        pathTT1 = "C:/Users/Nagaarjun.G/Desktop/Phonepe/pulse/data/top/transaction/country/india/state/"

        # List all directories (states) in the specified path
        top_trans_list = os.listdir(pathTT1)

        # Define a dictionary to store extracted data
        tt_data = {
            "States": [],
            "Years": [],
            "Quarter": [],
            "Pincodes": [],
            "Transaction_count": [],
            "Transaction_amount": []

        }

        # Loop through each state directory
        for state in top_trans_list:
            # Form the path for the current state directory
            present_state = os.path.join(pathTT1, state)
            
            # List all directories (years) within the current state directory
            aggr_year_list = os.listdir(present_state)

            # Loop through each year directory within the current state directory
            for year in aggr_year_list:
                # Form the path for the current year directory
                present_year = os.path.join(present_state, year)
                
                # List all JSON files within the current year directory
                aggr_file_list = os.listdir(present_year)

                # Loop through each JSON file in the current year directory
                for file in aggr_file_list:
                    # Form the path for the current JSON file
                    present_file = os.path.join(present_year, file)

                    # Open and read JSON data from the file
                    with open(present_file, "r") as f:
                        try:
                            # Attempt to parse JSON data from the file
                            TT_json_data = json.load(f)
                            for i in TT_json_data["data"]["pincodes"]:
                                entityname = i["entityName"]
                                count = i["metric"]["count"]
                                amount = i["metric"]["amount"]

                        
                                tt_data["Pincodes"].append(entityname)
                                tt_data["Transaction_count"].append(count)
                                tt_data["Transaction_amount"].append(amount)
                                tt_data["States"].append(state)
                                tt_data["Years"].append(int(year))                                
                                tt_data["Quarter"].append(int(file.strip(".json")))
                                
                            
                        except json.JSONDecodeError as e:
                            # Handle JSON decoding errors
                            print(f"Error decoding JSON in file '{present_file}': {e}")

                        except (KeyError, IndexError) as e:
                            # Handle key errors or index errors in accessing JSON data
                            print(f"Error accessing required data in file '{present_file}': {e}")

                        except ValueError as e:
                            # Handle value errors (e.g., conversion to integer)
                            print(f"Value error in file '{present_file}': {e}")

            
        if tt_data:
            return tt_data
        else:
            return None



    def df_source_files_top_user(self):

        # Define the base path where the JSON files are located
        pathTU2 = "C:/Users/Nagaarjun.G/Desktop/Phonepe/pulse/data/top/user/country/india/state/"

        # List all directories (states) in the specified path
        top_user_list = os.listdir(pathTU2)

        # Define a dictionary to store extracted data
        tu_data = {
            "States": [],
            "Years": [],
            "Quarter": [],
            "Pincodes": [],
            "RegisteredUsers": []
        }

        # Loop through each state directory
        for state in top_user_list:
            # Form the path for the current state directory
            present_state = os.path.join(pathTU2, state)
            
            # List all directories (years) within the current state directory
            aggr_year_list = os.listdir(present_state)

            # Loop through each year directory within the current state directory
            for year in aggr_year_list:
                # Form the path for the current year directory
                present_year = os.path.join(present_state, year)
                
                # List all JSON files within the current year directory
                aggr_file_list = os.listdir(present_year)

                # Loop through each JSON file in the current year directory
                for file in aggr_file_list:
                    # Form the path for the current JSON file
                    present_file = os.path.join(present_year, file)

                    # Open and read JSON data from the file
                    with open(present_file, "r") as f:
                        try:
                            # Attempt to parse JSON data from the file
                            TU_json_data = json.load(f)
                            for i in TU_json_data["data"]["pincodes"]:
                                entityname = i["name"]
                                registeredUsers = i["registeredUsers"]
                             
                                tu_data["Pincodes"].append(entityname)
                                tu_data["RegisteredUsers"].append(registeredUsers)
                                tu_data["States"].append(state)
                                tu_data["Years"].append(int(year))                                
                                tu_data["Quarter"].append(int(file.strip(".json")))
                                
                            
                        except json.JSONDecodeError as e:
                            # Handle JSON decoding errors
                            print(f"Error decoding JSON in file '{present_file}': {e}")

                        except (KeyError, IndexError) as e:
                            # Handle key errors or index errors in accessing JSON data
                            print(f"Error accessing required data in file '{present_file}': {e}")

                        except ValueError as e:
                            # Handle value errors (e.g., conversion to integer)
                            print(f"Value error in file '{present_file}': {e}")

        if tu_data:
            return tu_data
        else:
            return None





