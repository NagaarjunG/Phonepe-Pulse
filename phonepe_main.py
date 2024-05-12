import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer,Float,Numeric,Double,BIGINT,VARCHAR
from sqlalchemy.exc import IntegrityError
import mysql.connector
import plotly.express as px
from phonepe_json import json_file_df
from phonepe_graph import p_graph
from database import DB_Mgmt
import requests
import json

db_file = DB_Mgmt()
db_file.create_database()
db_file.table_creation()

json_file = json_file_df()
graph_file= p_graph()
aggregated_transacation=pd.DataFrame(json_file.df_source_files_agg_transaction())
aggregated_user=pd.DataFrame(json_file.df_source_files_agg_user())
map_transaction=pd.DataFrame(json_file.df_source_files_map_transaction())
map_user=pd.DataFrame(json_file.df_source_files_map_user())
top_transaction=pd.DataFrame(json_file.df_source_files_top_transaction())
top_user=pd.DataFrame(json_file.df_source_files_top_user())

def clean_state_names(df):
    df["States"] = df["States"].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
    df["States"] = df["States"].str.replace("-", " ")
    df["States"] = df["States"].str.title()
    df["States"] = df["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')

clean_state_names(aggregated_transacation)
clean_state_names(aggregated_user)
clean_state_names(map_transaction)
clean_state_names(map_user)
clean_state_names(top_transaction)
clean_state_names(top_user)

table_name="agg_transactions"
db_file.df_to_sql(aggregated_transacation,table_name)

table_name="agg_users"
db_file.df_to_sql(aggregated_user,table_name)

table_name="map_transactions"
db_file.df_to_sql(map_transaction,table_name)

table_name="map_users"
db_file.df_to_sql(map_user,table_name)

table_name="top_transactions"
db_file.df_to_sql(top_transaction,table_name)

table_name="top_users"
db_file.df_to_sql(top_user,table_name)





agg_transaction_query = "select States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount from agg_transactions;"
query_result = db_file.Query_Output(agg_transaction_query)
if query_result:
    agg_transactions_df = pd.DataFrame(query_result, columns=['States', 'Years','Quarter','Transaction_type','Transaction_count','Transaction_amount'])



agg_users_query = "select States, Years, Quarter, Brands, User_count, Percentage from agg_users;"
query_result = db_file.Query_Output(agg_users_query)
if query_result:
    agg_users_df = pd.DataFrame(query_result, columns=['States', 'Years','Quarter','Brands','User_count','Percentage'])


agg_users_query = "select States, Years, Quarter, Brands, User_count, Percentage from agg_users;"
query_result = db_file.Query_Output(agg_users_query)
if query_result:
    agg_users_df = pd.DataFrame(query_result, columns=['States', 'Years','Quarter','Brands','User_count','Percentage'])


map_transaction_query = "select a.States as States, sum(RegisteredUsers) as User_count,sum(AppOpens) as Total_Used_Apps,Sum(Transaction_count) as Total_Transactions,Sum(Transaction_Amount) as Transaction_Amount from map_users a,map_transactions b where a.States=b.States and a.Years=b.Years and a.Quarter=b.Quarter and a.Districts=b.Districts group by a.States;"
map_transaction_query_result = db_file.Query_Output(map_transaction_query)
if map_transaction_query_result:
    # map_transactions_df = pd.DataFrame(map_transaction_query_result, columns=['State', 'Year','Quarter','District','User_Count','Total_Used_Apps','Total_Transactions','Transaction_Amount'])
    map_transactions_df = pd.DataFrame(map_transaction_query_result, columns=['State', 'User_Count', 'Total_Used_Apps','Total_Transactions', 'Transaction_Amount'])








top_transaction_query = "select States, Years, Quarter,Pincodes,Transaction_count,Transaction_amount  from top_transactions;"
query_result = db_file.Query_Output(top_transaction_query)
if query_result:
    top_transaction_df = pd.DataFrame(query_result, columns=['States', 'Years','Quarter','Pincodes','Transaction_count','Transaction_amount'])


top_user_query = "select States, Years, Quarter,Pincodes,RegisteredUsers from top_users;"
query_result = db_file.Query_Output(top_user_query)
if query_result:
    top_user_df = pd.DataFrame(query_result, columns=['States', 'Years','Quarter','Pincodes','RegisteredUsers'])




custom_css = """
<style>
body {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; /* Use a clean sans-serif font */
    background-color: #f5f5f5; /* Set a light gray background color */
    color: #333; /* Set text color to dark gray */
    margin: 0; /* Reset margin */
    padding: 0; /* Reset padding */
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100vh; /* Full viewport height */
}

.container {
    text-align: center;
    padding: 50px;
    background-color: #ffffff; /* White background */
    border-radius: 10px;
    box-shadow: 0px 5px 15px rgba(0, 0, 0, 0.1); /* Soft box shadow */
    max-width: 80%; /* Limit container width */
}

.header {
    font-size: 48px;
    margin-bottom: 20px;
    color: #6f42c1; /* Purple color for header */
}

.subheader {
    font-size: 24px;
    color: #888; /* Light gray color for subheader */
    margin-bottom: 30px;
}

.btn {
    display: inline-block;
    padding: 12px 24px;
    font-size: 18px;
    font-weight: bold;
    color: #ffffff;
    background-color: #6f42c1; /* Purple color for button */
    border: none;
    border-radius: 5px;
    cursor: pointer;
    text-decoration: none;
    transition: background-color 0.3s ease;
}

.btn:hover {
    background-color: #8248c9; /* Darker purple on hover */
}
</style>
"""

# Render the custom CSS within the Streamlit app
st.markdown(custom_css, unsafe_allow_html=True)

# Streamlit content using the defined styles

st.markdown("<h1 class='header'>Welcome to PhonePe Data Visualization & Exploration</h1>", unsafe_allow_html=True)
st.markdown("<p class='subheader'>Unlock the power of your data!</p>", unsafe_allow_html=True)
st.markdown("<a class='btn' href='#'>Get Started</a>", unsafe_allow_html=True)  # Insert the button here
st.markdown("</div>", unsafe_allow_html=True)


# Render homepage content within a styled container
with st.container():
    st.subheader("Explore insights and trends from PhonePe transactions")

    st.markdown("---")  # Horizontal rule

    st.write("This app allows you to analyze aggregated data, map visualizations, and top charts from PhonePe.")

    st.markdown("---")  # Horizontal rule

    # Button to explore data
    if st.button("Explore Data", key="explore_button"):
        st.success("Let's get started with data exploration!")



# Sidebar menu for navigation and options
with st.sidebar:
    st.title("Navigation Menu")

    # Selection box for main options
    select_option = st.selectbox("Select Option", ["HOME", "DATA EXPLORE", "TOP CHARTS"])
    # Sidebar title with emoji
st.sidebar.title("Help & Support 🆘")

# Button to read the documentation
if st.sidebar.button("Read the Documentation"):
    st.sidebar.write("Redirecting to documentation...")
    # Add redirection or navigation to documentation URL
    st.sidebar.markdown("<script>window.open('https://docs.example.com', '_blank')</script>", unsafe_allow_html=True)

# Button to email for support
if st.sidebar.button("Email for Support"):
    st.sidebar.write("Opening email client...")
    # Add functionality to open default email client with pre-filled email
    st.sidebar.markdown("<script>window.open('mailto:support@example.com', '_blank')</script>", unsafe_allow_html=True)

# Styled link for documentation
st.sidebar.markdown("<a href='https://docs.example.com' style='color: blue; text-decoration: underline;' target='_blank'>Read the Documentation</a>", unsafe_allow_html=True)

# Styled link for email support
st.sidebar.markdown("<a href='mailto:support@example.com' style='color: white; text-decoration: none;' target='_blank'>Email for Support</a>", unsafe_allow_html=True)

  # Initialize database management object (replace DB_Mgmt() with your database management class)
db_file = DB_Mgmt()  # Replace DB_Mgmt() with your class for database management
if select_option == "HOME":
        # Display key insights with color and emojis
    st.markdown("### Key Insights 💡📊")
    st.write("- Total number of transactions processed.")
    st.write("- Top states with the highest transaction amounts.")
    st.write("- Most frequent transaction types.")
    st.write("- Distribution of user demographics.")

    st.markdown("---")  # Horizontal rule

    # Update the message with color and emojis
    st.markdown("### Why Explore Your Data? 🕵️‍♂️🔍")
    st.write("Analyzing your data uncovers hidden patterns and actionable insights. 📈✨")

    # Additional styling for each insight and message
    st.markdown("---")  # Horizontal rule

    # Total number of transactions processed
    st.markdown("### Total Number of Transactions Processed 🔄")
    st.write("Understanding the overall volume of transactions provides insights into business activity.")

    # Top states with the highest transaction amounts
    st.markdown("### Top States with Highest Transaction Amounts 🌟📊")
    st.write("Identifying which states generate the most revenue can inform targeted marketing strategies.")

    # Most frequent transaction types
    st.markdown("### Most Frequent Transaction Types 🔄📊")
    st.write("Recognizing popular transaction types helps optimize inventory and service offerings.")

    # Distribution of user demographics
    st.markdown("### Distribution of User Demographics 🧑‍🤝‍🧑🌎")
    st.write("Understanding your user base demographics aids in personalized marketing and product development.")



if select_option == "TOP CHARTS":
  

    # Streamlit content and logic
    st.header("Explore Top Charts")

    # Define SQL queries corresponding to user choices
    sql_queries = {
        "Q1: Which states have the highest total transaction amounts?": '''
            SELECT states, SUM(transaction_amount) AS transaction_amount
            FROM agg_transactions
            GROUP BY states
            ORDER BY transaction_amount DESC
            LIMIT 10;
        ''',
        "Q2: Which states have the lowest total transaction amounts?": '''
            SELECT states, SUM(transaction_amount) AS transaction_amount
            FROM agg_transactions
            GROUP BY states
            ORDER BY transaction_amount ASC
            LIMIT 10;
        ''',
        "Q3: Which states have the highest total number of users?": '''
            SELECT States, SUM(User_count) AS Total_User_Count
            FROM agg_users
            GROUP BY States
            ORDER BY Total_User_Count DESC
            LIMIT 10;
        ''',
        "Q4: Which states have the lowest total number of users?": '''
            SELECT States, SUM(User_count) AS Total_User_Count
            FROM agg_users
            GROUP BY States
            ORDER BY Total_User_Count ASC
            LIMIT 10;
        ''',
        "Q5: What are the top 10 transactions with the highest transaction amounts?": '''
            SELECT Pincodes, SUM(Transaction_Amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count
            FROM top_transactions
            GROUP BY Pincodes
            ORDER BY Transaction_Amount DESC
            LIMIT 10;
        ''',
        "Q6: What are the top 10 transactions with the lowest transaction amounts?": '''
            SELECT Pincodes, SUM(Transaction_Amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count
            FROM top_transactions
            GROUP BY Pincodes
            ORDER BY Transaction_Amount ASC
            LIMIT 10;
        ''',
        "Q7: Which states have the highest number of registered users?": '''
            SELECT States, SUM(RegisteredUsers) AS RegisteredUsers
            FROM top_users
            GROUP BY States
            ORDER BY RegisteredUsers DESC
            LIMIT 10;
        ''',
        "Q8: Which states have the lowest number of registered users?": '''
            SELECT States, SUM(RegisteredUsers) AS RegisteredUsers
            FROM top_users
            GROUP BY States
            ORDER BY RegisteredUsers ASC
            LIMIT 10;
        ''',
        "Q9: Which transaction types have the highest total transaction amounts?": '''
            SELECT Transaction_type, SUM(Transaction_amount) AS Total_Transaction_Amount
            FROM agg_transactions
            GROUP BY Transaction_type
            ORDER BY Total_Transaction_Amount DESC
            LIMIT 10;
        '''
    }

    # Display dropdown menu to select the query
    query_choice = st.selectbox(
        "Select a query:",
        list(sql_queries.keys())
    )

    # Display the selected query above the "Execute Query" button
    st.write(f"You selected: {query_choice}")

    # Execute selected SQL query when the user clicks the button
    if st.button("Execute Query"):
        if query_choice in sql_queries:
            sql_query = sql_queries[query_choice]

            # Execute SQL query to fetch data
            query_result = db_file.Query_Output(sql_query)

            # If query result is obtained, create a DataFrame from the result
            if query_result:
                result_df = pd.DataFrame(query_result)

                # Determine the appropriate chart type based on the query choice
                if "transaction_amount" in result_df.columns:
                    # Create a bar chart for transaction amounts
                    fig = px.bar(
                        result_df,
                        x="states",
                        y="transaction_amount",
                        title="Top States by Transaction Amounts",
                        color="states",
                        height=650,
                        width=800
                    )
                elif "Total_User_Count" in result_df.columns:
                    # Check if Total_User_Count is numeric
                        # Create a scatter plot for total user counts with correct marker sizes
                        fig = px.area(
                        result_df,
                        x="States",
                        y="Total_User_Count",
                        title="Top States by Total User Count",
                        height=650,
                        width=800
                        )
                elif "Transaction_amount" in result_df.columns and "Transaction_count" in result_df.columns:
                    # Create a line chart for top transactions by amount
                    result_df["Pincodes"]=result_df["Pincodes"].astype(str)
                    fig = px.line(
                        result_df,
                        x="Pincodes",
                        y="Transaction_amount",
                        title="Top 10 Transactions by Transaction Amounts",
                        line_shape="linear",
                        height=650,
                        width=800
                    )
                    # Update x-axis type to categorical
                    fig.update_xaxes(type='category')

                elif "RegisteredUsers" in result_df.columns:
                    # Create a pie chart for registered users per state
                    fig = px.pie(
                        result_df,
                        names="States",
                        values="RegisteredUsers",
                        title="Top States by Registered Users",
                        height=650,
                        width=800
                    )
                elif "Total_Transaction_Amount" in result_df.columns:
                    # Create a pie chart for transaction types by total transaction amount
                    fig = px.pie(
                        result_df,
                        names="Transaction_type",
                        values="Total_Transaction_Amount",
                        title="Top Transaction Types by Total Transaction Amount",
                        height=650,
                        width=800
                    )
                    # Display the Plotly chart in Streamlit
                st.plotly_chart(fig)

            else:
                st.warning("No data available for the selected query.")

        else:
            st.warning("Invalid query selection. Please select a valid query.")









# Display content based on user selection
if select_option == "DATA EXPLORE":
    st.header("Data Exploration")

    # Data exploration category selection
    explore_category = st.radio("Select Exploration Category", ["Aggregated", "Map", "Top"])
    

    if explore_category == "Aggregated":
        st.subheader("Aggregated Data Exploration")
        explore_type = st.radio("Select Explore Type", ["Transaction", "User"])

    
        if explore_type == "Transaction":
            st.write("Explore Aggregated Transaction Data")

             
            selected_state = st.selectbox("Select State", agg_transactions_df["States"].unique())
            # Use the selected year to process the data (example function call)
            chart=graph_file.Transaction_amount_count_By_State(agg_transactions_df,selected_state )

            st.plotly_chart(chart)
            # Select state, year, and quarter in the second column
        
            # Allow user to select state, year, and quarter
            selected_state = st.selectbox("Select States", agg_transactions_df["States"].unique())
            selected_year = st.selectbox("Select Years", agg_transactions_df["Years"].unique())
            selected_qtr = st.selectbox("Select Quarters", agg_transactions_df["Quarter"].unique())
                        
            # Use the selected parameters to process the data
            #chart=graph_file.Transaction_details_by_three(agg_transactions_df, state=selected_state, year=selected_year, qtr=selected_qtr)
            chart=graph_file.Transaction_details_by_three(agg_transactions_df, selected_state, selected_year, selected_qtr)

            st.plotly_chart(chart)

        elif explore_type == "User":
            st.write("Explore Aggregated User Data")
            # Create two columns for user data exploration
    

            
            # Display a selectbox to choose the year for user data with a unique key
            selected_year = st.selectbox("Select Years", agg_users_df["Years"].unique())
            # Use the selected year to process the data (example function call)
            chart=graph_file.Brand_usage_count_by_year(agg_users_df, selected_year)
            st.write("Explore Aggregated User Data")

            st.plotly_chart(chart)

            # Allow user to select state, year, and quarter
            selected_state = st.selectbox("Select State", agg_users_df["States"].unique())
            selected_year = st.selectbox("Select Year", agg_users_df["Years"].unique())
            selected_qtr = st.selectbox("Select Quarter", agg_users_df["Quarter"].unique())

            # Call Brand_usage_details_by_three function with selected parameters
            chart=graph_file.Brand_usage_details_by_three(agg_users_df, state=selected_state, year=selected_year, qtr=selected_qtr)

            st.plotly_chart(chart)

    elif explore_category == "Map":
        st.write("Map Data Exploration")
    # Add code for map data exploration here

        data_viz_map_trans = graph_file.map_transaction_user(map_transactions_df)
        st.plotly_chart(data_viz_map_trans)

        


    elif explore_category == "Top":
        st.subheader("Top Data Exploration")
        explore_type = st.radio("Select Explore Type", ["Transaction", "User"])

        if explore_type == "Transaction":
            st.write("Explore Top Transaction Data")

            # Display a selectbox to choose the year for user data with a unique key
            selected_state = st.selectbox("Select State", top_transaction_df["States"].unique())
            selected_year = st.selectbox("Select Year", top_transaction_df["Years"].unique())
            # Call Brand_usage_details_by_three function with selected parameters
            chart=graph_file.top_transaction_by_year(top_transaction_df, selected_state, selected_year)

            st.plotly_chart(chart)


            
        if explore_type == "User":
            st.write("Explore Top User Data")

            # Display a selectbox to choose the year for user data with a unique key
            selected_state = st.selectbox("Select States", top_user_df["States"].unique())
            selected_year = st.selectbox("Select Years", top_user_df["Years"].unique())
            # Call Brand_usage_details_by_three function with selected parameters
            chart=graph_file.top_users_by_three(top_user_df, selected_state, selected_year)

            st.plotly_chart(chart)



        

# ## MAP ##

































































































































            





































































































































            


















































































































































