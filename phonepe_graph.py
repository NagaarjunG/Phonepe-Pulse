import plotly.express as px
import requests







class p_graph: 
    def Transaction_amount_count_By_State(self,aggregated_transaction, state):
        # Filter the aggregated_user DataFrame for the specified year
        BUY = aggregated_transaction[aggregated_transaction["States"] == state]
        BUY.reset_index(drop=True, inplace=True)

        # Create static treemap chart
        fig = px.treemap(BUY, 
                        path=["Years", "Quarter", "Transaction_type"], 
                        values="Transaction_amount",
                        color="Transaction_type",
                        color_discrete_sequence=px.colors.qualitative.Bold,
                        title=f"Perfomences of Transaction Sector {state}",
                        hover_name="Transaction_type",  # Set the hover name to "Brands"
                        hover_data={"Quarter": True, "Transaction_count": True, "Transaction_amount": True},
                        #labels={"User_count": "User Count"},
                        height=800)

        return fig



    def Transaction_details_by_three(self,aggregated_transaction, state, year, qtr):
        
       
            TCTAY = aggregated_transaction[(aggregated_transaction["States"] == state) & 
                                        (aggregated_transaction["Years"] == year) & 
                                        (aggregated_transaction["Quarter"] == qtr)].reset_index(drop=True)
            # Create pie chart
            fig = px.pie(TCTAY, values="Transaction_amount", 
                        names="Transaction_type", 
                        title=f"TRANSACTION AMOUNT AND TRANSACTION COUNT - {state} For the Year {year}",
                        color="Transaction_type",
                        color_discrete_sequence=px.colors.qualitative.Bold,
                        height=800,
                        hole=0.5)

            return fig
    
        


    def Brand_usage_count_by_year(self,aggregated_user, year):
        # Filter the aggregated_user DataFrame for the specified year
        BUY = aggregated_user[aggregated_user["Years"] == year]
        BUY.reset_index(drop=True, inplace=True)

        # Create static treemap chart
        fig = px.treemap(BUY, 
                        path=["States", "Quarter", "Brands"], 
                        values="User_count",
                        color="States",
                        color_discrete_sequence=px.colors.qualitative.Bold,
                        title=f"Smartphone Brand Usage in {year}",
                        hover_name="Brands",  # Set the hover name to "Brands"
                        hover_data={"Quarter": True, "User_count": True, "Percentage": ":.2%"},
                        #labels={"User_count": "User Count"},
                        height=800)

        return fig



    def Brand_usage_details_by_three(self,aggregated_user, state, year, qtr):

        AUSYQ = aggregated_user[(aggregated_user["States"] == state) & 
                                        (aggregated_user["Years"] == year) & 
                                        (aggregated_user["Quarter"] == qtr)]
        
        AUSYQ.reset_index(drop=True, inplace=True)

        # Create pie chart
        fig = px.pie(AUSYQ, values="User_count", 
                    names="Brands", 
                    title=f"Smartphone Brand Usage in {state}",
                    # hover_data={"User_count": True, "Percentage": ":.2%"},  # Include user count and percentage in hover
                    color="Brands",
                    color_discrete_sequence=px.colors.qualitative.Bold,
                    height=800,
                    hole=0.5)
            
        return fig
    



    def map_transaction_user(self,map_tran_user):
        # Filter the merged DataFrame for the specified year
        map_tran_user_df = map_tran_user
        # map_tran_user_df = map_tran_user_df.groupby(["State"]).agg({
        #     "Total_Transactions": "sum",
        #     "Transaction_Amount": "sum",
        #     "User_Count": "sum",
        #     "Total_Used_Apps": "sum"
        # }).reset_index()

        # URL to fetch the GeoJSON data for Indian states
        geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        # Fetch the GeoJSON data for Indian states
        response = requests.get(geojson_url)
        geojson_data = response.json()
        # Create choropleth map using Plotly Express
        map_tran_user_graph = px.choropleth(
            map_tran_user_df,
            geojson=geojson_data,
            locations="State",
            featureidkey="properties.ST_NM",
            color="State",
            hover_name="State",
            hover_data={
                "Transaction_Amount": True,
                "Total_Transactions": ":,",
                "User_Count": ":,",
                "Total_Used_Apps": ":,"
            },
            title=f"Overall Users and Revenue Report ",
            color_continuous_scale="Viridis",
            range_color=(0, map_tran_user_df["User_Count"].max()),  # Set the color range based on Transaction_count
            width=800,
            height=600
        )

        map_tran_user_graph.update_geos(visible=False)
        return map_tran_user_graph


        
            
    




    def top_transaction_by_year(self,top_transaction, state,year):
        # Filter top_transaction DataFrame for the specified year and state
        filtered_users = top_transaction.copy()
        
        
        filtered_users = filtered_users[filtered_users["States"] == state]


        filtered_users = filtered_users[filtered_users["Years"] == year]
        
        # Aggregate transaction metrics per quarter and pincode for the selected state
        state_quarter_pincode_summary = filtered_users.groupby(["Quarter", "Pincodes"]).agg({
            "Transaction_count": "sum",
            "Transaction_amount": "sum"
        }).reset_index()

        # Construct title based on selected parameters
        title_parts = []
        if state:
            title_parts.append(f"for {state}")
        if year:
            title_parts.append(f"in {year}")    
        
        title = "Transaction Metrics Across Quarters and Pincodes"
        if title_parts:
            title += " " + ", ".join(title_parts)

        # Create a sunburst chart to visualize transaction metrics across quarters and pincodes
        fig = px.sunburst(state_quarter_pincode_summary, 
                        path=["Quarter", "Pincodes"], 
                        values="Transaction_amount",
                        title=title,
                        template="plotly_white",
                        color="Pincodes",  # Color segments based on Pincodes
                        hover_data={"Transaction_count": True, "Transaction_amount": ":,.2f"},
                        )

        # Update layout and axis labels
        fig.update_layout(
            font=dict(family="Arial", size=12, color="black"),
            plot_bgcolor='White',  # Set background color
            hoverlabel=dict(font_size=12, font_family="Arial"),
            
        )


        return fig



    def top_users_by_three(self,top_users, state, selected_year):
        # Filter top_users DataFrame based on optional parameters
        
        filtered_users = top_users[top_users["States"] == state]    


        filtered_users = filtered_users[filtered_users["Years"] == selected_year]

        

        # Construct title based on selected parameters
        title_parts = []
        if state:
            title_parts.append(f"for {state}")
        if selected_year:
            title_parts.append(f"in {selected_year}")    
        
        title = "Registered Users"
        if title_parts:
            title += " " + ", ".join(title_parts)

        # Create grouped bar chart
        fig = px.bar(filtered_users,
                    x="Quarter",
                    y="RegisteredUsers",
                    color="Pincodes",
                    title=title,
                    labels={"Quarter": "Quarter", "RegisteredUsers": "Registered Users"},
                    barmode="group",
                    hover_data=["Pincodes"],
                    #category_orders={"Quarter": sorted(filtered_users["Quarter"].unique())}
                    )

        # Update layout and axis labels
        fig.update_layout(
            xaxis_title="Quarter",
            yaxis_title="Registered Users",
            font=dict(family="Arial", size=12, color="black"),
            plot_bgcolor='white',
            hovermode="x",
            hoverlabel=dict(font_size=12, font_family="Arial"),
            autosize=True
        )

        return fig

   


    




