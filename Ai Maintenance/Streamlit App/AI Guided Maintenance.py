#   _____ _                            _ _ _                           _ 
#  / ____| |                          | (_) |       /\               | |
# | (___ | |_ _ __ ___  __ _ _ __ ___ | |_| |_     /  \   _ __  _ __ | |
#  \___ \| __| '__/ _ \/ _` | '_ ` _ \| | | __|   / /\ \ | '_ \| '_ \| |
#  ____) | |_| | |  __/ (_| | | | | | | | | |_   / ____ \| |_) | |_) |_|
# |_____/ \__|_|  \___|\__,_|_| |_| |_|_|_|\__| /_/    \_\ .__/| .__/(_)
#                                                        | |   | |      
#  

# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("AI-Guided Maintenance")
st.image('https://s3.amazonaws.com/assets.ottomotors.com/vehicles/product-card-OTTO_100.png', caption='')

# Get the current credentials
session = get_active_session()


question = st.text_input('Question', 'OTTO 1500 agv is not driving straight.  How do I troubleshoot and resolve this issue?')

if st.button(":snowflake: Submit", type="primary"):
#Create Tabs
	tab1, tab2, tab3 = st.tabs(["1 - Repair Manuals (Only)","2 - Internal Repair Logs (Only)","3 - Combined Insights"])

	with tab1:

	    # Review Manuals and provide response/recommendation
	    manuals_query = f"""
	    SELECT * FROM TABLE(REPAIR_MANUALS_LLM('{question}'));
	    """
	    
	    manuals_response = session.sql(manuals_query).collect()

	    st.subheader('Recommended actions from review of maintenance manuals:')

	    st.write(manuals_response[0].FILE_NAME)
	    st.write(manuals_response[0].RESPONSE)

	    st.subheader('Repair manual "chunks" and their relative scores:')    
	    st.write(manuals_response)

	with tab2:
	    
	    logs_query = f"""
	    SELECT * FROM TABLE(REPAIR_LOGS_LLM('{question}'));
	    """

	    logs_response = session.sql(logs_query).collect()
	    
	    st.subheader('Recommended actions from review of repair logs:')
	    st.write(logs_response[0].RESPONSE)

	    st.subheader('Insights gathered from these most relevant repair logs:')
	    st.write(logs_response[0].RELEVANT_REPAIR_LOGS)


	with tab3:
	    
	    combined_query = f"""
	    SELECT * FROM TABLE(COMBINED_REPAIR_LLM('{question}'));
	    """

	    combined_response = session.sql(combined_query).collect()
	    
	    st.subheader('Combined Recommendations:')
	    st.write(logs_response[0].RESPONSE)


