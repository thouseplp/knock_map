import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


st.set_page_config(
    page_title="Appointment Dashboard",
    initial_sidebar_state="collapsed",
    layout="wide"
)

st.logo("https://i.ibb.co/bbH9pgH/Purelight-Logo.webp")

hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .css-10trblm {padding-top: 0px; padding-bottom: 0px;}
    .css-1d391kg {padding-top: 0px !important;}
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Function to create a Snowflake session
def create_snowflake_session():
    connection_parameters = {
        "account": st.secrets["snowflake"]["account"],
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "role": st.secrets["snowflake"]["role"],
        "warehouse": st.secrets["snowflake"]["warehouse"],
        "database": st.secrets["snowflake"]["database"],
        "schema": st.secrets["snowflake"]["schema"],
    }
    return Session.builder.configs(connection_parameters).create()

# Initialize Snowpark session
session = create_snowflake_session()

# Cache functions to avoid redundant queries
@st.cache_data(show_spinner=False, persist=True)
def get_users(data_version):
    users_query = """
        SELECT DISTINCT FULL_NAME, SALESFORCE_ID
        FROM operational.airtable.vw_users 
        WHERE role_type = 'Closer' AND term_date IS NULL
    """
    return session.sql(users_query).to_pandas()

@st.cache_data(show_spinner=False, persist=True)
def get_market(data_version):
    users_query = """
        SELECT MARKET, MARKET_GROUP, RANK, NOTES
        FROM raw.snowflake.lm_markets 
    """
    return session.sql(users_query).to_pandas()

@st.cache_data(show_spinner=False, persist=True)
def get_profile_pictures(data_version):
    profile_picture_query = """
        SELECT FULL_NAME, PROFILE_PICTURE
        FROM operational.airtable.vw_users
    """
    return session.sql(profile_picture_query).to_pandas()

@st.cache_data(show_spinner=False, persist=True)
def get_appointments(data_version):
    appointments_query = """
        SELECT * FROM raw.snowflake.lm_appointments
    """
    return session.sql(appointments_query).to_pandas()

# Check if data_version exists in session state
if 'data_version' not in st.session_state:
    st.session_state['data_version'] = 0

# Load data with caching and pass data_version as a dependency
df_users = get_users(st.session_state['data_version'])
df_markets = get_market(st.session_state['data_version'])
profile_picture = get_profile_pictures(st.session_state['data_version'])
appointments = get_appointments(st.session_state['data_version'])
unique_markets_df = appointments[['MARKET']].drop_duplicates()


# Merge the dataframes on the full name
merged_df = df_users.merge(
    appointments, left_on='FULL_NAME', right_on='NAME', how='left'
).merge(
    profile_picture, on='FULL_NAME', how='left'
)

# Rename and drop columns as needed
merged_df = merged_df.rename(columns={
    'FULL_NAME': 'FULL_NAME',
    'PROFILE_PICTURE_y': 'PROFILE_PICTURE'
})

if 'CLOSER' in merged_df.columns:
    merged_df = merged_df.drop(columns=['CLOSER'])

# Fill NaN values and ensure correct data types
merged_df['MARKET'] = merged_df['MARKET'].fillna('No Market').astype(str)
merged_df['GOAL'] = merged_df['GOAL'].fillna(0).astype(int)
merged_df['RANK'] = merged_df['RANK'].fillna(100).astype(int)
merged_df['FM_GOAL'] = merged_df['FM_GOAL'].fillna(0).astype(int)
merged_df['FM_RANK'] = merged_df['FM_RANK'].fillna(100).astype(int)
merged_df['TYPE'] = merged_df['TYPE'].fillna('🏠🏃 Hybrid').astype(str)
merged_df['PROFILE_PICTURE'] = merged_df['PROFILE_PICTURE'].fillna('https://i.ibb.co/ZNK5xmN/pdycc8-1-removebg-preview.png').astype(str)

# Convert 'ACTIVE' column to boolean
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna('No').astype(str)
merged_df['ACTIVE'] = merged_df['ACTIVE'].str.strip().str.lower().map({'yes': True, 'no': False})
merged_df['ACTIVE'] = merged_df['ACTIVE'].fillna(False)

# Ensure 'TYPE' column has valid options
valid_types = ['🏠🏃 Hybrid', '🏃 Field Marketing', '🏠 Web To Home']
merged_df['TYPE'] = merged_df['TYPE'].apply(lambda x: x if x in valid_types else '🏠🏃 Hybrid')

# Ensure 'MARKET' column has valid options
valid_market_types = df_markets['MARKET'].unique()
merged_df['MARKET'] = merged_df['MARKET'].apply(lambda x: x if x in valid_market_types else 'No Market')

# Prepare the dataframe for editing
edit_df = merged_df[['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK', 'SALESFORCE_ID']].copy()

# Initialize session state
if 'filtered_edit_df' not in st.session_state:
    st.session_state['filtered_edit_df'] = edit_df.copy()

# Display the editable dataframe
st.warning("ⓘ This page is for managers only. If you're not a manager or responsible for updating closer targets, please use the appointments page only.")

st.write("## 🎯 Edit Closer Targets")

# Modify option lists to include 'All'
def get_market_options(filtered_df):
    return ['All Markets'] + sorted(filtered_df['MARKET'].unique())

def get_closer_options(filtered_df):
    return ['All Closers'] + sorted(filtered_df['FULL_NAME'].unique())

def get_type_options(filtered_df):
    return ['All Channels'] + sorted(filtered_df['TYPE'].unique())

# Initialize with the full dataframe
filtered_edit_df = st.session_state['filtered_edit_df'].copy()

# Create columns for the filters
cols1, cols2, cols3 = st.columns(3)

# First filter: Market
with cols1:
    market_input = st.selectbox('', get_market_options(filtered_edit_df), index=0, key='market_select')

# Filter by market if not 'All Markets'
if market_input != 'All Markets':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['MARKET'] == market_input]

# Second filter: Closer
with cols2:
    closer_input = st.selectbox('', get_closer_options(filtered_edit_df), index=0, key='closer_select')

# Filter by closer if not 'All Closers'
if closer_input != 'All Closers':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['FULL_NAME'] == closer_input]

# Third filter: Type
with cols3:
    type_input = st.selectbox('', get_type_options(filtered_edit_df), index=0, key='type_select')

# Filter by type if not 'All Channels'
if type_input != 'All Channels':
    filtered_edit_df = filtered_edit_df[filtered_edit_df['TYPE'] == type_input]

# Sort the filtered DataFrame by 'FULL_NAME'
filtered_edit_df = filtered_edit_df.sort_values(by='FULL_NAME')

# Wrap the data editor and save button in a form
with st.form('editor_form'):
    # Reset indices for comparison
    original_filtered_df = filtered_edit_df.copy().reset_index(drop=True)
    
    # Configure the data editor with column configurations
    edited_df = st.data_editor(
        filtered_edit_df.reset_index(drop=True),
        column_order=['PROFILE_PICTURE', 'FULL_NAME', 'MARKET', 'TYPE', 'ACTIVE', 'GOAL', 'RANK', 'FM_GOAL', 'FM_RANK'],
        disabled={'FULL_NAME': True, 'PROFILE_PICTURE': True},
        hide_index=True,
        use_container_width=True,
        column_config={
            'PROFILE_PICTURE': st.column_config.ImageColumn(
                label=' '
            ),
            'ACTIVE': st.column_config.CheckboxColumn(
                'Active',
                help="Check if the closer is active",
                default=False
            ),
            'FULL_NAME': st.column_config.TextColumn(
                'Name'
            ),
            'MARKET': st.column_config.SelectboxColumn(
                'Market',
                options=valid_market_types,
                help="Select the market",
                required=True
            ),
            'GOAL': st.column_config.NumberColumn(
                'W2H Goal'
            ),
            'RANK': st.column_config.NumberColumn(
                'W2H Rank'
            ),
            'FM_GOAL': st.column_config.NumberColumn(
                'FM Goal'
            ),
            'FM_RANK': st.column_config.NumberColumn(
                'FM Rank'
            ),
            'TYPE': st.column_config.SelectboxColumn(
                'Type',
                options=valid_types,
                help="Select the type of channel",
                required=True
            ),
        }
    )
    
    # Add a submit button within the form
    submitted = st.form_submit_button('Save changes')

# Process the form submission
if submitted:
    # Normalize data types before comparison
    edited_df = edited_df.astype(str)
    original_filtered_df = original_filtered_df.astype(str)

    # Strip whitespaces
    edited_df = edited_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    original_filtered_df = original_filtered_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Compare the edited data with the original data
    changes = edited_df.compare(original_filtered_df)

    if changes.empty:
        st.info("No changes detected.")
    else:
        # Update session state with new edited data
        st.session_state['filtered_edit_df'].update(edited_df)
        # After updating the database
        if 'data_version' not in st.session_state:
            st.session_state['data_version'] = 0
        st.session_state['data_version'] += 1

        # Accumulate queries for batch execution
        queries = []
        for idx in changes.index.unique():
            row = edited_df.loc[idx]
            full_name = row['FULL_NAME'].replace("'", "''")
            new_goal = int(row['GOAL'])
            new_rank = int(row['RANK'])
            fm_goal = int(row['FM_GOAL'])
            fm_rank = int(row['FM_RANK'])
            new_active = row['ACTIVE']
            new_type = row['TYPE']
            new_market = row['MARKET']
            profile_picture = row['PROFILE_PICTURE']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            active_str = 'Yes' if new_active == 'True' else 'No'

            query = f"""
            MERGE INTO raw.snowflake.lm_appointments AS target
            USING (SELECT '{full_name}' AS NAME) AS source
            ON target.NAME = source.NAME
            WHEN MATCHED THEN
                UPDATE SET
                    GOAL = {new_goal},
                    RANK = {new_rank},
                    FM_GOAL = {fm_goal},
                    FM_RANK = {fm_rank},
                    ACTIVE = '{active_str}',
                    TYPE = '{new_type}',
                    MARKET = '{new_market}',
                    TIMESTAMP = '{timestamp}',
                    PROFILE_PICTURE = '{profile_picture}'
            WHEN NOT MATCHED THEN
                INSERT (CLOSER_ID, NAME, GOAL, RANK, FM_GOAL, FM_RANK, ACTIVE, TYPE, MARKET, TIMESTAMP, PROFILE_PICTURE)
                VALUES ('{row['SALESFORCE_ID']}', '{full_name}', {new_goal}, {new_rank}, {fm_goal}, {fm_rank}, '{active_str}', '{new_type}', '{new_market}', '{timestamp}', '{profile_picture}');
            """
            queries.append(query)

        # Execute all queries in a batch
        with st.spinner('Saving changes...'):
            for query in queries:
                try:
                    session.sql(query).collect()
                    st.success(f"Saved changes for {row['FULL_NAME']}")
                except Exception as e:
                    st.error(f"Error saving changes for {row['FULL_NAME']}: {str(e)}")

# --- Market Form ---
st.divider()
st.write("## 🏙️ Edit Markets")

with st.form('market_editor_form'):
    # Reset index for comparison
    original_market_df = df_markets.copy().reset_index(drop=True)

    edited_market_df = st.data_editor(
        df_markets[['MARKET', 'MARKET_GROUP', 'RANK', 'NOTES']].reset_index(drop=True),
        num_rows="dynamic",
        hide_index=True,
        use_container_width=True,
        column_config={
            'MARKET': st.column_config.TextColumn('Market'),
            'MARKET_GROUP': st.column_config.TextColumn('Market Group'),
            'RANK': st.column_config.NumberColumn('Rank'),
            'NOTES': st.column_config.TextColumn('Notes'),
        }
    )

    submitted_market = st.form_submit_button('Save Changes')

if submitted_market:
    # Reset index for comparison
    edited_market_df = edited_market_df.reset_index(drop=True)
    original_market_df = original_market_df.reset_index(drop=True)

    # Get the sets of markets
    original_markets = set(original_market_df['MARKET'])
    edited_markets = set(edited_market_df['MARKET'])

    new_markets = edited_markets - original_markets
    deleted_markets = original_markets - edited_markets
    common_markets = original_markets & edited_markets

    # Initialize lists to store queries
    queries = []

    # Handle deleted markets
    for market in deleted_markets:
        market_safe = market.replace("'", "''")
        query = f"DELETE FROM raw.snowflake.lm_markets WHERE MARKET = '{market_safe}';"
        queries.append((query, f"Deleted market '{market}'"))

    # Handle new markets
    new_markets_df = edited_market_df[edited_market_df['MARKET'].isin(new_markets)]
    for idx, row in new_markets_df.iterrows():
        # Process as insert
        market = row['MARKET']
        if pd.isna(market) or market == '':
            st.error("Market name cannot be empty.")
            continue  # Skip this row
        market = market.replace("'", "''")

        # Handle 'MARKET_GROUP' field
        market_group = row.get('MARKET_GROUP', '')
        if pd.isna(market_group):
            market_group = ''
        market_group = market_group.replace("'", "''")

        # Handle 'RANK' field
        rank = row.get('RANK', '')
        if pd.isna(rank) or rank == '':
            rank_value = 'NULL'
        else:
            try:
                rank_value = int(rank)
            except (ValueError, TypeError):
                st.error(f"Invalid rank value for market '{market}'. Rank must be an integer.")
                continue  # Skip this row

        # Handle 'NOTES' field
        notes = row.get('NOTES', '')
        if pd.isna(notes):
            notes = ''
        notes = notes.replace("'", "''")

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        query = f"""
        INSERT INTO raw.snowflake.lm_markets (MARKET, MARKET_GROUP, RANK, NOTES, TIMESTAMP)
        VALUES ('{market}', '{market_group}', {rank_value}, '{notes}', '{timestamp}');
        """
        queries.append((query, f"Inserted new market '{market}'"))

    # Handle updated markets
    for market in common_markets:
        # Get rows
        edited_row = edited_market_df[edited_market_df['MARKET'] == market].iloc[0]
        original_row = original_market_df[original_market_df['MARKET'] == market].iloc[0]

        # Compare rows (excluding 'MARKET' as it's the key)
        columns_to_compare = ['MARKET_GROUP', 'RANK', 'NOTES']
        if not edited_row[columns_to_compare].equals(original_row[columns_to_compare]):
            # There are changes
            market_safe = market.replace("'", "''")

            # Handle 'MARKET_GROUP' field
            market_group = edited_row.get('MARKET_GROUP', '')
            if pd.isna(market_group):
                market_group = ''
            market_group = market_group.replace("'", "''")

            # Handle 'RANK' field
            rank = edited_row.get('RANK', '')
            if pd.isna(rank) or rank == '':
                rank_value = 'NULL'
            else:
                try:
                    rank_value = int(rank)
                except (ValueError, TypeError):
                    st.error(f"Invalid rank value for market '{market}'. Rank must be an integer.")
                    continue  # Skip this row

            # Handle 'NOTES' field
            notes = edited_row.get('NOTES', '')
            if pd.isna(notes):
                notes = ''
            notes = notes.replace("'", "''")

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            query = f"""
            UPDATE raw.snowflake.lm_markets
            SET MARKET_GROUP = '{market_group}', RANK = {rank_value}, NOTES = '{notes}', TIMESTAMP = '{timestamp}'
            WHERE MARKET = '{market_safe}';
            """
            queries.append((query, f"Updated market '{market}'"))

    # Execute all queries in a batch
    if queries:
        with st.spinner('Saving changes...'):
            for query, message in queries:
                try:
                    session.sql(query).collect()
                    st.success(message)
                except Exception as e:
                    st.error(f"Error processing {message}: {str(e)}")
        # Reload data
        df_markets = get_market()
    else:
        st.info("No changes detected.")
    # After updating the database
    if 'data_version' not in st.session_state:
        st.session_state['data_version'] = 0
    st.session_state['data_version'] += 1
