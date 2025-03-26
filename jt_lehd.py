import pandas as pd
import geopandas as gpd
import numpy as np
import requests
import re
import json
import os

def get_fips(state):
    with open('fips_dict.json', 'r') as f:
        fips_dict = json.load(f)

    state_usps, state_name, state_fips, counties = None, None, None, None

    if len(state) == 2:
        state = state.upper()
        if state in fips_dict:
            state_usps = state
            state_fips = fips_dict[state]['state_fips']
            state_name = fips_dict[state]['state_name']
            counties = fips_dict[state]['counties']
        else:
            raise ValueError(f"USPS code '{state}' not found in FIPS dictionary...")
    else:
        for key in fips_dict:
            if fips_dict[key]['state_name'].lower() == state.lower():
                state_usps = key
                state_fips = fips_dict[key]['state_fips']
                state_name = fips_dict[key]['state_name']
                counties = fips_dict[key]['counties']
                break

    if state_usps is None or state_name is None or state_fips is None or counties is None:
        raise ValueError(f"Invalid state input; please check the spelling or use the USPS 2-char abbreviation (e.g., MA).")

    return state_usps, state_name, state_fips, counties

def get_blocks(year, states):
    # Convert year to integer for comparison
    year = int(year)

    # Load the FIPS dictionary from 'fips_dict.json' file
    with open('fips_dict.json', 'r') as f:
        fips_dict = json.load(f)

    # Define the URL and GEOID header based on the year
    if year < 2010:
        url_template = "https://www2.census.gov/geo/pvs/tiger2010st/{state_fips}_{state_name}/{state_fips}/tl_2010_{state_fips}_tabblock00.zip"
        geoid_header = 'BLOCKID00'
    elif 2010 <= year <= 2019:
        url_template = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK/tl_2020_{state_fips}_tabblock10.zip"
        geoid_header = 'GEOID10'
    elif year >= 2020:
        url_template = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{state_fips}_tabblock20.zip"
        geoid_header = 'GEOID20'
    else:
        raise ValueError("Sorry, the census archive only goes as far back as 2000; we will consult IPUMS for earlier data (WIP)")

    # Ensure that state codes are uppercase
    states = [state.upper() for state in states]

    # Initialize a list to hold all GeoDataFrames
    gdfs = []

    # Iterate over the specified states in the list
    for state_input in states:
        state_info = None
        state_name = None
        state_fips = None

        # Check if the input matches a state abbreviation
        if state_input in fips_dict:
            state_info = fips_dict[state_input]
            state_name = state_info['state_name']
            state_fips = state_info['state_fips']
        
        # If not found, check if it matches a state FIPS code
        else:
            for abbr, info in fips_dict.items():
                if info['state_fips'] == state_input:
                    state_info = info
                    state_name = state_info['state_name']
                    state_fips = state_info['state_fips']
                    break
        
        # If state_info is still None, input is invalid
        if state_info is None:
            print(f"State '{state_input}' not found in FIPS dictionary.")
            continue

        # Construct the URL for the current state
        tigris_url = url_template.format(state_fips=state_fips, state_name=state_name)

        # Fetch the data from the URL
        try:
            response = requests.get(tigris_url)
            if response.status_code != 200:
                print(f"Failed to download data for {state_name} ({state_input}) with status code {response.status_code}")
                continue

            # Save the downloaded zip file
            zip_path = f"{state_fips}_tabblock{year}.zip"
            with open(zip_path, 'wb') as f:
                f.write(response.content)

            # Read the shapefile from the zip archive
            try:
                gdf = gpd.read_file(f"zip://{zip_path}")
                # Rename the geoid column to 'GEOID'
                gdf = gdf.rename(columns={geoid_header: 'GEOID'})

                # Add the GeoDataFrame to the list
                gdfs.append(gdf)
                print(f"Fetched block shapes for {state_name} ({state_input})")

            finally:
                os.remove(zip_path)  # Remove the zip file after reading

        except Exception as e:
            print(f"Error fetching data for {state_name} ({state_input}): {e}")

    # Concatenate all GeoDataFrames into one complete GeoDataFrame
    if gdfs:
        complete_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
        print("Successfully combined selected state data into one complete GeoDataFrame.")
        return complete_gdf
    else:
        raise Exception("No data was successfully fetched for the specified states.")

def get_muni(muni, state):
    # Use get_fips to get the state FIPS code
    state_usps, state_name, state_fips, _ = get_fips(state)

    # Construct the URL for the COUSUB file
    cousub_url = f"https://www2.census.gov/geo/tiger/TIGER2021/COUSUB/tl_2021_{state_fips}_cousub.zip"

    # Fetch the COUSUB data
    try:
        response = requests.get(cousub_url)
        if response.status_code != 200:
            raise Exception(f"Failed to download county subdivision data with status code {response.status_code}")

        # Save the downloaded zip file
        zip_path = f"{state_fips}_cousub.zip"
        with open(zip_path, 'wb') as f:
            f.write(response.content)

        # Read the shapefile from the zip archive
        try:
            gdf = gpd.read_file(f"zip://{zip_path}")

            # Filter by the 'NAME' column to match the input 'muni', case-insensitive
            muni_gdf = gdf[gdf['NAME'].str.strip().str.lower() == muni.strip().lower()]

            if muni_gdf.empty:
                raise ValueError(f"No match found for municipality '{muni}' in state '{state}'.")

            print(f"Successfully extracted municipality '{muni}' from the county subdivision file.")
            return muni_gdf

        finally:
            os.remove(zip_path)  # Remove the zip file after reading

    except Exception as e:
        print(f"Error fetching or processing county subdivision data for {state}: {e}")

    
def fetch_OD(muni, state, year, direction):
    # Convert state to lowercase
    state = state.lower()

    # Define valid states and year range
    valid_states = [
        'ak', 'al', 'ar', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga',
        'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 'la', 'ma', 'md', 'me',
        'mi', 'mn', 'mo', 'ms', 'mt', 'nc', 'nd', 'ne', 'nh', 'nj', 'nm',
        'nv', 'ny', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx',
        'ut', 'va', 'vt', 'wa', 'wi', 'wv', 'wy'
    ]
    valid_years = range(2003, 2022)

    # Check for valid state
    if state not in valid_states:
        print("State not found. Please make sure to use a valid 2-letter state abbreviation.")
        return None

    # Check for valid year
    if year not in valid_years:
        print("Year not found. Please make sure to use YYYY, and note that data is available only from 2003 to 2021.")
        return None

    # Initialize an empty list to hold DataFrames
    dfs = []

    # Fetch main dataset for the input state
    main_url = fr"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/od/{state}_od_main_JT00_{year}.csv.gz"
    try:
        main_df = pd.read_csv(main_url, compression='gzip')
        dfs.append(main_df)
        print(f"Fetched main dataset for {state}.")
    except Exception as e:
        print(f"Failed to fetch main dataset for {state}: {e}")

    # Fetch aux datasets for all states, including the input state
    for s in valid_states:
        aux_url = fr"https://lehd.ces.census.gov/data/lodes/LODES8/{s}/od/{s}_od_aux_JT00_{year}.csv.gz"
        try:
            aux_df = pd.read_csv(aux_url, compression='gzip')
            dfs.append(aux_df)
            print(f"Fetched aux dataset for {s}.")
        except Exception as e:
            print(f"Failed to fetch aux dataset for {s}: {e}")

    # Concatenate all DataFrames into one
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)

        # Convert specific columns to string type
        combined_df = combined_df.astype({'w_geocode': str, 'h_geocode': str})

        # Ensure 15-digit geocodes by adding a leading '0' if needed
        for col in ['w_geocode', 'h_geocode']:
            combined_df[col] = combined_df[col].apply(lambda x: '0' + x if len(x) == 14 else x)

        # Define the mapping of original column names to new labels
        OD_code_map = {
            "w_geocode": "w_GEOID",
            "h_geocode": "h_GEOID",
            "S000": "tot_jobs",
            "SA01": "wrkr_<=29",
            "SA02": "wrkr_30-54",
            "SA03": "wrkr_54+",
            "SE01": "jobs_<=$1250/m",
            "SE02": "jobs_$1251-$3333/m",
            "SE03": "jobs_>$3333/m",
            "SI01": "sector_prod",
            "SI02": "sector_util",
            "SI03": "sector_other",
            "createdate": "createdate"
        }

        # Rename columns based on the mapping dictionary
        combined_df.rename(columns=OD_code_map, inplace=True)

        # fetch municipality boundaries
        muni_gdf = get_muni(muni, state)
        # fetch state blocks
        state_blks = get_blocks(year, [state])

        # identify blocks within muni boundaries
        muni_blocks = gpd.clip(state_blks, muni_gdf)
        muni_blks_GEOID = muni_blocks['GEOID'].unique().tolist()
        
        # scan h_GEOID (ORIGIN) for GEOIDs in list of blocks within municipality
        From_muni = combined_df[combined_df['h_GEOID'].isin(muni_blks_GEOID)]
        print("Number of workers from municipality:", len(From_muni))
        # group this data by unique destination GEOID
        From_muni_sum = From_muni.drop('h_GEOID', axis=1)
        From_muni_sum = From_muni_sum.groupby('w_GEOID').sum().reset_index()
        print("Number of destination blocks of workers from municipality:", len(From_muni_sum))
        # get list of states where these destination blocks are
        From_muni_sum['fips'] = From_muni_sum['w_GEOID'].str[:2]
        From_states = From_muni_sum['fips'].unique().tolist()
        print("Number of states where workers in municipality are commuting from:", len(From_muni_sum))

        # scan w_GEOID (DESTINATION) for GEOIDs in list of blocks within municipality
        To_muni = combined_df[combined_df['w_GEOID'].isin(muni_blks_GEOID)]
        print("Number of workers w jobs in municipality:", len(To_muni))
        # group this data by unique origin GEOID
        To_muni_sum = To_muni.drop('w_GEOID', axis=1)
        To_muni_sum = To_muni_sum.groupby('h_GEOID').sum().reset_index()
        print("Number of origin blocks of workers to municipality:", len(To_muni_sum))
        # get list of states where these origin blocks are
        To_muni_sum['fips'] = To_muni_sum['h_GEOID'].str[:2]
        To_states = To_muni_sum['fips'].unique().tolist()
        print("Number of states where workers from municipality are commuting to:", len(From_muni_sum))

        # create list of all states that show up across the To and From datasets
        all_states = list(set(From_states + To_states))
        all_states.sort()

        # grab all block polygons across USA 
        # (need to brainstorm more efficient approach -- get list of all unique first 2 digits of GEOID to get list of states...)
        all_blocks = get_blocks(year, all_states)
        all_blocks['GEOID'] = all_blocks['GEOID'].astype(str)

        # merge (i.e. join) the From_muni and To_muni datasets to their block shapes
        print("joining dataframes to their block shapes...")
        From_muni_gdf = all_blocks.merge(From_muni_sum, left_on='GEOID', right_on='w_GEOID', how='inner')
        To_muni_gdf = all_blocks.merge(To_muni_sum, left_on='GEOID', right_on='h_GEOID', how='inner')

        # Export and return based on direction
        if direction == "from":
            From_muni_gdf.to_file(f"From_{muni}.gpkg", driver='GPKG')
            return From_muni_gdf
        elif direction == "to":
            To_muni_gdf.to_file(f"To_{muni}.gpkg", driver='GPKG')
            return To_muni_gdf
        else:
            print("Invalid direction. Use 'from' or 'to'.")
            return None





def fetch_WAC(muni, state, year):
    # Convert state to lowercase
    state = state.lower()

    # Define valid states and year range
    valid_states = [
        'ak', 'al', 'ar', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga',
        'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 'la', 'ma', 'md', 'me',
        'mi', 'mn', 'mo', 'ms', 'mt', 'nc', 'nd', 'ne', 'nh', 'nj', 'nm',
        'nv', 'ny', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx',
        'ut', 'va', 'vt', 'wa', 'wi', 'wv', 'wy'
    ]
    valid_years = range(2003, 2022)

    # Check for valid state
    if state not in valid_states:
        print("State not found. Please make sure to use a valid 2-letter state abbreviation.")
        return None

    base_url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/wac/"
    
    try:
        html = requests.get(base_url).text
    except Exception as e:
        print(f"Failed to access WAC directory for {state}: {e}")
        return None

    # Extract all valid years from filenames
    matches = re.findall(fr"{state}_wac_S000_JT00_(\d{{4}}).csv.gz", html)
    if not matches:
        print("No WAC files found for this state.")
        return None

    available_years = sorted(set(int(m) for m in matches))
    latest_year = available_years[-1]

    if year == "latest":
        year = latest_year
    elif isinstance(year, int) and year > latest_year:
        print(f"Latest year available is {latest_year}. Fetching that.")
        year = latest_year
    elif isinstance(year, int) and year not in available_years:
        print(f"Year {year} not available for {state}. Available years: {available_years}")
        return None

    file_url = f"{base_url}{state}_wac_S000_JT00_{year}.csv.gz"
    try:
        df = pd.read_csv(file_url, compression='gzip')
        print(f"Fetched main dataset for {state}, year {year}.")
        return df
    except Exception as e:
        print(f"Failed to fetch WAC dataset for {state} in {year}: {e}")
        return None
    
    WAC_variables = {
        "Variable": [
            "w_geocode", "C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03",
            "CNS01", "CNS02", "CNS03", "CNS04", "CNS05", "CNS06", "CNS07", "CNS08",
            "CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS14", "CNS15", "CNS16",
            "CNS17", "CNS18", "CNS19", "CNS20", "CR01", "CR02", "CR03", "CR04",
            "CR05", "CR07", "CT01", "CT02", "CD01", "CD02", "CD03", "CD04", "CS01",
            "CS02", "CFA01", "CFA02", "CFA03", "CFA04", "CFA05", "CFS01", "CFS02",
            "CFS03", "CFS04", "CFS05", "createdate"
        ],
    
        "Explanation": [
            "Workplace Census Block Code",
            "Total number of jobs",
            "Number of jobs for workers age 29 or younger",
            "Number of jobs for workers age 30 to 54",
            "Number of jobs for workers age 55 or older",
            "Number of jobs with earnings $1250/month or less",
            "Number of jobs with earnings $1251/month to $3333/month",
            "Number of jobs with earnings greater than $3333/month",
            "Number of jobs in NAICS sector 11 (Agriculture, Forestry, Fishing and Hunting)",
            "Number of jobs in NAICS sector 21 (Mining, Quarrying, and Oil and Gas Extraction)",
            "Number of jobs in NAICS sector 22 (Utilities)",
            "Number of jobs in NAICS sector 23 (Construction)",
            "Number of jobs in NAICS sector 31-33 (Manufacturing)",
            "Number of jobs in NAICS sector 42 (Wholesale Trade)",
            "Number of jobs in NAICS sector 44-45 (Retail Trade)",
            "Number of jobs in NAICS sector 48-49 (Transportation and Warehousing)",
            "Number of jobs in NAICS sector 51 (Information)",
            "Number of jobs in NAICS sector 52 (Finance and Insurance)",
            "Number of jobs in NAICS sector 53 (Real Estate and Rental and Leasing)",
            "Number of jobs in NAICS sector 54 (Professional, Scientific, and Technical Services)",
            "Number of jobs in NAICS sector 55 (Management of Companies and Enterprises)",
            "Number of jobs in NAICS sector 56 (Administrative and Support and Waste Management and Remediation Services)",
            "Number of jobs in NAICS sector 61 (Educational Services)",
            "Number of jobs in NAICS sector 62 (Health Care and Social Assistance)",
            "Number of jobs in NAICS sector 71 (Arts, Entertainment, and Recreation)",
            "Number of jobs in NAICS sector 72 (Accommodation and Food Services)",
            "Number of jobs in NAICS sector 81 (Other Services [except Public Administration])",
            "Number of jobs in NAICS sector 92 (Public Administration)",
            "Number of jobs for workers with Race: White, Alone10",
            "Number of jobs for workers with Race: Black or African American Alone",
            "Number of jobs for workers with Race: American Indian or Alaska Native Alone",
            "Number of jobs for workers with Race: Asian Alone10",
            "Number of jobs for workers with Race: Native Hawaiian or Other Pacific Islander Alone",
            "Number of jobs for workers with Race: Two or More Race Groups",
            "Number of jobs for workers with Ethnicity: Not Hispanic or Latino",
            "Number of jobs for workers with Ethnicity: Hispanic or Latino",
            "Number of jobs for workers with Educational Attainment: Less than high school",
            "Number of jobs for workers with Educational Attainment: High school or equivalent, no college",
            "Number of jobs for workers with Educational Attainment: Some college or Associate degree",
            "Number of jobs for workers with Educational Attainment: Bachelor's degree or advanced degree",
            "Number of jobs for workers with Sex: Male",
            "Number of jobs for workers with Sex: Female",
            "Number of jobs for workers at firms with Firm Age: 0-1 Years",
            "Number of jobs for workers at firms with Firm Age: 2-3 Years",
            "Number of jobs for workers at firms with Firm Age: 4-5 Years",
            "Number of jobs for workers at firms with Firm Age: 6-10 Years",
            "Number of jobs for workers at firms with Firm Age: 11+ Years",
            "Number of jobs for workers at firms with Firm Size: 0-19 Employees",
            "Number of jobs for workers at firms with Firm Size: 20-49 Employees",
            "Number of jobs for workers at firms with Firm Size: 50-249 Employees",
            "Number of jobs for workers at firms with Firm Size: 250-499 Employees",
            "Number of jobs for workers at firms with Firm Size: 500+ Employees",
            "Date on which data was created, formatted as YYYYMMDD"
        ],

        "Label": [
            "GEOID",
            "Total",
            "age_<=29",
            "age_30_to_54",
            "age_55+",
            "monthly_earnings_<=$1250",
            "monthly_earnings_$1251_to_$3333",
            "monthly_earnings_$3333+",
            "sector_Agriculture_Forestry_Fishing_Hunting",
            "sector_Mining_Quarrying_Oil_Gas",
            "sector_Utilities",
            "sector_Construction",
            "sector_Manufacturing",
            "sector_WholesaleTrade",
            "sector_Retail Trade",
            "sector_Transportation_Warehousing",
            "sector_Information",
            "sector_Finance_Insurance",
            "sector_RealEstateRentalLeasing",
            "sector_ProfessionalScientificTechnicalServices",
            "sector_ManagementCompaniesEnterprises",
            "sector_WasteManagement&Remediation Services",
            "sector_EducationalServices",
            "sector_Healthcare&SocialAssistance",
            "sector_ArtsEntertainmentRecreation)",
            "sector_Accommodation&FoodServices)",
            "sector_Other_ExceptPublicAdmin",
            "Sector_Public_Admin",
            "race_white",
            "race_Black",
            "race_AmericanIndian",
            "race_Asian",
            "race_Hawaiian",
            "race_multiracial",
            "ethnicity_NonLatino",
            "ethnicity_Latino",
            "edu_<HS",
            "edu_HSorGED",
            "edu_SomeCollege",
            "edu_Bach+",
            "sex_M",
            "sex_F",
            "firm_0to1yr_old",
            "firm_2to3yr_old",
            "firm_4to5yr_old",
            "firm_6to10yr_old",
            "firm_11+yr_old",
            "firm_size_0to19ppl",
            "firm_size_20to49ppl",
            "firm_size_50to249ppl",
            "firm_size_250to499ppl",
            "firm_size_500+ppl",
            "Date"
        ]
    }
    # Create DataFrame for WAC variables
    WAC_dictionary = pd.DataFrame(WAC_variables)
    # Use the 'Variable' column as the index to directly map 'Label' values for renaming
    main_df.rename(columns=WAC_dictionary.set_index('Variable')['Label'].to_dict(), inplace=True)
    main_df['GEOID'] = main_df['GEOID'].astype(str)
    # find more elegant approach to above step (right now it's dictionary to df and back to dictionary)

    # fetch blocks shapes
    state_blocks = get_blocks(2021, [state])
    state_blocks['GEOID'] = state_blocks['GEOID'].astype(str)

    # Join WAC data to block shapes
    state_blocks = state_blocks.merge(main_df, left_on='GEOID', right_on='GEOID', how='left')

    # fetch muni shape
    muni_gdf = get_muni(muni, state)
    
    # clip to muni
    muni_blocks = gpd.clip(state_blocks, muni_gdf)

    # export to geopackage
    muni_blocks.to_file(f"WAC_{muni}.gpkg", driver='GPKG')

    return muni_blocks