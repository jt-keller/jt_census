import json
import pandas as pd
import geopandas as gpd
import requests
import os


# Dictionary to store year-specific information
years_dict = {
    '2020': {
        'file': 'dhc',
        'tigris_url': "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{state_fips}_tabblock20.zip",
        'geoid_header': 'GEOID20',
        'race': 'P5',
        'age': 'P12',
        'median_age': 'P13'
    },
    '2010': {
        'file': 'sf1',
        'tigris_url': "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK/tl_2020_{state_fips}_tabblock10.zip",
        'geoid_header': 'GEOID10',
        'race': 'P5',
        'age': 'P12',
        'median_age': 'P13'
    },
    '2000': {
        'file': 'sf1',
        'tigris_url': "https://www2.census.gov/geo/pvs/tiger2010st/{state_fips}_{state_name}/{state_fips}/tl_2010_{state_fips}_tabblock00.zip",
        'geoid_header': 'BLKIDFP00',
        'race': 'P008',
        'age': 'P012',
        'median_age': 'P013'
    }
}

def vars_dec(year, return_type="short"):
    year = str(year)
    
    if year not in years_dict:
        raise ValueError("Sorry -- the only years available here are 2020, 2010, and 2000.")

    file = years_dict[year]['file']
    vars_url = f"https://api.census.gov/data/{year}/dec/{file}/variables.html"

    vars_df = pd.read_html(vars_url)[0]
    vars_df = vars_df[['Name', 'Label', 'Concept', 'Group']]
    if return_type == "short":
        concepts = vars_df.drop_duplicates(subset=['Concept'], keep='first')[['Concept', 'Group']]
        return concepts
    elif return_type == "long":
        return vars_df
    else:
        raise ValueError("Invalid return_type. Choose either 'long' or 'short'.")


def vars_acs(year, return_type="short"):
    year = str(year)
    
    vars_url = f"https://api.census.gov/data/{year}/acs/acs5/variables.html"

    try:
        vars_df = pd.read_html(vars_url)[0]
    except Exception as e:
        print(f"Failed to fetch ACS variables for year {year}: {e}. Please choose a year from 2009 to 2023.")
    
    vars_df = vars_df[['Name', 'Label', 'Concept', 'Group']]
    if return_type == "short":
        concepts = vars_df.drop_duplicates(subset=['Concept'], keep='first')[['Concept', 'Group']]
        return concepts
    elif return_type == "long":
        return vars_df
    else:
        raise ValueError("Invalid return_type. Choose either 'long' or 'short'.")



def get_fips(state):
    with open('fips_dict.json', 'r') as f:
        fips_dict = json.load(f)

    state_usps, state_name, state_fips, counties = None, None, None, None

    if len(state) == 2:
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


def get_tig(year, state, units='block'):
    if 2001 <= int(year) <= 2009:
        year = '2000'
    elif 2010 <= int(year) <= 2019:
        year = '2010'
    elif int(year) >= 2020:
        year = '2020'
    else:
        year = str(year)

    state_usps, state_name, state_fips, _ = get_fips(state)

    if year not in years_dict:
        raise ValueError("Sorry, the census archive only goes as far back as 2000; we will consult IPUMS for earlier data (WIP)")

    if units == 'block':
        year_info = years_dict[year]
        tigris_url = year_info['tigris_url'].format(state_fips=state_fips, state_name=state_name)
        geoid_header = year_info['geoid_header']

        response = requests.get(tigris_url)

        if response.status_code != 200:
            raise Exception(f"Failed to download TIGER data with status code {response.status_code}: {response.text}")

        zip_path = f"{state_fips}_tabblock{year}.zip"

    elif units == 'bg':
        tigris_url = f"https://www2.census.gov/geo/tiger/TIGER{year}/{units.upper()}/tl_{year}_{state_fips}_{units.lower()}.zip"
        
        response = requests.get(tigris_url)

        if response.status_code != 200:
            raise Exception(f"Failed to download TIGER data with status code {response.status_code}: {response.text}")
        
        zip_path = f"{state_fips}_BG{year}.zip"


    with open(zip_path, 'wb') as f:
        f.write(response.content)

    try:
        gdf = gpd.read_file(f"zip://{zip_path}")
    finally:
        os.remove(zip_path)
    
    if 'tabblock' in zip_path:
        gdf = gdf.rename(columns={geoid_header: 'GEOID'})

    return gdf



def get_dec(year, state, county, var_group, apikey):
    year = str(year)
    
    if year not in years_dict:
        raise ValueError("Sorry -- the only years available here are 2020, 2010, and 2000.")

    year_info = years_dict[year]
    file = year_info['file']

    if var_group == "race":
        var_group = year_info['race']
    elif var_group == "age":
        var_group = year_info['age']
    elif var_group == 'median_age':
        var_group = year_info['median_age']

    # Get FIPS from fips_dict
    state_usps, state_name, state_fips, counties = get_fips(state)

    # get variables data
    vars_df = vars_dec(year, "long")

    county_fips = None
    for key, value in counties.items():
        if county.lower() in key.lower():
            county_fips = value
            break

    if county_fips is None:
        raise ValueError(f"County '{county}' not found in the FIPS dictionary for state '{state_fips}'. Please check the spelling.")

    # Fill base URL with variables
    url_template = "https://api.census.gov/data/{year}/dec/{file}?get=group({var_group})&for=block:*&in=state:{state_fips}%20county:{county_fips}&key={apikey}"
    url = url_template.format(year=year, file=file, var_group=var_group, state_fips=state_fips, county_fips=county_fips, apikey=apikey)
   
    # Pull data from URL
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")

    try:
        data = response.json()
    except ValueError as e:
        raise Exception(f"Failed to parse JSON response: {e}")

    df = pd.DataFrame(data[1:], columns=data[0])

    # Rename columns
    for col in df.columns:
        if var_group in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    vars_dict = vars_df.set_index('Name')['Label'].to_dict()
    df = df.rename(columns=vars_dict)

    # Clean the DataFrame
    df.rename(columns={'Geography': 'GEOID'}, inplace=True)
    df['GEOID'] = df['GEOID'].str.replace('1000000US', '')
    df['GEOID'] = df['GEOID'].astype(str)
    df = df.loc[:, ~df.columns.str.contains(var_group)]
    df.columns = df.columns.str.replace('Estimate!!', '').str.replace('!!', '', regex=True).str.replace(' ', '')

    # get tigris shapes
    tigris = get_tig(year, state)

    # spatialize census data w tigris shapes
    gdf = gpd.GeoDataFrame(df.merge(tigris[['GEOID', 'geometry']], on='GEOID', how='inner'), geometry='geometry', crs=tigris.crs)
    gdf = gdf.loc[:, ~gdf.columns.duplicated()]

    return gdf



def get_acs(year, state, county, var_group, apikey):
    year = str(year)

    # Get FIPS from fips_dict
    state_usps, state_name, state_fips, counties = get_fips(state)

    # get variables data
    vars = vars_acs(year, "long")

    county_fips = None
    for key, value in counties.items():
        if county.lower() in key.lower():
            county_fips = value
            break

    if county_fips is None:
        raise ValueError(f"County '{county}' not found in the FIPS dictionary for state '{state_fips}'. Please check the spelling.")

    # Fetch the actual data
    response = requests.get(f"https://api.census.gov/data/{year}/acs/acs5?get=NAME,group({var_group})&for=block%20group:*&in=state:{state_fips}%20county:{county_fips}&key={apikey}")
    df = pd.DataFrame(response.json()[1:], columns=response.json()[0])

    # Convert string columns that contain the 'var' string in their name and are numeric to numbers
    for col in df.columns:
        if var_group in col:  # Check if 'var' string is in the column name
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Create a mapping dictionary from 'Name' to 'Label' and rename the columns using the mapping
    vars_mapping = vars.set_index('Name')['Label'].to_dict()
    df = df.rename(columns=vars_mapping)
    
    # Clean the DataFrame
    df.rename(columns={'Geography': 'GEOID'}, inplace=True)
    df['GEOID'] = df['GEOID'].str.replace('1500000US', '')
    df['GEOID'] = df['GEOID'].astype(str)
    df = df.loc[:, ~df.columns.str.contains(var_group)]
    df.columns = df.columns.str.replace('Estimate!!', '').str.replace('!!', '', regex=True).str.replace(' ', '')
    
    # get tigris shapes
    tigris = get_tig(year, state, units='bg')

    # spatialize census data w tigris shapes
    gdf = gpd.GeoDataFrame(df.merge(tigris[['GEOID', 'geometry']], on='GEOID', how='inner'), geometry='geometry', crs=tigris.crs)
    gdf = gdf.loc[:, ~gdf.columns.duplicated()]

    return gdf    
    
    

