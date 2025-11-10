import pandas as pd
import geopandas as gpd
import requests
import json
from shapely.geometry import Point
import plotly.graph_objects as go
import os
from sqlalchemy import create_engine, inspect, text
import folium
import json
from dotenv import load_dotenv
import numpy as np

# other sources
# ecology surface water standards
#https://geo.wa.gov/datasets/4cd8bdeaa372425f9de28ce64955d36f_7/explore?location=47.242192%2C-120.592491%2C7.41&showTable=true

# population in poverty
# https://geo.wa.gov/datasets/6cc232508784436ab93965f0775b84c6_0/explore?location=47.224740%2C-120.811974%2C7.54

# 303d water quality assessment
# https://geo.wa.gov/datasets/b2fdb9e45dcb448caeab079b5636816d_4/explore?location=47.749336%2C-122.146616%2C10.00

# wa high res change detection
# https://geo.wa.gov/datasets/2259cc832d7a4c2eaa557b7b478e3288_1/explore?location=47.392686%2C-120.869000%2C7.62

# we really want to first clip watersheds to sites
# clip census data to site_watersheds
# append stats to clipped census_watersheds


def fetch_nhd_waterbodies_geojson():
   # water bodies
   # https://geo.wa.gov/datasets/2259cc832d7a4c2eaa557b7b478e3288_1/explore?location=47.392686%2C-120.869000%2C7.62
   # https://services.arcgis.com/6lCKYNJLvwTXqrmp/arcgis/rest/services/NHD/FeatureServer/6/query?outFields=*&where=1%3D1&f=geojson
   
   # flow lines
   # https://geo.wa.gov/datasets/waecy::hydrography-nhd-flowlines/about
   # https://services.arcgis.com/6lCKYNJLvwTXqrmp/arcgis/rest/services/NHD/FeatureServer/3/query?outFields=*&where=1%3D1&f=geojson
    """Fetch watershed boundaries from King County GIS"""
    try:
        geojson_url = "https://services.arcgis.com/6lCKYNJLvwTXqrmp/arcgis/rest/services/NHD/FeatureServer/5/query?outFields=*&where=1%3D1&f=geojson"
        response = requests.get(geojson_url)
        return response.json()
    except Exception as e:
        print(f"Error fetching GeoJSON: {e}")
        return None


def fetch_cao_geojson():
    # https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/2587/query?outFields=*&where=1%3D1&f=geojson
    # https://gis-kingcounty.opendata.arcgis.com/datasets/9ff7b65f45c94880bd8a6466c191f264_2587/explore?location=47.463068%2C-121.930050%2C10.19
    # fetch cao boundaries from king county gis

    """Fetch watershed boundaries from King County GIS"""
    try:
        geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/2587/query?outFields=*&where=1%3D1&f=geojson"
        response = requests.get(geojson_url)
        return response.json()
    except Exception as e:
        print(f"Error fetching GeoJSON: {e}")
        return None

def fetch_environmental_health_geojson():
    # shorter version
    # https://geo.wa.gov/datasets/c2c929f4bf0046aa814648823ccb6206_0/explore?location=47.224740%2C-120.811974%2C7.54
    # https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/Environmental_Effects/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
    
    # full version havent figured out how to query the ehd 
    # https://geo.wa.gov/datasets/WADOH::full-environmental-health-disparities-version-2-extract/about
    # https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/EHD_Combined_V2/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
    # report
    #https://deohs.washington.edu/washington-environmental-health-disparities-map-project
    """Fetch watershed boundaries from King County GIS"""
    try:
        geojson_url = "https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/Environmental_Effects/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
        response = requests.get(geojson_url)
        return response.json()
    except Exception as e:
        print(f"Error fetching GeoJSON: {e}")
        return None
    
def fetch_ppov_geojson():
    # https://geo.wa.gov/datasets/6cc232508784436ab93965f0775b84c6_0/explore?location=47.184033%2C-120.811974%2C7.54
    # https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/Population_Living_in_Poverty_v2/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
    """Fetch watershed boundaries from King County GIS"""
    try:
        geojson_url = "https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/Population_Living_in_Poverty_v2/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
        response = requests.get(geojson_url)
        return response.json()
    except Exception as e:
        print(f"Error fetching GeoJSON: {e}")
        return None
    
def get_table_data(table_name, selected_site=None, parameter=None):
    # get connection information
    load_dotenv()
    DATABASE_URL = os.environ.get("DATABASE_URL")
    USERNAME_PASSWORD_PAIRS = os.environ.get("USERNAME_PASSWORD_PAIRS")
    USERNAME_PASSWORD_PAIRS = [tuple(pair.split(":")) for pair in USERNAME_PASSWORD_PAIRS.split(",")]
    SECRET_KEY = os.environ.get("SECRET_KEY")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set.")
    # SQLAlchemy engine
    engine = create_engine(DATABASE_URL)
    if selected_site == None:
        base_query = f'SELECT * FROM "{table_name}"'
        params = None
    else:
        # Build base query
        base_query = f'SELECT * FROM "{table_name}" WHERE site = :site'
        params = {"site": selected_site}

    # Add parameter filter if provided
    if parameter is not None:
        base_query += " AND parameter = :parameter"
        params["parameter"] = parameter

    query = text(base_query)

    with engine.connect() as conn:
        if params == None:
            df = pd.read_sql(query, conn)
        else:
            df = pd.read_sql(query, conn, params=params)

    return df

def site_import(parameter = None):
    """import sites, filters converts to gef exports"""
    # 1. Load sites data
    sites = get_table_data("site")
    # paramters to list  not actually needed because you can read a string but this is better
    sites['parameter'] = sites['parameter'].apply(lambda x: json.loads(x) if x != '[]' else [])
    # filter sites by parameter if provided
    if parameter != "None":
        sites = sites[sites['parameter'].apply(lambda x: parameter in x if isinstance(x, list) else False)]
    
    # sites location processing
    sites['coordinates'] = sites['location'].apply(json.loads)
    sites['latitude'] = sites['coordinates'].apply(lambda x: x[0])
    sites['longitude'] = sites['coordinates'].apply(lambda x: x[1])
    sites = sites.drop('coordinates', axis=1)

    site_points = [Point(lon, lat) for lon, lat in zip(sites['longitude'], sites['latitude'])]
    sites_gdf = gpd.GeoDataFrame(sites, geometry=site_points, crs='EPSG:4326')

    # drop columns
    sites_gdf = sites_gdf.dropna(subset=['longitude', 'latitude', 'location'])
    sites_gdf.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/sites.geojson", driver="GeoJSON")
    return sites_gdf

def watershed_import():
    if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/watersheds.geojson"):
        print("watersheds exists") #print("Clipped file already exists!")
        # Load the existing file
        watersheds = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/watersheds.geojson")
        return watersheds
    else:
        print("importing watersheds")
        # import watersheds 
        #"""Fetch watershed boundaries from King County GIS"""
        try:
            #geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/hydro___base/MapServer/344/query?outFields=*&where=1%3D1&f=geojson"
            # https://gis-kingcounty.opendata.arcgis.com/datasets/afb2bb73bff048c48554fedd2366d83a_237/explore?location=47.462842%2C-121.887700%2C9.58
            geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/237/query?outFields=*&where=1%3D1&f=geojson"
            response = requests.get(geojson_url)
            # Convert response to GeoDataFrame
            watersheds = gpd.read_file(response.text)
            watersheds = watersheds.to_crs('EPSG:4326')
            watersheds = watersheds.drop(columns=["OBJECTID_1", "CONDITION"])
            watersheds = watersheds.rename(columns={"STUDY_UNIT": "basin"})
            watersheds = watersheds.set_index("OBJECTID")
            # Save to file
            watersheds.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/watersheds.geojson", driver="GeoJSON")
        except Exception as e:
            #print(f"Error fetching GeoJSON: {e}")
            return None
        return watersheds
        
def site_basin(sites_gdf, watersheds):
        """assigns basin to sites"""
        #if not "basin" in sites_gdf:
        sites_gdf = gpd.sjoin(sites_gdf, watersheds, how='left', predicate='intersects')
        sites_gdf = sites_gdf[['site', 'project', 'notes', 'latitude', 'longitude', 'geometry', 'basin']]
        return sites_gdf
        #else:
        #    return sites_gdf
        
def watershed_condition(sites_gdf, census_gdf, watersheds):
        """adds watershed environmental_condition to site watersheds, uses basin"""
        """ this is partially redundent since I am using the environmental condition gdf for the watersheds layer but i want to use a different watersheds layer"""
        """Fetch watershed boundaries from King County GIS"""
        #if "environmental_condition" not in watersheds.columns:
        #print("environmental condition not found")
        #geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/hydro___base/MapServer/344/query?outFields=*&where=1%3D1&f=geojson"
        # https://gis-kingcounty.opendata.arcgis.com/datasets/afb2bb73bff048c48554fedd2366d83a_237/explore?location=47.462842%2C-121.887700%2C9.58
        #geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/237/query?outFields=*&where=1%3D1&f=geojson"
        #response = requests.get(geojson_url)
    
        """Fetch watershed boundaries from King County GIS"""
    
        #geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/hydro___base/MapServer/344/query?outFields=*&where=1%3D1&f=geojson"
        # https://gis-kingcounty.opendata.arcgis.com/datasets/afb2bb73bff048c48554fedd2366d83a_237/explore?location=47.462842%2C-121.887700%2C9.58
        #geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/237/query?outFields=*&where=1%3D1&f=geojson"
        #response = requests.get(geojson_url)
        #condition = gpd.read_file(response.text)
        condition  = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/environmental_condition_of_basins.geojson")
        condition = condition.to_crs('EPSG:4326')
        condition = condition.drop(columns=["OBJECTID_1"])
        condition = condition.rename(columns={"STUDY_UNIT": "basin"})
        condition = condition.rename(columns={"CONDITION": "environmental_condition"})
        condition = condition.set_index("OBJECTID")
        watersheds = watersheds.merge(condition[['basin', "environmental_condition"]], on='basin', how='left')
        census_gdf = census_gdf.merge(condition[['basin', "environmental_condition"]], on='basin', how='left')
     
        #site_watersheds.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/site_watersheds.geojson", driver="GeoJSON")
        return census_gdf, watersheds
    #else:
        #print("environmental condition found")
        #return watersheds
    #site_watersheds = site_watersheds.loc[site_watersheds.sjoin(watershed_condition, how="inner", predicate='intersects').index.unique()]

def filter_cao(sites_gdf, watersheds):
    if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/cao_clipped.geojson"):
        print("cao data exists")
        # Load the existing file
        cao_gdf = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/cao_clipped.geojson")
        
    else:
        print("importing cao data")
        # Set environment variable and process
        #os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
        #nhd_waterbodies_gdf = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/cao.geojson")
         # https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/2587/query?outFields=*&where=1%3D1&f=geojson
        # https://gis-kingcounty.opendata.arcgis.com/datasets/9ff7b65f45c94880bd8a6466c191f264_2587/explore?location=47.463068%2C-121.930050%2C10.19
        # fetch cao boundaries from king county gis

        geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/2587/query?outFields=*&where=1%3D1&f=geojson"
        response = requests.get(geojson_url)
        cao_gdf = gpd.read_file(response.text)
        cao_gdf = cao_gdf.to_crs('EPSG:4326')
        cao_gdf = cao_gdf[['HAZARD_TYPE', 'HAZARD_SUBTYPE','HAZARD_BUFFER','geometry']]
        #cao_gdf = cao_gdf.loc[cao_gdf.sjoin(site_watersheds, how="inner", predicate='intersects').index.unique()]
        cao_gdf = cao_gdf.sjoin(site_watersheds[['basin', 'geometry']], how="inner", predicate='intersects').drop(columns=['index_right'])
        # Clip and save
        #nhd_waterbodies_gdf = nhd_waterbodies_gdf.clip(site_watersheds)
        cao_gdf.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/cao_clipped.geojson", driver="GeoJSON")
    return cao_gdf
   
def filter_nhd_centerlines(watersheds):
    #https://geo.wa.gov/datasets/71fa52e7d6224fde8b09facb12b30f04_3/explore?location=47.775316%2C-120.094375%2C6.99
    if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/nhd_centerlines_clipped.geojson"):
    #    print("nhd centerlines filter exists")
    #    # Load the existing file
        os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
        nhd_centerlines = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/nhd_centerlines_clipped.geojson")
    
    else:
        print("import nhd centerlines")
        # Set environment variable and process
        os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
        nhd_centerlines = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/nhd_centerlines.geojson")
        # Ensure same CRS
        if nhd_centerlines.crs != watersheds.crs:
            nhd_centerlines = nhd_centerlines.to_crs(watersheds.crs)
        # remove unneeded columns
        columns_to_drop = ['FType',  'FCode', 'FDate', 'WBArea_Permanent_Identifier', 'FlowDir', 'InNetwork', 'ReachCode', 'Resolution', 'MainPath', 'InNetwork ', 'KnownStreamOrder', 'From_Node', 'Permanent_Identifier', 'GlobalID', 'column3', 'GNIS_ID', 'To_Node', 'HydroID', 'NextDownID']
        nhd_centerlines = nhd_centerlines.drop(columns=columns_to_drop, errors='ignore')
        
        #add basin information
        # Clip to shape first
        nhd_centerlines = nhd_centerlines.clip(watersheds)

        # Then add basin information
        nhd_centerlines = nhd_centerlines.sjoin(
                watersheds[['basin', 'geometry']], 
                how="left", 
                predicate='intersects'
            ).drop(columns=['index_right'])
            # Remove duplicates if a line intersects multiple basins (keep first match)
        nhd_centerlines = nhd_centerlines.drop_duplicates(subset='OBJECTID', keep='first')
        nhd_centerlines = nhd_centerlines.loc[nhd_centerlines["StreamOrder"].notna()]
       
        nhd_centerlines.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/nhd_centerlines_clipped.geojson", driver="GeoJSON")
    return nhd_centerlines
def filter_nhd_waterbodies(sites_gdf, watersheds):
    #"""gets sites, filters by parameter, gets watersheds and finds intersecting watersheds"""
    # get watersheds
    # Check if file exists

    #if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/nhd_centerlines_clipped.geojson"):
        #print("nhd waterbodies filter exists")
    #    # Load the existing file
    #    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    #    nhd_waterbodies = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/wa_nhd_waterbodies_clipped.geojson")
    
    #else:
        print("import nhd water bodies")
        # Set environment variable and process
        os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
        nhd_waterbodies = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/wa_nhd_waterbodies.geojson")
        # Ensure same CRS
        if nhd_waterbodies.crs != site_watersheds.crs:
            nhd_waterbodies = nhd_waterbodies.to_crs(site_watersheds.crs)
        print("origiona waterbodies")
        print(nhd_waterbodies)

       
        # Clip and save
        # .clip() is easier but this assigns the basin to the new gdf
        nhd_waterbodies = nhd_waterbodies.sjoin(watersheds[['basin', 'geometry']], how="inner", predicate='intersects').drop(columns=['index_right'])
        print("nhd waterbodies join")
        print(nhd_waterbodies)
        #nhd_waterbodies_gdf = nhd_waterbodies_gdf.clip(site_watersheds)
        nhd_waterbodies = nhd_waterbodies[["OBJECTID", "basin", "Elevation", "ReachCode", "geometry"]]
        print(nhd_waterbodies)
        nhd_waterbodies.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/wa_nhd_waterbodies_clipped.geojson", driver="GeoJSON")
        return nhd_waterbodies
def filter_riparian_sun(site_watersheds):
    # https://gis-kingcounty.opendata.arcgis.com/datasets/26b644a6a119428fb27a3165f954ab78_2547/explore?location=47.456010%2C-121.890076%2C10.15
    """gets sites, filters by parameter, gets watersheds and finds intersecting watersheds"""
    # get watersheds
    # Check if file exists

    if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/king_county_fema_floodplain_100yr_area_clipped.geojson"):
        print("Clipped file already exists!")
        # Load the existing file
        clipped_gdf = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/king_county_fema_floodplain_100yr_area_clipped.geojson")
        from shapely.geometry import MultiPoint

        
        
    else:
        # Set environment variable and process
        os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
        full_gdf = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/king_county_fema_floodplain_100yr_area.geojson")
        # Ensure same CRS
        if full_gdf.crs != site_watersheds.crs:
            full_gdf = full_gdf.to_crs(site_watersheds.crs)
        # Clip and save
        clipped_gdf = full_gdf.clip(site_watersheds)
        clipped_gdf.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/king_county_fema_floodplain_100yr_area_clipped.geojson", driver="GeoJSON")
    return clipped_gdf

def filter_cso_points(watersheds, buffer_distance = None):
   #https://gis-kingcounty.opendata.arcgis.com/datasets/a78ebaf964764515a477b11c2bf2c881_2800/explore?location=47.812494%2C-122.264168%2C11.87

    #if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/king_county_fema_floodplain_100yr_area_clipped.geojson"):
    #    print("Clipped file already exists!")
    #    # Load the existing file
    #    clipped_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/cache_render_gis_data/CSO_points_clipped.geojson")
    #    from shapely.geometry import MultiPoint

    #if "CSO_status" in watersheds.columns:
    #    pass
        
    #else:
        # Set environment variable and process
        os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
        full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/cache_render_gis_data/CSO_points.geojson")
        
        # Ensure same CRS
        if full_gdf.crs != watersheds.crs:
            full_gdf = full_gdf.to_crs(watersheds.crs)
        # column managment
        full_gdf.columns = full_gdf.columns.str.replace('OF_', '', regex=False)
        columns_to_drop = ['X_COORD', 'Y_COORD', 'LATITUDE', 'LONGITUDE','OBJECTID', 'DSN']
        full_gdf = full_gdf.drop(columns=columns_to_drop, errors='ignore')
       
        watersheds_proj = watersheds.to_crs('EPSG:2926')  # Washington State Plane North (feet)
        full_gdf_proj = full_gdf.to_crs('EPSG:2926')  # Use same CRS
        full_gdf_proj["CSO_status"] = True
        if not buffer_distance:
            buffer_distance = 0
        
        watersheds_buffered = watersheds_proj.copy()
        watersheds_buffered['geometry'] = watersheds_proj.geometry.buffer(buffer_distance)

        # Perform spatial join
        joined = full_gdf_proj.sjoin(watersheds_buffered, how='left', predicate='within')
        # Perform spatial join to find which points fall within buffered watersheds
        #joined = watersheds.sjoin(full_gdf, how='left', predicate='within')
        joined.loc[joined["CSO_status"].isna(), "CSO_status"] = False
        joined = joined[['basin', 'CSO_status']]
        joined = joined.drop_duplicates(subset = "basin")
        
        watersheds = watersheds.merge(joined, on="basin", how="left")
        watersheds.loc[watersheds["CSO_status"].isna(), "CSO_status"] = False
        
        # Set CSO_status to True for points that fall within any watershed
        #

        # Copy the CSO_status column back to the original full_gdf
       
        full_gdf.to_file("C:/Users/IHiggins/OneDrive - King County/cache_render_gis_data/CSO_points_clipped.geojson", driver="GeoJSON")
        #print(clipped_gdf)
        return full_gdf, watersheds

def wtd_service_area(watersheds):
    # https://gis-kingcounty.opendata.arcgis.com/datasets/7da451dd786c4e05a75f568483f87880_2478/explore?location=47.524357%2C-122.101020%2C10.05
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/cache_render_gis_data/WTD_service_area.geojson")
        
    # Ensure same CRS
    if full_gdf.crs != watersheds.crs:
        full_gdf = full_gdf.to_crs(watersheds.crs)
    clipped = watersheds.clip(full_gdf)
    clipped["wtd_service_area"] = True
    clipped = clipped[["basin", "wtd_service_area"]]
    watersheds = watersheds.merge(clipped, on="basin", how="left")
    watersheds.loc[watersheds["wtd_service_area"].isna(), "wtd_service_area"] = False
   
    return full_gdf, watersheds

def filter_census_data(sites_gdf, watersheds):
    """filter census tracks by basin, return census tract with basin"""
    #https://gis-kingcounty.opendata.arcgis.com/datasets/26b644a6a119428fb27a3165f954ab78_2547/explore?location=47.456010%2C-121.890076%2C10.15
    #"""gets sites, filters by parameter, gets watersheds and finds intersecting watersheds"""
    #"""uses environmental health for census info so a bit redundent but hopefully this makes codee more useable for expansion"""
    #if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/census_clipped.geojson"):
    #    print("census clip eixits")
    #    # Load the existing file
    #    clipped_gdf = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/census_clipped.geojson")
    #else:
        # Set environment variable and process
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/EHD.geojson")
    #print("creating census clip")
        # Ensure same CRS
    #if full_gdf.crs != watersheds.crs:
    full_gdf = full_gdf.to_crs("EPSG:4326")
    
    
    clipped_gdf = full_gdf.overlay(watersheds[['basin', 'geometry']], how='intersection')
        
    clipped_gdf = clipped_gdf[['TRACTCE10', 'GEOID10', 'geometry', 'basin']]
    clipped_gdf = clipped_gdf.explode(index_parts=False).reset_index(drop=True) # if a census track is bisected by a watershed you wanna create two tracts
    
    clipped_gdf.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/census_clipped.geojson", driver="GeoJSON")
    return clipped_gdf
     
def crop_census_data(census_gdf, site_watersheds):  
    """crops census data to site watersheds"""
    # Clip to watershed boundaries
    full_gdf = census_gdf.copy()
    
    full_gdf = full_gdf.drop(columns=['basin'], errors='ignore')

    # Now overlay will only have one basin column (from site_watersheds)
    clipped_gdf = full_gdf.overlay(site_watersheds[['basin', 'geometry']], how='intersection')
    clipped_gdf = clipped_gdf.explode(index_parts=False).reset_index(drop=True)
    #clipped_gdf = full_gdf.overlay(site_watersheds[['basin', 'geometry']], how='intersection')
    #clipped_gdf = clipped_gdf.explode(index_parts=False).reset_index(drop=True)
   
    clipped_gdf.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/census_site_watersheds.geojson", driver="GeoJSON")
    return clipped_gdf 

def filter_environmental_health(sites_gdf, watersheds, census_gdf):

    # shorter version
    # https://geo.wa.gov/datasets/c2c929f4bf0046aa814648823ccb6206_0/explore?location=47.224740%2C-120.811974%2C7.54
    # https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/Environmental_Effects/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
    
    # full version havent figured out how to query the ehd 
    # https://geo.wa.gov/datasets/WADOH::full-environmental-health-disparities-version-2-extract/about
    # https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/EHD_Combined_V2/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
    # report
    #https://deohs.washington.edu/washington-environmental-health-disparities-map-project
    # load EHD data
    #if "Environmental_Health_Disparities" in sites_gdf.columns:
    #    print("environmental statistcs present")
    #    return sites_gdf, site_watersheds, census_gdf
    #else:
    print("calculating environmental statistics")
        ## clip census tracks to EHD data
    ehd_data = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/EHD.geojson")
    censsu_gdf = census_gdf.reset_index(drop = False)

    # set crs
    ehd_data = ehd_data.to_crs("EPSG:4326")
    census_gdf = census_gdf .to_crs("EPSG:4326")
    ehd_data = ehd_data.clip(watersheds)
    
    census_gdf = census_gdf.merge(ehd_data, how="left", on = "TRACTCE10", suffixes=('', '_ehd'))

    census_gdf = census_gdf.drop(columns=census_gdf.filter(regex='_ehd$').columns)
    census_gdf = census_gdf.drop(columns=census_gdf.filter(regex='Rank$').columns)
    census_gdf = census_gdf.drop(columns=['CountyFIPS10','County10', 'Proximity_to_Heavy_Traffic_Road', 'Transportation_Expense',])
    census_gdf = census_gdf.rename(columns={'Environmental_Health_Disparitie': 'Environmental_Health_Disparities'})
    census_gdf = census_gdf.rename(columns={'Socioeconomic_Factors_Theme_Ran': 'Socioeconomic_Factors_Theme'})
    census_gdf = census_gdf.rename(columns={'Environmental_Effects_Theme_Ran': 'Environmental_Effects_Theme'})
    census_gdf = census_gdf.rename(columns={'Environmental_Exposures_Theme_R': 'Environmental_Exposures_Theme'})
    census_gdf = census_gdf.rename(columns={'Toxic_Release_from_Facilities__': 'Toxic_Release_from_Facilities'})
    census_gdf = census_gdf.rename(columns={'Proximity_to_Heavy_Traffic_Ro_1': 'Proximity_to_Heavy_Traffic'})
    census_gdf = census_gdf.rename(columns={'Sensitive_Populations_Theme_Ran': 'Sensitive_Populations_Theme'})
    
    
    # this will create duplicate census tracts when a watershed spans census trackts but I think thats okay for now
    #ehd_with_watersheds = gpd.sjoin(census_gdf, watersheds['geometry'], how='left', predicate='intersects')
    #ehd_with_watersheds = ehd_with_watersheds.drop(columns = ['basin_left', 'OBJECTID_right', 'index_right'])
    #print("ehd with watersheds")
    #print(ehd_with_watersheds)
    #print(ehd_with_watersheds.columns)
        # Calculate statistics for each watershed
    """statistics_list = ['Diesel_PM2_5_Emissions', 'Ozone_Concentration', 'PM2_5',
        'Proximity_to_Heavy_Traffic_Ro_1', 'Toxic_Release_from_Facilities__',
        'Lead_Risk_from_Housing', 'PTSDFs', 'PNPL', 'PRMP', 'PWDIS', 'LEP',
        'No_HS_Diploma', 'POC', 'Poverty', 'Unaffordable_Housing', 'Unemployed',
        'CVD', 'LBW', 'Environmental_Exposures_Theme_R',
        'Environmental_Effects_Theme_Ran', 'Socioeconomic_Factors_Theme_Ran',
        'Sensitive_Populations_Theme_Ran', 'Environmental_Health_Disparities']"""

    statistics_list = ['Diesel_PM2_5_Emissions', 'Ozone_Concentration', 'PM2_5',
        'Proximity_to_Heavy_Traffic', 'Toxic_Release_from_Facilities','PTSDFs', 'PNPL', 'PRMP', 'PWDIS', 'LEP',
        'POC', 'Poverty', 
        'CVD', 'LBW', 'Environmental_Exposures_Theme',
        'Environmental_Effects_Theme', 'Socioeconomic_Factors_Theme', 'Environmental_Health_Disparities']

        # PTSDFs = proximity to Proximity to Hazardous Waste Treatment Storage and Disposal Facilities
        # PNPL = Proximity to National Priorities List Facilities (Superfund Sites)
        # PRMP = Proximity to Risk Management Plan
        # PWDIS = Proximity to Wastewater discharge

        # calculate average for each in EHD map and add to watersheds and site lsit\
    
    for s in statistics_list:
        stat = census_gdf.groupby('basin')[f'{s}'].agg(['mean']).round(1)
        stat.columns = [f'{s}']
            # Merge back to watersheds

        watersheds = watersheds.merge(stat, left_on = "basin", right_index = True, how='left')
        sites_gdf = sites_gdf.merge(stat, left_on = "basin", right_index = True, how = "left")

    return sites_gdf, watersheds, census_gdf

def filter_watersheds(sites_gdf, watersheds):
    #if os.path.exists("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/site_watersheds.geojson"):
    #    print("site watersheds exists")
        # Load the existing file
    #    site_watersheds = gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/site_watersheds.geojson")
    #else:
    #    print("clipping site watersheds")
   
    site_watersheds = watersheds.loc[watersheds.sjoin(sites_gdf, how="inner", predicate='intersects').index.unique()]
        # Save to file
    site_watersheds.to_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/site_watersheds.geojson", driver="GeoJSON")
   
    return site_watersheds

def filter_percent_pov(site_watersheds):
        # adds average ehd rank to site_watersheds and clips ppov to watershed boundaries
    """Calculate average environmental health rank for each watershed"""
            
    # get environmental health data
    geojson_data = fetch_ppov_geojson()
    if not geojson_data:
        print("Failed to fetch environmental health data")
        return site_watersheds  # return original watersheds
            
    # Convert to GeoDataFrame
    ppov_gdf = gpd.GeoDataFrame.from_features(geojson_data['features'], crs='EPSG:4326')
            
    # Ensure same CRS
    if ppov_gdf.crs != site_watersheds.crs:
        ppov_gdf = ppov_gdf.to_crs(site_watersheds.crs)

    # CLIP the poverty data to the watershed boundaries (this trims the geometries)
    ppov_clipped = ppov_gdf.clip(site_watersheds)

    # Spatial join to assign each clipped poverty polygon to a watershed
    ppov_with_watersheds = gpd.sjoin(ppov_clipped, site_watersheds, how='left', predicate='intersects')

    # Calculate statistics for each watershed
    watershed_stats = ppov_with_watersheds.groupby('basin')['Percent_Living_in_Poverty'].agg([
        'mean', 'median', 'std', 'count', 'min', 'max'
    ])
    watershed_stats = watershed_stats.round(2)
    watershed_stats.columns = ['avg_ppov', 'median_ppov', 'std_ppov', 'ppov_count', 'min_ppov', 'max_ppov']
    watershed_stats = watershed_stats[["avg_ppov"]]

    # Merge stats back to watersheds
    site_watersheds = site_watersheds.merge(
        watershed_stats, 
        left_on='basin', 
        right_index=True, 
        how='left'
    )

    # Return watersheds with stats and the clipped poverty data
    return site_watersheds, ppov_clipped

def create_map(sites_gdf, watersheds, site_watersheds, census_gdf, cao_gdf = None, cso_gdf = None, wtd_service_area = None, nhd_centerlines = None, nhd_waterbodies = None):
    import folium
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    import matplotlib.colors as mcolors

    # assign color values
    def get_color_from_value(value, min_val, max_val, colormap='YlOrRd'):
        """Get hex color from value using matplotlib colormap"""
        if max_val == min_val:
            normalized = 0.5
        else:
            normalized = (value - min_val) / (max_val - min_val)
        
        cmap = plt.get_cmap(colormap)
        rgb = cmap(normalized)[:3]
        return mcolors.rgb2hex(rgb)

    # Get bounds
    bounds = sites_gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

  
    # create base map
    try:
        # Try to create map with aerial imagery
        m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=10,
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        zoom_control=True,
        scrollWheelZoom=True
    )

        # Add a rectangle showing your bounds for context
        folium.Rectangle(
            bounds=[[bounds[1], bounds[0]], [bounds[3], bounds[2]]],
            color='gray',
            fill=True,
            fillOpacity=0.1).add_to(m)
    except:
        # Create map with no tiles initially
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=10,
            tiles=None,
            zoom_control=True,
            scrollWheelZoom=True
        )
        
        #tiles_group = folium.FeatureGroup(name="Basemaps")
        folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Esri',
            overlay=True,  # Change to True when inside FeatureGroup
            control=False
        )#.add_to(tiles_group)
        #tiles_group.add_to(m)

        """# Optionally add other basemaps
        folium.TileLayer(
            tiles='OpenStreetMap',
            name='Street Map',
            overlay=False,
            control=True
        ).add_to(m)"""

        # Add all your other layers here...

        # Add layer control at the end
        #folium.LayerControl().add_to(m)

    # Add a rectangle showing your bounds for context
    """folium.Rectangle(
        bounds=[[bounds[1], bounds[0]], [bounds[3], bounds[2]]],
        color='gray',
        fill=True,
        fillOpacity=0.1).add_to(m)"""


    # Add sites as a named layer
    if not sites_gdf.empty:
            sites_layer = folium.FeatureGroup(name='Sites', show=True)
            
            for idx, row in sites_gdf.iterrows():
                site = row['site']
                basin = row['basin'] if 'basin' in row else 'N/A'  # Handle case where basin might not exist
                
                folium.CircleMarker(
                    location=[row.geometry.y, row.geometry.x],
                    radius=3,
                    popup=f"Site: {site}<br>Basin: {basin}",
                    tooltip=f"Site: {site}<br>Basin: {basin}",
                    color='black', fillColor='black',
                    fillOpacity=0.8,
                    weight=1
                ).add_to(sites_layer)
            
            sites_layer.add_to(m)
    
    # Add site watersheds as a named layer
    if not site_watersheds.empty:
        site_watersheds_layer = folium.FeatureGroup(name='Site Watersheds', show=True)
        folium.GeoJson(
            site_watersheds,
            style_function=lambda x: {
                'fillColor': 'lightblue',
                'color': 'black',
                'weight': .5,
                'fillOpacity': 0.3,
            },
            tooltip=folium.GeoJsonTooltip(fields=['basin'], aliases=['Basin:'])
        ).add_to(site_watersheds_layer)
        site_watersheds_layer.add_to(m)


     # Add all watersheds as a named layer
    if not watersheds.empty:
        watersheds_layer = folium.FeatureGroup(name='All Watersheds', show=False)
        folium.GeoJson(
            watersheds,
            style_function=lambda x: {
                'fillColor': 'lightblue',
                'color': 'black',
                'weight': .6,
                'fillOpacity': 0.3,},
        tooltip=folium.GeoJsonTooltip(fields=['basin'], aliases=['Basin:']),
        ).add_to(watersheds_layer)
        watersheds_layer.add_to(m)
    # add ndh centerlines
    # Create a single feature group for all streams
    if nhd_centerlines is not None and not nhd_centerlines.empty:
    # Create a single feature group for all streams
       
        # Create a single feature group for all streams
        streams_layer = folium.FeatureGroup(name='NHD Streams')

        for idx, row in nhd_centerlines.iterrows():
         
            # bases stream weight (line width) on stream order
            stream_order = float(row['StreamOrder']) if pd.notna(row['StreamOrder']) else 1
            weight = np.log1p(stream_order)/1.5 #* 2  # log1p is log(1+x), multiply by 2 for visibility
            wight = max(0.25, weight) # handles log(1) = 0 and really small values that wouldnt be visable
            weight = min(0.45, weight)

            folium.GeoJson(
                row['geometry'],
                style_function=lambda x, w=weight: {  # Capture weight with default parameter
                    'color': 'blue',
                    'weight': w,
                    'opacity': 0.7},
                tooltip=f"Stream: {row.get('GNIS_Name', 'Unnamed')}<br>Order: {row.get('StreamOrder', 'N/A')}<br>Basin: {row.get('basin', 'N/A')}"
            ).add_to(streams_layer)

        streams_layer.add_to(m)
    if nhd_waterbodies is not None and not nhd_waterbodies.empty:
    # Create a single feature group for all streams
       
        # Create a single feature group for all streams
        waterbodies_layer = folium.FeatureGroup(name='NHD Waterbodies')

        for idx, row in nhd_waterbodies.iterrows():
         
            """# bases stream weight (line width) on stream order
            stream_order = float(row['StreamOrder']) if pd.notna(row['StreamOrder']) else 1
            weight = np.log1p(stream_order)/1.5 #* 2  # log1p is log(1+x), multiply by 2 for visibility
            wight = max(0.25, weight) # handles log(1) = 0 and really small values that wouldnt be visable
            weight = min(0.45, weight)
            """
            folium.GeoJson(
                row['geometry'],
                style_function=lambda x, w=weight: {  # Capture weight with default parameter
                    'color': 'blue',
                    'weight': 1,
                    'opacity': 0.7},
                #tooltip=f"Stream: {row.get('GNIS_Name', 'Unnamed')}<br>Order: {row.get('StreamOrder', 'N/A')}<br>Basin: {row.get('basin', 'N/A')}"
            ).add_to(waterbodies_layer)

        waterbodies_layer.add_to(m)
     # CAO data as a named layer
    if cao_gdf is not None and not cao_gdf.empty:
        cao_layer = folium.FeatureGroup(name='CAO Data', show=False)
        folium.GeoJson(
            cao_gdf,
            style_function=lambda feature: {
                'fillColor': 'yellow',
                'color': 'black',
                'weight': 0.5,
                'fillOpacity': 0.5,
            }
        ).add_to(cao_layer)
        cao_layer.add_to(m)
   # CSO locations
   # CSO locations
    if "CSO_status" in watersheds.columns:
        # Filter watersheds with CSO status
        cso_watersheds = watersheds[watersheds["CSO_status"] == True]
        # Create layer for CSO watersheds
        cso_watershed_layer = folium.FeatureGroup(name='CSO Watersheds', show=False)
        
        for idx, row in cso_watersheds.iterrows():
            folium.GeoJson(
                row.geometry,
                style_function=lambda x: {
                    'fillColor': 'orange',
                    'color': 'darkorange',
                    'weight': 2,
                    'fillOpacity': 0.3
                },
                popup = folium.Popup(f"<b>{row.get('basin', 'N/A')}</b><br>"
                     f"CSO present<br>", 
                     max_width=200)
            ).add_to(cso_watershed_layer)
        
        cso_watershed_layer.add_to(m)

    if cso_gdf is not None and not cso_gdf.empty:
        cso_layer = folium.FeatureGroup(name='CSO Points', show=False)
        # Filter out rows with missing geometry
        cso_valid = cso_gdf[cso_gdf.geometry.notna()]
        for idx, row in cso_valid.iterrows():
            # Extract coordinates from geometry (Point object)
            lon, lat = row.geometry.x, row.geometry.y
            
            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                popup = folium.Popup(f"<b>Combined Sewer Overflow (CSO)</b><br>"
                     f"Label: {row.get('LABEL', 'N/A')}<br>"
                     f"Status: {row.get('STATUS', 'N/A')}<br>"
                     f"Owner: {row.get('OWNER', 'N/A')}",
                     max_width=200),
                color='darkorange',
                fillColor='darkorange',
                fillOpacity=0.7,
                weight=2
            ).add_to(cso_layer)
        
        cso_layer.add_to(m)
    ### wtd service area
    if wtd_service_area is not None:
        # Create a feature group for the wtd_service_area layer
        wtd_layer = folium.FeatureGroup(name='WTD Service Area')
        
        for idx, row in wtd_service_area.iterrows():
            folium.GeoJson(
                row['geometry'],
                style_function=lambda x: {
                    'fillColor': 'transparent',
                    'color': '#B7410E',  # Rust-orange
                    'weight': 2,
                    'dashArray': '5, 5',  # Dotted line
                    'fillOpacity': 0
                },
                tooltip="wtd_service_area"
            ).add_to(wtd_layer)
        
        # Add the feature group to the map
        wtd_layer.add_to(m)
        
    if "wtd_service_area" in watersheds.columns:
        # Filter watersheds with CSO status
        wtd_watersheds = watersheds[watersheds["wtd_service_area"] == True]
        # Create layer for CSO watersheds
        wtd_watersheds_layer = folium.FeatureGroup(name='WTD Watersheds', show=False)
        
        for idx, row in wtd_watersheds.iterrows():
            folium.GeoJson(
                row.geometry,
                style_function=lambda x: {
                    'fillColor': '#B7410E',
                    'color': 'darkorange',
                    'weight': 2,
                    'fillOpacity': 0.3
                },
                popup = folium.Popup(f"<b>{row.get('basin', 'N/A')}</b><br>"
                     f"Within WTD service area<br>", 
                     max_width=200)
            ).add_to(wtd_watersheds_layer)
        
        wtd_watersheds_layer.add_to(m)     
    # Define census tract themes
    census_themes = {
        'Socioeconomic_Factors_Theme': {
            'name': 'Census Tracks - Socioeconomic Factors',
            'label': 'Socioeconomic Factors'
        },
        'Sensitive_Populations_Theme': {
            'name': 'Census Tracks - Sensitive Populations',
            'label': 'Sensitive Populations'
        },
        'Environmental_Health_Disparities': {
            'name': 'Census Tracks - Environmental Health Disparities',
            'label': 'Environmental Health Disparities'
        }
    }

    # Add census tract layers with different themes
    if census_gdf is not None and not census_gdf.empty:
        for col, config in census_themes.items():
            if col not in census_gdf.columns:
                print(f"Warning: Column {col} not found in census_gdf")
                continue
            
            census_layer = folium.FeatureGroup(name=config['name'], show=False)
            
            # Get min/max for color scaling
            min_val = census_gdf[col].min()
            max_val = census_gdf[col].max()
           
            # Add each census tract with color based on value
            for idx, row in census_gdf.iterrows():
                
                value = row[col]
                color = get_color_from_value(value, min_val, max_val, 'YlOrRd')
                
                folium.GeoJson(
                    row.geometry,
                    style_function=lambda x, color=color: {
                        'fillColor': color,
                        'color': 'black',
                        'weight': 0.5,
                        'fillOpacity': 0.6,
                    },
                    tooltip=f"{config['label']}: {value:.2f}"
                ).add_to(census_layer)
            
            census_layer.add_to(m)
    
    # Watershed condition data processing
    site_watersheds.loc[site_watersheds["environmental_condition"] == "High", "environmental_condition"] = 1
    site_watersheds.loc[site_watersheds["environmental_condition"] == "Medium", "environmental_condition"] = 2
    site_watersheds.loc[site_watersheds["environmental_condition"] == "Low", "environmental_condition"] = 3

    # Define your themes
    themes = {
        'environmental_condition': {'name': 'Environmental Condition', 'colormap': 'YlGn_r'},
        'Proximity_to_Heavy_Traffic': {'name': 'Proximity to Heavy Traffic', 'colormap': 'Reds'},
        'Environmental_Exposures_Theme': {'name': 'Environmental Exposure', 'colormap': 'Oranges'},
        'Environmental_Effects_Theme': {'name': 'Environmental Effects', 'colormap': 'YlOrRd'},
        'Socioeconomic_Factors_Theme': {'name': 'Socioeconomic Factors', 'colormap': 'Purples'},
        'Environmental_Health_Disparities': {'name': 'Environmental Health Disparities Score', 'colormap': 'YlGnBu'},
    }

    # Add site watersheds with theme layers
    if not site_watersheds.empty:
        for col, config in themes.items():
            if col not in site_watersheds.columns:
                print(f"Warning: Column {col} not found in site_watersheds")
                continue
            
            # Create feature group for this theme with proper name
            fg = folium.FeatureGroup(name=config['name'], show=False)
            
            # Get min/max for color scaling
            min_val = site_watersheds[col].min()
            max_val = site_watersheds[col].max()
            
            # Add each watershed polygon
            for idx, row in site_watersheds.iterrows():
                value = row[col]
                basin = row['basin']
                color = get_color_from_value(value, min_val, max_val, config['colormap'])
                
                folium.GeoJson(
                    row.geometry,
                    style_function=lambda x, color=color: {
                        'fillColor': color,
                        'color': 'black',
                        'weight': 1,
                        'fillOpacity': 0.6,},
                    tooltip=f"Basin: {basin}<br>{config['name']}: {value:.2f}").add_to(fg)
            fg.add_to(m)

    

   
 
    # Add layer control at the end
    folium.LayerControl(position='topright', collapsed=False).add_to(m)
    
    # sites
    
    return m
def create_map_plotly(sites_gdf, watersheds, site_watersheds, census_gdf, cao_gdf=None, cso_gdf=None, 
               wtd_service_area=None, nhd_centerlines=None, nhd_waterbodies=None):
    import plotly.graph_objects as go
    import pandas as pd
    import numpy as np
    from plotly.subplots import make_subplots
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    import matplotlib.colors as mcolors

    def get_color_from_value(value, min_val, max_val, colormap='YlOrRd'):
        """Get hex color from value using matplotlib colormap"""
        if max_val == min_val:
            normalized = 0.5
        else:
            normalized = (value - min_val) / (max_val - min_val)
        
        cmap = plt.get_cmap(colormap)
        rgb = cmap(normalized)[:3]
        return mcolors.rgb2hex(rgb)

    # Get bounds for centering
    bounds = sites_gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    # Create figure
    fig = go.Figure()

    # Watershed condition data processing
    site_watersheds = site_watersheds.copy()
    site_watersheds.loc[site_watersheds["environmental_condition"] == "High", "environmental_condition"] = 1
    site_watersheds.loc[site_watersheds["environmental_condition"] == "Medium", "environmental_condition"] = 2
    site_watersheds.loc[site_watersheds["environmental_condition"] == "Low", "environmental_condition"] = 3

    # Define themes for site watersheds
    themes = {
        'environmental_condition': {'name': 'Environmental Condition', 'colormap': 'YlGn_r'},
        'Proximity_to_Heavy_Traffic': {'name': 'Proximity to Heavy Traffic', 'colormap': 'Reds'},
        'Environmental_Exposures_Theme': {'name': 'Environmental Exposure', 'colormap': 'Oranges'},
        'Environmental_Effects_Theme': {'name': 'Environmental Effects', 'colormap': 'YlOrRd'},
        'Socioeconomic_Factors_Theme': {'name': 'Socioeconomic Factors', 'colormap': 'Purples'},
        'Environmental_Health_Disparities': {'name': 'Environmental Health Disparities Score', 'colormap': 'YlGnBu'},
    }

    # Add site watersheds with theme layers
    if not site_watersheds.empty:
        for col, config in themes.items():
            if col not in site_watersheds.columns:
                print(f"Warning: Column {col} not found in site_watersheds")
                continue
            
            min_val = site_watersheds[col].min()
            max_val = site_watersheds[col].max()
            
            for idx, row in site_watersheds.iterrows():
                value = row[col]
                basin = row['basin']
                color = get_color_from_value(value, min_val, max_val, config['colormap'])
                
                # Extract coordinates from geometry
                if row.geometry.geom_type == 'Polygon':
                    x, y = row.geometry.exterior.xy
                    lons, lats = list(x), list(y)
                elif row.geometry.geom_type == 'MultiPolygon':
                    lons, lats = [], []
                    for polygon in row.geometry.geoms:
                        x, y = polygon.exterior.xy
                        lons.extend(list(x) + [None])
                        lats.extend(list(y) + [None])
                else:
                    continue
                
                fig.add_trace(go.Scattermapbox(
                    lon=lons,
                    lat=lats,
                    mode='lines',
                    fill='toself',
                    fillcolor=color,
                    line=dict(color='black', width=1),
                    opacity=0.6,
                    name=config['name'],
                    legendgroup=config['name'],
                    showlegend=(idx == site_watersheds.index[0]),
                    hovertext=f"Basin: {basin}<br>{config['name']}: {value:.2f}",
                    hoverinfo='text',
                    visible=False
                ))

    # Census tract themes
    census_themes = {
        'Socioeconomic_Factors_Theme': {
            'name': 'Census Tracks - Socioeconomic Factors',
            'label': 'Socioeconomic Factors'
        },
        'Sensitive_Populations_Theme': {
            'name': 'Census Tracks - Sensitive Populations',
            'label': 'Sensitive Populations'
        },
        'Environmental_Health_Disparities': {
            'name': 'Census Tracks - Environmental Health Disparities',
            'label': 'Environmental Health Disparities'
        }
    }

    # Add census tract layers
    if census_gdf is not None and not census_gdf.empty:
        for col, config in census_themes.items():
            if col not in census_gdf.columns:
                print(f"Warning: Column {col} not found in census_gdf")
                continue
            
            min_val = census_gdf[col].min()
            max_val = census_gdf[col].max()
            
            for idx, row in census_gdf.iterrows():
                value = row[col]
                color = get_color_from_value(value, min_val, max_val, 'YlOrRd')
                
                if row.geometry.geom_type == 'Polygon':
                    x, y = row.geometry.exterior.xy
                    lons, lats = list(x), list(y)
                elif row.geometry.geom_type == 'MultiPolygon':
                    lons, lats = [], []
                    for polygon in row.geometry.geoms:
                        x, y = polygon.exterior.xy
                        lons.extend(list(x) + [None])
                        lats.extend(list(y) + [None])
                else:
                    continue
                
                fig.add_trace(go.Scattermapbox(
                    lon=lons,
                    lat=lats,
                    mode='lines',
                    fill='toself',
                    fillcolor=color,
                    line=dict(color='black', width=0.5),
                    opacity=0.6,
                    name=config['name'],
                    legendgroup=config['name'],
                    showlegend=(idx == census_gdf.index[0]),
                    hovertext=f"{config['label']}: {value:.2f}",
                    hoverinfo='text',
                    visible=False
                ))

    # Add WTD watersheds
    if "wtd_service_area" in watersheds.columns:
        wtd_watersheds = watersheds[watersheds["wtd_service_area"] == True]
        
        for idx, row in wtd_watersheds.iterrows():
            if row.geometry.geom_type == 'Polygon':
                x, y = row.geometry.exterior.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiPolygon':
                lons, lats = [], []
                for polygon in row.geometry.geoms:
                    x, y = polygon.exterior.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                fill='toself',
                fillcolor='#B7410E',
                line=dict(color='darkorange', width=2),
                opacity=0.3,
                name='WTD Watersheds',
                legendgroup='WTD Watersheds',
                showlegend=(idx == wtd_watersheds.index[0]),
                hovertext=f"Basin: {row.get('basin', 'N/A')}<br>Within WTD service area",
                hoverinfo='text',
                visible=False
            ))

    # Add WTD service area boundary
    if wtd_service_area is not None:
        for idx, row in wtd_service_area.iterrows():
            if row.geometry.geom_type == 'Polygon':
                x, y = row.geometry.exterior.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiPolygon':
                lons, lats = [], []
                for polygon in row.geometry.geoms:
                    x, y = polygon.exterior.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                line=dict(color='#B7410E', width=2, dash='dash'),
                name='WTD Service Area',
                legendgroup='WTD Service Area',
                showlegend=(idx == wtd_service_area.index[0]),
                hovertext="WTD Service Area",
                hoverinfo='text',
                visible=True
            ))

    # Add CSO watersheds
    if "CSO_status" in watersheds.columns:
        cso_watersheds = watersheds[watersheds["CSO_status"] == True]
        
        for idx, row in cso_watersheds.iterrows():
            if row.geometry.geom_type == 'Polygon':
                x, y = row.geometry.exterior.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiPolygon':
                lons, lats = [], []
                for polygon in row.geometry.geoms:
                    x, y = polygon.exterior.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                fill='toself',
                fillcolor='orange',
                line=dict(color='darkorange', width=2),
                opacity=0.3,
                name='CSO Watersheds',
                legendgroup='CSO Watersheds',
                showlegend=(idx == cso_watersheds.index[0]),
                hovertext=f"Basin: {row.get('basin', 'N/A')}<br>CSO present",
                hoverinfo='text',
                visible=False
            ))

    # Add CSO points
    if cso_gdf is not None and not cso_gdf.empty:
        cso_valid = cso_gdf[cso_gdf.geometry.notna()]
        if not cso_valid.empty:
            lons = [geom.x for geom in cso_valid.geometry]
            lats = [geom.y for geom in cso_valid.geometry]
            hover_texts = [f"<b>Combined Sewer Overflow (CSO)</b><br>"
                          f"Label: {row.get('LABEL', 'N/A')}<br>"
                          f"Status: {row.get('STATUS', 'N/A')}<br>"
                          f"Owner: {row.get('OWNER', 'N/A')}"
                          for _, row in cso_valid.iterrows()]
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='markers',
                marker=dict(size=8, color='darkorange'),
                name='CSO Points',
                hovertext=hover_texts,
                hoverinfo='text',
                visible=False
            ))

    # Add CAO data
    if cao_gdf is not None and not cao_gdf.empty:
        for idx, row in cao_gdf.iterrows():
            if row.geometry.geom_type == 'Polygon':
                x, y = row.geometry.exterior.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiPolygon':
                lons, lats = [], []
                for polygon in row.geometry.geoms:
                    x, y = polygon.exterior.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                fill='toself',
                fillcolor='yellow',
                line=dict(color='black', width=0.5),
                opacity=0.5,
                name='CAO Data',
                legendgroup='CAO Data',
                showlegend=(idx == cao_gdf.index[0]),
                hoverinfo='text',
                visible=False
            ))

    # Add NHD waterbodies
    if nhd_waterbodies is not None and not nhd_waterbodies.empty:
        for idx, row in nhd_waterbodies.iterrows():
            if row.geometry.geom_type == 'Polygon':
                x, y = row.geometry.exterior.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiPolygon':
                lons, lats = [], []
                for polygon in row.geometry.geoms:
                    x, y = polygon.exterior.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                fill='toself',
                fillcolor='blue',
                line=dict(color='blue', width=1),
                opacity=0.7,
                name='NHD Waterbodies',
                legendgroup='NHD Waterbodies',
                showlegend=(idx == nhd_waterbodies.index[0]),
                hoverinfo='text',
                visible=True
            ))

    # Add NHD centerlines (streams)
    if nhd_centerlines is not None and not nhd_centerlines.empty:
        for idx, row in nhd_centerlines.iterrows():
            stream_order = float(row['StreamOrder']) if pd.notna(row['StreamOrder']) else 1
            weight = np.log1p(stream_order) / 1.5
            weight = max(0.25, min(0.45, weight))
            
            if row.geometry.geom_type == 'LineString':
                x, y = row.geometry.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiLineString':
                lons, lats = [], []
                for line in row.geometry.geoms:
                    x, y = line.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                line=dict(color='blue', width=weight),
                opacity=0.7,
                name='NHD Streams',
                legendgroup='NHD Streams',
                showlegend=(idx == nhd_centerlines.index[0]),
                hovertext=f"Stream: {row.get('GNIS_Name', 'Unnamed')}<br>"
                         f"Order: {row.get('StreamOrder', 'N/A')}<br>"
                         f"Basin: {row.get('basin', 'N/A')}",
                hoverinfo='text',
                visible=True
            ))

    # Add all watersheds
    if not watersheds.empty:
        for idx, row in watersheds.iterrows():
            if row.geometry.geom_type == 'Polygon':
                x, y = row.geometry.exterior.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiPolygon':
                lons, lats = [], []
                for polygon in row.geometry.geoms:
                    x, y = polygon.exterior.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                fill='toself',
                fillcolor='lightblue',
                line=dict(color='black', width=0.6),
                opacity=0.3,
                name='All Watersheds',
                legendgroup='All Watersheds',
                showlegend=(idx == watersheds.index[0]),
                hovertext=f"Basin: {row['basin']}",
                hoverinfo='text',
                visible=False
            ))

    # Add site watersheds
    if not site_watersheds.empty:
        for idx, row in site_watersheds.iterrows():
            if row.geometry.geom_type == 'Polygon':
                x, y = row.geometry.exterior.xy
                lons, lats = list(x), list(y)
            elif row.geometry.geom_type == 'MultiPolygon':
                lons, lats = [], []
                for polygon in row.geometry.geoms:
                    x, y = polygon.exterior.xy
                    lons.extend(list(x) + [None])
                    lats.extend(list(y) + [None])
            else:
                continue
            
            fig.add_trace(go.Scattermapbox(
                lon=lons,
                lat=lats,
                mode='lines',
                fill='toself',
                fillcolor='lightblue',
                line=dict(color='black', width=0.5),
                opacity=0.3,
                name='Site Watersheds',
                legendgroup='Site Watersheds',
                showlegend=(idx == site_watersheds.index[0]),
                hovertext=f"Basin: {row['basin']}",
                hoverinfo='text',
                visible=True
            ))

    # Add sites (always on top)
    if not sites_gdf.empty:
        lons = [geom.x for geom in sites_gdf.geometry]
        lats = [geom.y for geom in sites_gdf.geometry]
        hover_texts = [f"Site: {row['site']}<br>Basin: {row.get('basin', 'N/A')}" 
                      for _, row in sites_gdf.iterrows()]
        
        fig.add_trace(go.Scattermapbox(
            lon=lons,
            lat=lats,
            mode='markers',
            marker=dict(size=6, color='black'),
            name='Sites',
            hovertext=hover_texts,
            hoverinfo='text',
            visible=True
        ))

    # Update layout
    fig.update_layout(
        mapbox=dict(
            style='satellite',
            center=dict(lat=center_lat, lon=center_lon),
            zoom=10
        ),
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99,
            bgcolor="rgba(255, 255, 255, 0.8)"
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        height=800
    )

    return fig
if __name__ == '__main__':
    # You'll need to implement get_table_data() or replace it with your data loading method
    #result = main()
    # import sites, filter and process to geodataframe
    # import sites
    # import local sites

    sites_gdf =  gpd.read_file("C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/sites.geojson")
    #sites_gdf = site_import(parameter = "discharge")
    
    # import
    # Process sites with watersheds
    watersheds = watershed_import()
    cso_gdf, watersheds = filter_cso_points(watersheds, buffer_distance = 1000)
    wtd_service_area, watersheds = wtd_service_area(watersheds)

    sites_gdf = site_basin(sites_gdf, watersheds)

    census_gdf = filter_census_data(sites_gdf, watersheds)
    sites_gdf, watersheds, census_gdf = filter_environmental_health(sites_gdf, watersheds, census_gdf)
    census_gdf, watersheds = watershed_condition(sites_gdf, census_gdf, watersheds)
    site_watersheds = filter_watersheds(sites_gdf, watersheds)
    
    
    census_site_watersheds =  crop_census_data(census_gdf, site_watersheds)
    cao_gdf = filter_cao(sites_gdf, site_watersheds)
   
    #nhd_centerlines = filter_nhd_centerlines(watersheds)
    #nhd_waterbodies = filter_nhd_waterbodies(sites_gdf, watersheds)
    
   
    print("map")
    #m = create_map(sites_gdf, watersheds, site_watersheds, census_site_watersheds, cao_gdf, cso_gdf, wtd_service_area, None, None)
    #m.save('C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/watershed_map.html')
    
    fig = create_map_plotly(sites_gdf, watersheds, site_watersheds, census_site_watersheds, cao_gdf, cso_gdf, wtd_service_area, None, None)
    fig.save('C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/WTD_map.html')
    
    #m = create_map(sites_gdf, watersheds, site_watersheds, census_gdf, cao_gdf, cso_gdf, wtd_service_area, nhd_centerlines, nhd_waterbodies)
     # view map
    
    """import matplotlib.pyplot as plt
    import seaborn as sns
    from scipy import stats
    themes = [
       'Environmental_Exposures_Theme', 'Environmental_Effects_Theme',
       'Socioeconomic_Factors_Theme', 'Environmental_Health_Disparities']
    theme_values = [site_watersheds[theme].dropna() for theme in themes]
    f_stat, p_value = stats.f_oneway(*theme_values)

    # Box plot to visualize differences
    df_long = site_watersheds.melt(
        value_vars=themes,
        var_name='Theme', 
        value_name='Score'
    )

    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df_long, x='Theme', y='Score')
    plt.xticks(rotation=45, ha='right')
    plt.title(f'Distribution of Scores by Theme\nANOVA p-value: {p_value:.4f}')
    plt.tight_layout()
    #plt.show()

    print(f"F-statistic: {f_stat:.4f}")
    print(f"P-value: {p_value:.4f}")
    """