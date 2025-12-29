import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import json
import os
import folium
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import base64
from folium.plugins import LocateControl


def site_import(file_path, parameter=None):
    """Import sites from Excel, clean data, and convert to GeoDataFrame"""
    
    sites = pd.read_excel(file_path)
    sites = sites.iloc[:-1]  # Drop last row
   
    # Rename columns
    sites = sites.rename(columns={
        "SITE_CODE": "site", 
        "SITE_NAME": "site_name", 
        "DATE_INSTA": "date installed",
        "LAT": "latitude", 
        "LON": "longitude", 
        "GAGETAG": "parameter",
        "Program supporting": "program",
        "ISP site notes": "notes",
        'Annual equipment cost (Replacement cost of both types of gages Data logger and monitoring sensors=$4100 2025 cost.  10 year lifetime of equipment.  Ammortized yearly replacement cost $410)': "annual equipment cost"
    })
    
    # Standardize parameter names
    parameter_mapping = {
        'Precipitation Gauge(Recording)': 'precipitation',
        'Stream Gauge(Recording with Discharge)': 'discharge',
        'Water Temperature Recorder': 'water_temperature'
    }
    sites["parameter"] = sites["parameter"].replace(parameter_mapping)
    
    # Select relevant columns
    sites = sites[[
        "site", "site_name", "parameter", "latitude", "longitude", 
        "WRIA", "program", "notes", "Yearly Hours", "KM verified", 
        "KM notes", "annual equipment cost", "date installed", "WTD vs SWM"
    ]]
    
    # Validate coordinates exist
    if 'latitude' not in sites.columns or 'longitude' not in sites.columns:
        raise ValueError("Excel file must contain 'latitude' and 'longitude' columns")
    
    # Drop rows with missing coordinates
    sites = sites.dropna(subset=['longitude', 'latitude'])
    
    # Create GeoDataFrame
    site_points = [Point(lon, lat) for lon, lat in zip(sites['longitude'], sites['latitude'])]
    sites_gdf = gpd.GeoDataFrame(sites, geometry=site_points, crs='EPSG:4326')
    return sites_gdf


def wa_trailheads_import():
    """import washington trails"""
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/gis_data/WATrailheads_All.geojson")
    full_gdf = full_gdf.to_crs("EPSG:4326")

    for col in full_gdf.columns:
        if full_gdf[col].dtype == 'datetime64[ns]' or str(full_gdf[col].dtype).startswith('datetime'):
            full_gdf[col] = full_gdf[col].astype(str)
    return full_gdf

def wa_federal_trails_import():
    """import washington trails"""
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/gis_data/WATrails2017_Federal.geojson")
    full_gdf = full_gdf.to_crs("EPSG:4326")
    return full_gdf

def wa_other_trails_import():
    """import washington trails"""
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/gis_data/WATrails2017_Other.geojson")
    full_gdf = full_gdf.to_crs("EPSG:4326")
    return full_gdf

def wa_state_trails_import():
    """import washington trails"""
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/gis_data/WATrails2017_State.geojson")
    full_gdf = full_gdf.to_crs("EPSG:4326")
    return full_gdf



def wa_winter_trails_import():
    """import washington trails"""
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/gis_data/Winter_Rec_Non_Motorized_Trails_-5873392780439031327.geojson")
    full_gdf = full_gdf.to_crs("EPSG:4326")
    return full_gdf

def wa_state_parks_import():
    """import washington trails"""
    os.environ['OGR_GEOJSON_MAX_OBJ_SIZE'] = '0'
    full_gdf = gpd.read_file("C:/Users/IHiggins/OneDrive - King County/gis_data/WA_state_parks.geojson")
    full_gdf = full_gdf.to_crs("EPSG:4326")
    return full_gdf

def basin_import():
    """Import or download watershed basins from King County GIS"""
    cache_path = "C:/Users/ihiggins/OneDrive - King County/cache_render_gis_data/watersheds.geojson"
    
    if os.path.exists(cache_path):
        print("Loading cached watersheds")
        return gpd.read_file(cache_path)
    
    print("Downloading watersheds from King County GIS")
    try:
        geojson_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/enviro___base/MapServer/237/query?outFields=*&where=1%3D1&f=geojson"
        watersheds = gpd.read_file(geojson_url)
        watersheds = watersheds.to_crs('EPSG:4326')
        watersheds = watersheds.drop(columns=["OBJECTID_1", "CONDITION"], errors='ignore')
        watersheds = watersheds.rename(columns={"STUDY_UNIT": "basin"})
        watersheds = watersheds.set_index("OBJECTID")
        watersheds.to_file(cache_path, driver="GeoJSON")
        return watersheds
    except Exception as e:
        print(f"Error fetching watersheds: {e}")
        return None


def add_map_legend(m, layer_name='WTD Sites', show=True):
    """Add legend to map"""
    legend_html = f'''
    <div id="parameter-legend" style="
        position: fixed; bottom: 10px; right: 10px; width: 250px;
        background-color: white; opacity: 0.9; border: 2px solid grey; border-radius: 5px;
        padding: 10px; font-size: 14px; z-index: 9999;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.3); display: none;">
        <h4 style="margin: 0 0 10px 0; font-size: 16px; text-align: center; font-weight: bold;">WTD Sites by Parameter</h4>
        <div style="margin: 5px 0;">
            <span style="display: inline-block; width: 12px; height: 12px; 
                         background-color: #00A5E2; border-radius: 50%; margin-right: 8px;"></span>
            Stream Gage Sites
        </div>
        <div style="margin: 5px 0;">
            <span style="display: inline-block; width: 12px; height: 12px; 
                         background-color: #66c597; border-radius: 50%; margin-right: 8px;"></span>
            Rain Gage Sites
        </div>
        <hr style="border: none; border-top: 3px solid #74737A; margin: 5px 0;">
        <div style="margin: 5px 0;">
            <span style="display: inline-block; width: 20px; height: 0px; 
                        border-top: 3px dashed #AF6D23; margin-right: 8px; vertical-align: middle;"></span>
            WTD Service Area
        </div>
    </div>
    
    <script>
        function toggleLegend() {{
            var legend = document.getElementById('parameter-legend');
            var layerControl = document.querySelector('.leaflet-control-layers-overlays');
            
            if (layerControl) {{
                var inputs = layerControl.querySelectorAll('input[type="checkbox"]');
                inputs.forEach(function(input) {{
                    var label = input.parentElement.querySelector('span').textContent.trim();
                    if (label === "{layer_name}") {{
                        legend.style.display = input.checked ? 'block' : 'none';
                    }}
                }});
            }}
        }}
        
        setTimeout(toggleLegend, 500);
        document.addEventListener('click', function(e) {{
            if (e.target.type === 'checkbox') {{
                setTimeout(toggleLegend, 100);
            }}
        }});
        {'setTimeout(function() { document.getElementById("parameter-legend").style.display = "block"; }, 500);' if show else ''}
    </script>
    '''
    
    m.get_root().html.add_child(folium.Element(legend_html))
    return m


def create_map(wa_trailheads, wa_federal_trails, wa_other_trails, wa_state_trails, wa_winter_trails, wa_state_parks):
    """Create Folium map with sites and WTD service area"""
    
    # Center map on sites
    """bounds = sites_gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2"""
    # set bounds to WTD service area
    bounds = wa_trailheads.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon+.1],
        zoom_start=10,
        zoom_control=True,
        scrollWheelZoom=True,
        doubleClickZoom=True,
        tiles=None
    )
    ## location
    # Add locate control to show current position
    LocateControl(
        auto_start=False,  # Set to True to auto-locate on load
        position='topleft',
        strings={
            'title': 'Show my location',
            'popup': 'Your position'
        }
    ).add_to(m)
    # Add base layers
    folium.TileLayer(
        tiles='OpenStreetMap',
        name='Street Map',
        overlay=False,
        control=True,
        show = False,
    ).add_to(m)
    # Add base layers
    folium.TileLayer(
        tiles="Cartodb Positron",
        name='Simple Carto',
        overlay=False,
        control=True,
        show = False,
    ).add_to(m)
    folium.TileLayer(
        tiles='https://basemap.nationalmap.gov/arcgis/rest/services/USGSTopo/MapServer/tile/{z}/{y}/{x}',
        attr='USGS',
        name='USGS Topo',
        overlay=False,
        control=True,
        show = True,
    ).add_to(m)

    m.get_root().html.add_child(folium.Element("""
        <style>
            .leaflet-tile-pane {
                filter: brightness(0.9);  /* Adjust value: 0.5 = 50% brightness, 1.0 = normal */
            }
        </style>
    """))
    # CartoDB Dark Matter (dark theme)
    folium.TileLayer(
        tiles="Cartodb dark_matter",
        name='Dark Carto',
        overlay=False,
        control=True,
        show=False,
    ).add_to(m)
 
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Satellite',
        overlay=False,
        control=True,
        show=False
    ).add_to(m)
    
    # wa trailheads
    if wa_trailheads is not None and not wa_trailheads.empty:
        wa_trailheads_layer = folium.FeatureGroup(name='WA Trailheads', show=True)
        folium.GeoJson(
            wa_trailheads,
            marker=folium.Circle(
                radius=50,  # Radius in meters (for actual geographic size)
                fill=True,
                fillColor='#20B2AA',
                fillOpacity=0.7,
                color='black',
                weight=1
            ),
            tooltip="WA Trailheads"
        ).add_to(wa_trailheads_layer)
        wa_trailheads_layer.add_to(m)
    
    # wa_trails layer will have multiple components
    if wa_federal_trails is not None and not wa_federal_trails.empty:
        wa_trails_layer = folium.FeatureGroup(name='WA Federal Trails', show=True)
        folium.GeoJson(
            wa_federal_trails,
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': 'green',  # Changed to green
                'weight': 2,
                # 'dashArray': '10, 5',  # Removed for solid line
                'fillOpacity': 0
            },
            tooltip="WA Federal Trails"
        ).add_to(wa_trails_layer)
        wa_trails_layer.add_to(m)

    # wa_ other trails layer will have multiple components
    if wa_other_trails is not None and not wa_other_trails.empty:
        wa_other_trails_layer = folium.FeatureGroup(name='WA Other Trails', show=True)
        folium.GeoJson(
            wa_other_trails,
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': '#83D683',  # Changed to green
                'weight': 2,
                # 'dashArray': '10, 5',  # Removed for solid line
                'fillOpacity': 0
            },
            tooltip="WA Other Trails"
        ).add_to(wa_other_trails_layer)
        wa_other_trails_layer.add_to(m)

    # wa_ other trails layer will have multiple components
    if wa_state_trails is not None and not wa_state_trails.empty:
        wa_state_trails_layer = folium.FeatureGroup(name='WA State Trails', show=True)
        folium.GeoJson(
            wa_state_trails,
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': '#6AD44E',  # Changed to green
                'weight': 2,
                # 'dashArray': '10, 5',  # Removed for solid line
                'fillOpacity': 0
            },
            tooltip="WA State Trails"
        ).add_to(wa_state_trails_layer)
        wa_state_trails_layer.add_to(m)
    # Add WA State Parks
    if wa_state_parks is not None and not wa_state_parks.empty:
        wa_state_parks_layer = folium.FeatureGroup(name='WA State Parks', show=False)
        folium.GeoJson(
            wa_state_parks,
            style_function=lambda x: {
                'fillColor': '#9ACD32',  # Yellow-green (not too bright)
                'color': '#9ACD32',  # Border color
                'weight': 2,  # 2 point border
                'fillOpacity': 0.3  # Pretty transparent
            },
            tooltip="WA State Parks"
        ).add_to(wa_state_parks_layer)
        wa_state_parks_layer.add_to(m)

    if wa_winter_trails is not None and not wa_winter_trails.empty:
        wa_winter_trails_layer = folium.FeatureGroup(name='Washington State Winter Non-Motorized Trails', show=True)
        folium.GeoJson(
            wa_winter_trails,
            style_function=lambda x: {
                'fillColor': 'transparent',
                'color': '#AF6D23',
                'weight': 2,
                'dashArray': '10, 5',
                'fillOpacity': 0
            },
            tooltip="Washington State Winter Non-Motorized Trails"
        ).add_to(wa_winter_trails_layer)
        wa_winter_trails_layer.add_to(m)
    
   
    
    # Add sites
    # https://wondernote.org/color-palettes-for-web-digital-blog-graphic-design-with-hexadecimal-codes/
    #wtd_sites = sites_gdf[sites_gdf["WTD Service Area"] == True]
    #add_sites_colored_by_parameter(m, sites_gdf, layer_name='Sites by Parameter', show=True, radius=6)
    # discharge sites
   
    # Add layer control
    folium.LayerControl(collapsed=False, show=False).add_to(m)
    
    return m


def save_map_screenshot(html_path, output_path, pdf_path, window_size=(729, 943)):
    """Save map as static PNG screenshot"""
    
    # Read original HTML
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Create static version
    static_code = """
    <style>
    .leaflet-container {
        cursor: default !important;
        pointer-events: none !important;
    }
    .leaflet-control-zoom,
    .leaflet-control-attribution,
    .leaflet-control-layers {
        display: none !important;
    }
    </style>
    </head>
    """
    
    static_html = html_content.replace('</head>', static_code)
    
    # Save static version to temporary file
    static_html_path = html_path.replace('.html', '_static.html')
    with open(static_html_path, 'w', encoding='utf-8') as f:
        f.write(static_html)
    
    # Take screenshot of static version
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument(f'--window-size={window_size[0]},{window_size[1]}')
    
    driver = webdriver.Chrome(options=chrome_options)
    html_uri = Path(static_html_path).resolve().as_uri()
    driver.get(html_uri)
    time.sleep(2)
    driver.save_screenshot(output_path)
  

    

    # Wait for page to render (optional)
    time.sleep(2)

    # Use Chrome DevTools Protocol (CDP) to print to PDF
    pdf = driver.execute_cdp_cmd("Page.printToPDF", {
        "printBackground": True,
        "landscape": False
    })

    # Save the base64-encoded PDF data to file
    with open(pdf_path, "wb") as f:
        f.write(base64.b64decode(pdf['data']))

    driver.quit()

   
    # Optionally remove temporary static file
    # os.remove(static_html_path)


# Main execution
if __name__ == "__main__":
    # Import data
    #sites_gdf = site_import(file_path="WTD_map/data/WTD_LTM_Gages.xlsx")

    wa_trailheads = wa_trailheads_import()
    wa_federal_trails =  wa_federal_trails_import()
    wa_other_trails = wa_other_trails_import()
    wa_state_trails = wa_state_trails_import()
    wa_winter_trails = wa_winter_trails_import()
    wa_state_parks = wa_state_parks_import()
    # Process data
 
    
    # Create and save map
    m = create_map(wa_trailheads, wa_federal_trails, wa_other_trails, wa_state_trails, wa_winter_trails, wa_state_parks)
    m.save("data/wa_map.html")
    
    
    # Save screenshot
    save_map_screenshot(
        html_path='data/wa_map.html',
        output_path='data/wa_map.png',
        pdf_path='data/wa_map.pdf',
        window_size=(729, 943)
    )
    
    # remove wtd basins from mapping
    basins_filter = None
  
    # Save screenshot
    #save_map_screenshot(
    #    html_path='data/isp_map.html',
    #    output_path='data/isp_map.png',
    #    pdf_path='data/isp_map.pdf',
    #    window_size=(729, 943)
    #)
    print("Map generation complete!")
   