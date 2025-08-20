import logging
import os
import sys
import warnings
import datetime
import shutil
import zipfile
import traceback
from subprocess import PIPE, run
from concurrent.futures import ProcessPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd

# Ignore warnings about using '()' in str.contains
warnings.filterwarnings("ignore", "This pattern has match groups")

# Output and data paths (update to absolute if needed)
outPath = os.path.abspath("/sciclone/geograd/geoBoundaries/tmp/CGAZ/") + "/"
gBPath = os.path.abspath("/sciclone/geograd/geoBoundaries/database/geoBoundaries/releaseData/gbOpen/") + "/"
CGAZOuptutPath = os.path.abspath("/sciclone/geograd/geoBoundaries/database/geoBoundaries/releaseData/CGAZ") + "/"

stdGeom = os.path.abspath("/sciclone/geograd/geoBoundaries/geoBoundaryBot/dta/usDoSLSIB_Mar2020.geojson")
stdISO = os.path.abspath("/sciclone/geograd/geoBoundaries/geoBoundaryBot/dta/iso_3166_1_alpha_3.csv")

# Simple direct file-based logging
class DirectLogger:
    def __init__(self):
        self.log_dir = "/sciclone/geograd/geoBoundaries/logs/cgaz"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"cgaz_builder_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        # Log initial information
        self._write_log("=" * 80)
        self._write_log(f"Starting CGAZ Builder - {datetime.datetime.now()}")
        self._write_log(f"Log file: {self.log_file}")
        self._write_log(f"Output directory: {outPath}")
        self._write_log("=" * 80)
    
    def _write_log(self, message, level="INFO"):
        """Write directly to log file and print to console"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"{timestamp} - {message}\n"
        
        # Write to file
        try:
            with open(self.log_file, 'a') as f:
                f.write(log_line)
        except Exception as e:
            print(f"ERROR: Failed to write to log file: {e}")
        
        # Always print to console
        print(log_line.strip())
    
    def debug(self, message):
        self._write_log(f"DEBUG: {message}")
    
    def info(self, message):
        self._write_log(f"INFO: {message}")
    
    def warning(self, message):
        self._write_log(f"WARNING: {message}")
    
    def error(self, message):
        self._write_log(f"ERROR: {message}")
    
    def critical(self, message):
        self._write_log(f"CRITICAL: {message}")
        sys.exit(1)
        
    # Dummy method for compatibility with set_verbosity
    def setLevel(self, level):
        pass

# Initialize direct logger
logger = DirectLogger()
logger.info("Starting geometry processing...")


def cmd(command, **kwargs):
    """Run a shell command with enhanced logging."""
    logger.debug(f"Executing command: {command}")
    
    # Log environment information
    logger.debug(f"Current working directory: {os.getcwd()}")
    
    # Check for file operations in the command
    file_ops = ['>', '>>', '<', '|', 'cp', 'mv', 'rm', 'ogr2ogr', 'mapshaper']
    if any(op in command for op in file_ops):
        # Log file operations
        logger.debug("File operation detected in command")
        
        # Check for input/output files in command
        for arg in command.split():
            if any(ext in arg for ext in ['.geojson', '.topojson', '.shp', '.gpkg']):
                abs_path = os.path.abspath(arg)
                logger.debug(f"File path in command: {arg}")
                logger.debug(f"Absolute path: {abs_path}")
                logger.debug(f"File exists: {os.path.exists(abs_path)}")
                if os.path.isfile(abs_path):
                    logger.debug(f"File size: {os.path.getsize(abs_path)} bytes")
    
    # Execute the command
    logger.debug(f"Executing command: {command}")
    r = run(
        command, 
        stdout=PIPE, 
        stderr=PIPE, 
        universal_newlines=True, 
        shell=True, 
        **kwargs
    )
    
    # Log command results
    if r.returncode != 0:
        logger.error(f"Command failed with return code {r.returncode}")
        logger.error(f"Command: {command}")
        logger.error(f"Working directory: {os.getcwd()}")
        
        if r.stderr.strip():
            logger.error(f"Command stderr: {r.stderr.strip()}")
            
            # Additional debug for common errors
            if "No such file or directory" in r.stderr:
                logger.error("File not found error detected. Check if all required files exist.")
            elif "Unable to open datasource" in r.stderr:
                logger.error("Unable to open datasource. Check file paths and permissions.")
    
    # Log command output if verbose
    if r.stdout.strip():
        logger.debug(f"Command stdout: {r.stdout.strip()}")
    
    return r


def set_verbosity(verbose_level=0):
    """Dummy function - we log everything now."""
    logger.info("Verbosity setting ignored - logging all messages")
    logger.info("Starting data preprocessing...")


def preprocess_dta():
    """Preprocess the data by separating disputed regions, renaming countries, and adding ISO codes."""
    logger.info("Dissolving geometries based on ISO codes...")
    globalDta = gpd.read_file(stdGeom)
    isoCSV = pd.read_csv(stdISO)

    # Separate disputedG regions.
    # All disputedG regions will be assigned to a "Disputed" set of regions, burned in at the end.
    disputedG = globalDta[globalDta["COUNTRY_NA"].str.contains("(disp)")].copy()
    G = globalDta[~globalDta["COUNTRY_NA"].str.contains("(disp)")].copy()

    # For CGAZ, all territories are merged into their parent country.
    # Cleanup country names in DoS cases
    def country_renamer(country_na: str):
        test_dict = {
            "(US)": "United States",
            "(UK)": "United Kingdom",
            "(Aus)": "Australia",
            "Greenland (Den)": "Greenland",
            "(Den)": "Denmark",
            "(Fr)": "France",
            "(Ch)": "China",
            "(Nor)": "Norway",
            "(NZ)": "New Zealand",
            "Netherlands [Caribbean]": "Netherlands",
            "(Neth)": "Netherlands",
            "Portugal [": "Portugal",
            "Spain [": "Spain",
        }

        default = [country_na]
        country_na = [v for k, v in test_dict.items() if k in country_na] + default
        return country_na[0]

    G.COUNTRY_NA = G.COUNTRY_NA.map(country_renamer)

    # Add ISO codes

    # Need to just do a list at some point.
    # Don't want to change the underlying data is the challenge.
    def isoLookup(country):
        try:
            switcher = {
                "Antigua & Barbuda": "ATG",
                "Bahamas, The": "BHS",
                "Bosnia & Herzegovina": "BIH",
                "Congo, Dem Rep of the": "COD",
                "Congo, Rep of the": "COG",
                "Cabo Verde": "CPV",
                "Cote d'Ivoire": "CIV",
                "Central African Rep": "CAF",
                "Czechia": "CZE",
                "Gambia, The": "GMB",
                "Iran": "IRN",
                "Korea, North": "PRK",
                "Korea, South": "KOR",
                "Laos": "LAO",
                "Macedonia": "MKD",
                "Marshall Is": "MHL",
                "Micronesia, Fed States of": "FSM",
                "Moldova": "MDA",
                "Sao Tome & Principe": "STP",
                "Solomon Is": "SLB",
                "St Kitts & Nevis": "KNA",
                "St Lucia": "LCA",
                "St Vincent & the Grenadines": "VCT",
                "Syria": "SYR",
                "Tanzania": "TZA",
                "Vatican City": "VAT",
                "United States": "USA",
                "Antarctica": "ATA",
                "Bolivia": "BOL",
                "Brunei": "BRN",
                "Russia": "RUS",
                "Trinidad & Tobago": "TTO",
                "Swaziland": "SWZ",
                "Venezuela": "VEN",
                "Vietnam": "VNM",
                "Burma": "MMR"
            }
            
            # First try to find the country in the CSV
            isoCSV_match = isoCSV[isoCSV["Name"] == country]
            if len(isoCSV_match) == 1:
                isoCSV_match = isoCSV_match["Alpha-3code"].values[0]
                logger.debug(f"Found ISO code in CSV: {isoCSV_match} for country: {country}")
                return isoCSV_match
            
            # If not found in CSV, try the switcher dictionary
            switcher_match = switcher.get(country)
            if switcher_match:
                logger.debug(f"Using switcher match: {switcher_match} for country: {country}")
                return switcher_match
                
            logger.warning(f"No ISO code found for country: {country}")
            return None
            
        except Exception as e:
            logger.error(f"Error in isoLookup for country {country}: {str(e)}", exc_info=True)
            return None

    G["ISO_CODE"] = G.COUNTRY_NA.map(isoLookup)

    # check for nulls in ISO_CODE
    features_without_iso_code = G[G.ISO_CODE.isna() | G.ISO_CODE == ""]
    if len(features_without_iso_code) > 0:
        print("Error - no match.")
        print(features_without_iso_code)
        sys.exit(1)

    disputedG.COUNTRY_NA = disputedG.COUNTRY_NA.str.replace(" (disp)", "", regex=False)
    disputedG["ISO_CODE"] = disputedG.COUNTRY_NA.map(isoLookup)
    disputedG[disputedG.ISO_CODE.isna() | disputedG.ISO_CODE == ""].ISO_CODE = "None"

    G.to_file("./baseISO.geojson", driver="GeoJSON")
    disputedG.to_file("./disputedISO.geojson", driver="GeoJSON")


def process_geometry_wrapper(args, adm0):
    """Wrapper function to handle logging and exceptions for parallel processing."""
    try:
        logger.info(f"Starting processing for ADM0: {adm0}")
        # Initialize empty strings for this process
        adm0str, adm1str, adm2str = process_geometry(args, adm0, "", "", "")
        logger.info(f"Completed processing for ADM0: {adm0}")
        return {
            'adm0': adm0,
            'adm0str': adm0str,
            'adm1str': adm1str,
            'adm2str': adm2str,
            'success': True
        }
    except Exception as e:
        logger.error(f"Error processing ADM0 {adm0}: {str(e)}", exc_info=True)
        return {
            'adm0': adm0,
            'error': str(e),
            'success': False
        }

def process_geometries(args):
    """Process geometries for all ADM0 regions in parallel."""
    try:
        logger.info("Starting process_geometries")
        logger.info("Loading base ISO geometries")
        
        # Load the base ISO file
        base_iso_path = f"{outPath}baseISO.geojson"
        logger.info(f"Loading base ISO file from: {base_iso_path}")
        
        if not os.path.exists(base_iso_path):
            error_msg = f"Base ISO file not found at: {base_iso_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        G = gpd.read_file(base_iso_path)
        logger.info(f"Successfully loaded base ISO file with {len(G)} records")

        # Create a list of all the ADM0s
        adm0s = [x for x in G.ISO_CODE.unique() if x is not None]
        logger.info(f"Found {len(adm0s)} unique ADM0 regions to process: {adm0s}")

        # Initialize result accumulators
        all_adm0str = []
        all_adm1str = []
        all_adm2str = []
        failed_adm0s = []

        # Process ADM0s in parallel
        with ProcessPoolExecutor(max_workers=10) as executor:
            # Submit all tasks
            future_to_adm0 = {
                executor.submit(process_geometry_wrapper, args, adm0): adm0 
                for adm0 in adm0s
            }
            
            # Process results as they complete
            for future in as_completed(future_to_adm0):
                adm0 = future_to_adm0[future]
                try:
                    result = future.result()
                    if result['success']:
                        all_adm0str.append(result['adm0str'])
                        all_adm1str.append(result['adm1str'])
                        all_adm2str.append(result['adm2str'])
                        logger.info(f"Successfully processed {adm0}")
                    else:
                        failed_adm0s.append(adm0)
                        logger.error(f"Failed to process {adm0}: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    failed_adm0s.append(adm0)
                    logger.error(f"Exception processing {adm0}: {str(e)}")
        
        # Log summary
        if failed_adm0s:
            logger.warning(f"Failed to process {len(failed_adm0s)}/{len(adm0s)} ADM0 regions: {failed_adm0s}")
        else:
            logger.info("Successfully processed all ADM0 regions")
        
        # Combine all results
        return "".join(all_adm0str), "".join(all_adm1str), "".join(all_adm2str)
        
    except Exception as e:
        logger.critical(f"Script failed: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)


def process_geometry(args, adm0, adm0str, adm1str, adm2str):
    """Process geometry for a single ADM0 region."""
    logger.info(f"Starting process_geometry for ADM0: {adm0}")
    
    try:
        logger.debug(f"Processing ADM0: {adm0}")
        curISO = adm0
        
        # Load the base ISO file to get the geometry for this ADM0
        base_iso_path = f"{outPath}baseISO.geojson"
        logger.debug(f"Loading base ISO file for {curISO} from: {base_iso_path}")
        
        if not os.path.exists(base_iso_path):
            error_msg = f"Base ISO file not found at: {base_iso_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        G = gpd.read_file(base_iso_path)
        logger.debug(f"Loaded base ISO file with {len(G)} records")
        
        # Filter for the current ISO
        g = G[G['ISO_CODE'] == curISO]
        if g.empty:
            error_msg = f"No data found for ISO code: {curISO}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        logger.debug(f"Found {len(g)} records for ISO: {curISO}")
        
        if g.empty:
            logger.warning(f"No geometry found for ISO code: {curISO}")
            return adm0str, adm1str, adm2str

        # Save ADM0 geometry
        DTA_A0Path = os.path.join(outPath, f"ADM0_{curISO}.geojson")
        g.to_file(DTA_A0Path, driver="GeoJSON")
        # Process additional boundary files from geoBoundaries
        A0Path = os.path.join(gBPath, curISO, "ADM0", f"geoBoundaries-{curISO}-ADM0.geojson")
        A1Path = os.path.join(gBPath, curISO, "ADM1", f"geoBoundaries-{curISO}-ADM1.geojson")
        A2Path = os.path.join(gBPath, curISO, "ADM2", f"geoBoundaries-{curISO}-ADM2.geojson")
        
        logger.debug(f"ADM0 Path: {A0Path}")
        logger.debug(f"ADM1 Path: {A1Path}")
        logger.debug(f"ADM2 Path: {A2Path}")

        # Fall back to higher admin levels if needed
        if not os.path.isfile(A1Path):
            A1Path = A0Path
        if not os.path.isfile(A2Path):
            A2Path = A1Path
        
        # Generate output paths for GeoJSON
        adm1out = os.path.join(outPath, f"ADM1_{curISO}.geojson")
        adm2out = os.path.join(outPath, f"ADM2_{curISO}.geojson")
        
        # Process with mapshaper if input files exist
        if os.path.isfile(A1Path):
            cmd(f"mapshaper-xl {A1Path} -o format=geojson {adm1out}")
            adm1str += " " + adm1out
            
        if os.path.isfile(A2Path):
            cmd(f"mapshaper-xl {A2Path} -o format=geojson {adm2out}")
            adm2str += " " + adm2out
        
        adm0str += " " + DTA_A0Path
        logger.debug(f"Completed processing ADM0: {adm0}")
        return adm0str, adm1str, adm2str
        
    except Exception as e:
        logger.error(f"Error processing ADM0 {adm0}: {str(e)}", exc_info=True)
        return adm0str, adm1str, adm2str


def load_iso_name_lookup():
    """Load ISO code to country name mapping from CSV."""
    iso_lookup = {}
    try:
        iso_df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../../dta/iso_3166_1_alpha_3.csv'))
        iso_lookup = dict(zip(iso_df['Alpha-3code'], iso_df['Name']))
        logger.debug(f"Loaded ISO name lookup with {len(iso_lookup)} entries")
    except Exception as e:
        logger.error(f"Error loading ISO name lookup: {str(e)}")
    return iso_lookup

# Cache the ISO lookup
_ISO_NAME_LOOKUP = load_iso_name_lookup()

def filter_attributes(gdf, adm_level):
    """Filter GeoDataFrame to only include required attributes."""
    required_columns = {
        'geometry',  # Keep geometry column
        'shapeName',
        'shapeID',
        'shapeGroup',
        'shapeType'
    }
    
    # Create a copy to avoid SettingWithCopyWarning
    result = gdf.copy()
    
    # Set shapeID first as it's used for the name lookup
    if 'shapeID' not in result.columns:
        result['shapeID'] = result.get('GID_0', result.get('ISO_CODE', ''))
    
    # Handle shapeName based on ADM level
    if 'shapeName' not in result.columns:
        if adm_level == 'ADM0':
            # For ADM0, use the ISO name lookup
            result['shapeName'] = result['shapeID'].map(_ISO_NAME_LOOKUP).fillna('')
            # Fallback to original name if lookup fails
            if result['shapeName'].empty or result['shapeName'].isna().all():
                result['shapeName'] = result.get('NAME_0', result.get('NAME', ''))
        else:
            # For ADM1/ADM2, use existing name fields
            result['shapeName'] = result.get('NAME_1', result.get('NAME_2', result.get('NAME', '')))
    
    # Set shapeGroup if not present
    if 'shapeGroup' not in result.columns:
        result['shapeGroup'] = result['shapeID']
    
    # Set shapeType
    if 'shapeType' not in result.columns:
        result['shapeType'] = adm_level
    
    # Keep only required columns
    columns_to_keep = [col for col in result.columns if col in required_columns]
    return result[columns_to_keep]

def join_admins(adm0str, adm1str, adm2str):
    """Join ADM levels and ensure only required attributes are kept."""
    logger.debug("Joining ADM0 / ADM1 / ADM2s together into one large geom.")
    logger.debug(f"ADM0: {adm0str}")
    logger.debug(f"ADM1: {adm1str}")
    logger.debug(f"ADM2: {adm2str}")
    
    
    A0mapShaperFull = (
        "mapshaper-xl -i "
        + adm0str
        + " "
        + outPath
        + "disputedISO.geojson"
        + " combine-files -merge-layers force"
        + " name=globalADM0"
        +
        # " -simplify weighted " + ratio + "% keep-shapes" +
        " -clean gap-fill-area=10000km2"
        + " -o format=topojson "
        + (outPath + "geoBoundariesCGAZ_ADM0.topojson")
        + " -o format=geojson "
        + (outPath + "geoBoundariesCGAZ_ADM0.geojson")
        + " -o format=shapefile "
        + (outPath + "geoBoundariesCGAZ_ADM0.shp")
    )
    A1mapShaperFull = (
        "mapshaper-xl -i "
        + adm1str
        + " "
        + outPath
        + "disputedISO.geojson"
        + " combine-files -merge-layers force"
        + " name=globalADM1"
        +
        # " -simplify weighted " + ratio + "% keep-shapes" +
        " -clean gap-fill-area=10000km2"
        + " -o format=topojson "
        + (outPath + "geoBoundariesCGAZ_ADM1.topojson")
        + " -o format=geojson "
        + (outPath + "geoBoundariesCGAZ_ADM1.geojson")
        + " -o format=shapefile "
        + (outPath + "geoBoundariesCGAZ_ADM1.shp")
    )
    A2mapShaperFull = (
        "mapshaper-xl -i "
        + adm2str
        + " "
        + outPath
        + "disputedISO.geojson"
        + " combine-files -merge-layers force"
        + " name=globalADM2"
        +
        # " -simplify weighted " + ratio + "% keep-shapes" +
        " -clean gap-fill-area=10000km2"
        + " -o format=topojson "
        + (outPath + "geoBoundariesCGAZ_ADM2.topojson")
        + " -o format=geojson "
        + (outPath + "geoBoundariesCGAZ_ADM2.geojson")
        + " -o format=shapefile "
        + (outPath + "geoBoundariesCGAZ_ADM2.shp")
    )

    def generate_output_formats(adm_level):
        """Generate GeoPackage and Shapefile from the final GeoJSON."""
        geojson_path = f"{outPath}geoBoundariesCGAZ_{adm_level}.geojson"
        gpkg_path = f"{outPath}geoBoundariesCGAZ_{adm_level}.gpkg"
        shp_path = f"{outPath}geoBoundariesCGAZ_{adm_level}"
        
        # Read the final GeoJSON
        gdf = gpd.read_file(geojson_path)
        
        # Save as GeoPackage
        gdf.to_file(gpkg_path, driver='GPKG')
        logger.info(f"Generated GeoPackage: {gpkg_path}")
        
        # Save as Shapefile
        gdf.to_file(f"{shp_path}.shp", driver='ESRI Shapefile')
        logger.info(f"Generated Shapefile: {shp_path}.shp")
        
        # Create zip of shapefile components
        with zipfile.ZipFile(f"{shp_path}.zip", 'w', zipfile.ZIP_DEFLATED) as zipf:
            for ext in ['.shp', '.shx', '.dbf', '.prj']:
                file_path = f"{shp_path}{ext}"
                if os.path.exists(file_path):
                    zipf.write(file_path, os.path.basename(file_path))
        logger.info(f"Created shapefile zip: {shp_path}.zip")
    
    # Process ADM0 with mapshaper
    logger.info("Starting ADM0 mapshaper processing...")
    logger.info(A0mapShaperFull)
    cmd(A0mapShaperFull)
    
    # Process ADM1 with mapshaper
    logger.info("Starting ADM1 mapshaper processing...")
    logger.info(A1mapShaperFull)
    cmd(A1mapShaperFull)
    
    # Process ADM2 with mapshaper
    logger.info("Starting ADM2 mapshaper processing...")
    logger.info(A2mapShaperFull)
    cmd(A2mapShaperFull)
    
    # Now process each ADM level to generate final outputs
    for adm_level in ['ADM0', 'ADM1', 'ADM2']:
        logger.info(f"Processing final outputs for {adm_level}...")
        
        # Read the TopoJSON output from mapshaper
        topojson_path = f"{outPath}geoBoundariesCGAZ_{adm_level}.topojson"
        gdf = gpd.read_file(topojson_path)
        
        # Filter attributes
        filtered_gdf = filter_attributes(gdf, adm_level)
        
        # Save as GeoJSON (our source of truth)
        geojson_path = f"{outPath}geoBoundariesCGAZ_{adm_level}.geojson"
        filtered_gdf.to_file(geojson_path, driver='GeoJSON')
        logger.info(f"Generated final GeoJSON: {geojson_path}")
        
        # Generate other formats from the GeoJSON
        generate_output_formats(adm_level)
        
        logger.info(f"Completed processing for {adm_level}")

    # Clean up intermediate TopoJSON files
    for adm_level in ['ADM0', 'ADM1', 'ADM2']:
        topojson_path = f"{outPath}geoBoundariesCGAZ_{adm_level}.topojson"
        if os.path.exists(topojson_path):
            os.remove(topojson_path)
            logger.debug(f"Cleaned up intermediate file: {topojson_path}")


def dissolve_based_on_ISO_Code():
    cmd(
        f"mapshaper-xl ./baseISO.geojson -dissolve fields='ISO_CODE' multipart -o force format=geojson {outPath}baseISO.geojson"
    )
    cmd(
        f"mapshaper-xl ./disputedISO.geojson -dissolve fields='ISO_CODE' multipart -o format=geojson {outPath}disputedISO.geojson"
    )


def package_final_outputs():
    """Package and copy final output files to the CGAZ output directory."""
    try:
        logger.info("Starting to package final outputs...")
        
        # Ensure output directory exists
        os.makedirs(CGAZOuptutPath, exist_ok=True)
        
        for adm_level in ['ADM0', 'ADM1', 'ADM2']:
            # Base filenames
            base_filename = f"geoBoundariesCGAZ_{adm_level}"
            src_base = os.path.join(outPath, f"geoBoundariesCGAZ_{adm_level}")
            
            # 1. Copy GeoJSON
            src_geojson = f"{src_base}.geojson"
            dst_geojson = os.path.join(CGAZOuptutPath, f"{base_filename}.geojson")
            if os.path.exists(src_geojson):
                shutil.copy2(src_geojson, dst_geojson)
                logger.info(f"Copied {src_geojson} to {dst_geojson}")
            
            # 2. Copy GeoPackage
            src_gpkg = f"{src_base}.gpkg"
            dst_gpkg = os.path.join(CGAZOuptutPath, f"{base_filename}.gpkg")
            if os.path.exists(src_gpkg):
                shutil.copy2(src_gpkg, dst_gpkg)
                logger.info(f"Copied {src_gpkg} to {dst_gpkg}")
            
            # 3. Create zip with shapefile components
            shapefile_components = [f"{src_base}.{ext}" for ext in ['shp', 'shx', 'dbf', 'prj']]
            if all(os.path.exists(f) for f in shapefile_components):
                zip_path = os.path.join(CGAZOuptutPath, base_filename)
                with zipfile.ZipFile(f"{zip_path}.zip", 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in shapefile_components:
                        zipf.write(file, os.path.basename(file))
                logger.info(f"Created zip file: {zip_path}.zip")
        
        logger.info("Finished packaging all outputs")
        
    except Exception as e:
        logger.error(f"Error packaging final outputs: {str(e)}")
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Process CGAZ boundaries')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='Increase verbosity')
    args = parser.parse_args()
    
    try:
        set_verbosity(args.verbose)
        logger.info("Starting CGAZ boundary processing...")
        preprocess_dta()
        adm0str, adm1str, adm2str = process_geometries(args)
        join_admins(adm0str, adm1str, adm2str)
        package_final_outputs()
        logger.info("CGAZ boundary processing completed successfully")
    except Exception as e:
        error_msg = f"Error in CGAZ boundary processing: {str(e)}\n{traceback.format_exc()}"
        logger.critical(error_msg)
        sys.exit(1)
