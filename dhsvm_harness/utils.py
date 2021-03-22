import sys, statistics, shutil, os
from datetime import datetime
from django.utils.timezone import get_current_timezone
from ucsrb.models import PourPointBasin, StreamFlowReading, TreatmentScenario, FocusArea
from .settings import FLOW_METRICS, TIMESTEP, ABSOLUTE_FLOW_METRIC, DELTA_FLOW_METRIC, BASINS_DIR, RUNS_DIR, SUPERBASINS, DHSVM_BUILD

def getSegmentIdList(inlines):
    segment_id = []
    for line in inlines:
        line_list = line.split()
        if line_list[-1] == '"Totals"':
            return segment_id
        segment_id.append(line_list[-1].split('"')[1])
    return segment_id

def check_stream_segment_ids(inlines, segment_ids=None):
    if not isinstance(segment_ids, list):
        if segment_ids == None:
            segment_ids = getSegmentIdList(inlines)
        elif isinstance(segment_ids, str):
            if isinstance(int(segment_ids.split('_')[1]), int):
                segment_ids = [segment_ids]
            else:
                print("Unknown segment ID value: '%s'. Quitting...\n" % segment_ids)
                sys.exit(1)
        else:
            print("Unknown segment ID value: '%s'. Quitting...\n" % segment_ids)
            sys.exit(1)

    return segment_ids

def cleanStreamFlowData(flow_file, out_file, segment_ids=None):
    if not segment_ids:
        shutil.copyfile(flow_file, out_file)
        return True
    else:
        with open(flow_file, 'r') as f:
            inlines=f.readlines()

        segment_ids = check_stream_segment_ids(inlines, segment_ids)

        with open(out_file, 'w') as f:
            for line in inlines:
                for id in segment_ids:
                    if id in line:
                        f.write(line)
        return True

def readStreamFlowData(flow_file, segment_ids=None, scenario=None, is_baseline=True):

    with open(flow_file, 'r') as f:
        inlines=f.readlines()

    segment_ids = check_stream_segment_ids(inlines, segment_ids)

    readings_per_day = 24/TIMESTEP
    tz = get_current_timezone()

    for segment_name in segment_ids:
        try:
            basin = PourPointBasin.objects.get(segment_ID=segment_name)
        except Exception as e:
            print('No basin found for segment ID "%s"' % segment_name)
            continue

        segment_readings = {}
        for metric_key in FLOW_METRICS.keys():
            segment_readings[metric_key] = []

        for line in inlines:
            if '"%s"' % segment_name in line:
                data = line.split()
                timestamp = data[0]
                reading = data[4]
                time = tz.localize(datetime.strptime(timestamp, "%m.%d.%Y-%H:%M:%S"))

                for metric_key in FLOW_METRICS.keys():
                    if FLOW_METRICS[metric_key]['measure'] == 'abs':    # Establish abs flow rates/deltas for future reference
                        if FLOW_METRICS[metric_key]['delta']:
                            # Assumes 'Absolute Flow Rate' has already been addressed for this segment's timestep
                            if len(segment_readings[ABSOLUTE_FLOW_METRIC]) > 1:
                                value = segment_readings[ABSOLUTE_FLOW_METRIC][-1]['value'] - segment_readings[ABSOLUTE_FLOW_METRIC][-2]['value']
                            else:
                                value = 0
                        else:
                            value = float(reading)/float(TIMESTEP)
                    else:
                        relevant_readings = int(FLOW_METRICS[metric_key]['period']*readings_per_day)
                        if not FLOW_METRICS[metric_key]['delta']:
                            readings = [x['value'] for x in segment_readings[ABSOLUTE_FLOW_METRIC][-relevant_readings:]]
                            if FLOW_METRICS[metric_key]['measure'] == 'mean':
                                value = statistics.mean(readings)
                            else:
                                readings.sort()
                                value = readings[0]
                        else:
                            source_metric = FLOW_METRICS[metric_key]['source_metric']
                            try:
                                previous_value = segment_readings[source_metric][-2]['value']
                                latest_value = segment_readings[source_metric][-1]['value']
                                value = latest_value - previous_value
                            except IndexError:
                                value = 0

                    segment_readings[metric_key].append({
                        'timestep': timestamp,
                        'value': value
                    })

                    StreamFlowReading.objects.create(
                        timestamp=timestamp,
                        time=time,
                        basin=basin,
                        metric=metric_key,
                        is_baseline=is_baseline,
                        treatment=scenario,
                        value=value
                    )


# ======================================
# CREATE TREATMENT SCENARIO RUN DIR
# ======================================

def getRunDir(treatment_scenario, ts_superbasin_dict):

    # Runs directory
    try:
        os.path.isdir(RUNS_DIR)
    except OSError:
        print("Runs dir not found. Add RUNS_DIR to settings")

    # Create a dir for treatment scenario run using id
    treatment_scenario_id = treatment_scenario.id
    ts_run_dir_name = 'run_' + str(treatment_scenario_id)
    ts_run_dir = os.path.join(RUNS_DIR, ts_run_dir_name)

    if os.path.isdir(ts_run_dir):
        shutil.rmtree(ts_run_dir)

    os.mkdir(ts_run_dir)
    os.mkdir("%s/ts_inputs" % ts_run_dir)

    # --------------------------------------
    # Create sym links for met_data, shadows
    # --------------------------------------

    os.system("ln -s %s/inputs %s/inputs" % (ts_superbasin_dict['basin_dir'], ts_run_dir))

    # Current location:
    # os.system("ln -s %s/../met_data %s/inputs/met_data" % (ts_superbasin_dict['basin_dir'], ts_run_dir))
    # Future location:
    # os.system("ln -s %s/../met_data /usr/local/apps/marineplanner-core/runs/%s/inputs/met_data" % (ts_superbasin_dir, ts_run_dir))

    # os.system("ln -s %s/shadows %s/inputs/shadows" % (ts_superbasin_dict['basin_dir'], ts_run_dir))

    # Create output dir
    ts_run_output_dir = os.path.join(ts_run_dir, "output")

    os.mkdir(ts_run_output_dir)

    return ts_run_dir


# ======================================
# TREATMENT SCENARIO RUN SUPER BASIN
# ======================================

def getRunSuperBasinDir(treatment_scenario):

    # Original basin files directory
    try:
        os.path.isdir(BASINS_DIR)
    except OSError:
        print("Basins dir not found. add BASINS_DIR to settings")
        sys.exit()

    # TreatmentScenario superbasin
    ts_superbasin_code = treatment_scenario.focus_area_input.unit_id.split('_')[0]

    # Superbasin dir
    ts_superbasin_dir = SUPERBASINS[ts_superbasin_code]['inputs']

    return {
        'basin_dir': ts_superbasin_dir,
        'basin_code': ts_superbasin_code
    }

# ======================================
# CREATE MASK RASTER
# ======================================

def createBasinMask(ts, ts_run_dir):

    ts_run_mask_dir = os.path.join(ts_run_mask_dir, 'masked')
    if os.path.isdir(ts_run_mask_dir):
        shutil.rmtree(ts_run_mask_dir)
    os.mkdir(ts_run_mask_dir)

    mask_geom = ts.focus_area_input
    mask_feature = os.path.join(masked_dir, "basin.geojson")

    # make basin mask
    # os.system("rio shapes --projected %s > %s" % (mask, mask_feature))


def getSuperBasinDetails():
    #  xll
    #  yll
    #  cellsize
    #  rows
    #  cols
    print()


# ======================================
# CREATE TREATED VEG LAYER
# ======================================

def setVegLayers(treatment_scenario, ts_superbasin_dict, ts_run_dir):

    import json
    import tempfile
    import rasterio
    from rasterio.mask import mask
    from rasterio.merge import merge
    from rasterio import Affine
    from rasterio.io import MemoryFile
    from rasterio.enums import Resampling
    from shapely.geometry import shape
    from functools import partial
    import shapely.ops
    import pyproj
    import numpy

    ts_superbasin_dir = ts_superbasin_dict['basin_dir']
    ts_superbasin_code = ts_superbasin_dict['basin_code']

    # inputs for TreatmentScenario to feed into DHSVM
    ts_run_dir_inputs = os.path.join('%s/ts_inputs' % ts_run_dir)

    # Prescription ID
    rx_id = treatment_scenario.prescription_treatment_selection

    if rx_id == 'notr':
        # TODO: copy baseline veg bin file to ts_run_dir inputs (not tif)
        return True

    # Projection assigned for later use
    # TODO: add to settings
    PROJECTION = 'PROJCS["NAD_1983_USFS_R6_Albers",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",600000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-120.0],PARAMETER["Standard_Parallel_1",43.0],PARAMETER["Standard_Parallel_2",48.0],PARAMETER["Latitude_Of_Origin",34.0],UNIT["Meter",1.0]]'


    # Start a rasterio environment
    with rasterio.Env():

        # OPEN:
            # Baeline Veg Layer
            # Treatment Veg Layer
        baseline_veg_file = rasterio.open("%s/inputs/veg_files/%s_notr.tif" % (ts_superbasin_dir, ts_superbasin_code), "r")
        treatment_veg_file = rasterio.open("%s/inputs/veg_files/%s_%s.tif" % (ts_superbasin_dir, ts_superbasin_code, rx_id), "r")


        # CREATE treatment shape/feature from TreatmentScenario geometry
        feature = json.loads(treatment_scenario.geometry_dissolved.json)
        feature_shape = shape(feature)

        # Transform and reproject TS shape/feature to match UCSRB data bin files
        tfm = partial(pyproj.transform, pyproj.Proj("epsg:3857"), pyproj.Proj(PROJECTION))
        ts_shape = shapely.ops.transform(tfm, feature_shape)

        # TERMINOLOGY:
            # Mask - a shape/feature area of interest within a "larger area"
            # Clip - new dataset with bounds of original "larger area" that preserves only data that intersects the mask area of interest

        # Treatment Veg Layer

        clipped_treatment = mask(treatment_veg_file, ts_shape, nodata=0)
        clipped_treatment_mask = clipped_treatment[0]

        profile = treatment_veg_file.profile

        with tempfile.NamedTemporaryFile() as tmpfile:
            with rasterio.open(tmpfile, 'w', **profile) as dataset:
                tmpfile.write(clipped_treatment_mask)

            # with memfile.open( **profile) as treatment_memfile:
                # treatment_memfile.write(clipped_treatment_mask)

                merged_veg = merge([dataset, baseline_veg_file], nodata=0, dtype="uint8")
                merged_veg_layer = merged_veg[0]

        profile['driver'] = 'AAIGrid'
        # profile['nodata'] = 0

        with rasterio.open("%s/ts_clipped_treatment_layer.asc" % ts_run_dir_inputs, 'w', **profile) as dst:
            dst.write(merged_veg_layer)

        # Baseline Veg Layer

        # clipped_baseline = mask(baseline_veg_file, ts_shape, nodata=0)
        # clipped_baseline_mask = clipped_baseline[0]
        #
        # with rasterio.open("%s/baseline_veg_layer.asc" % ts_run_dir_inputs, 'w', **baseline_profile) as dst:
        #     dst.write(clipped_baseline_mask)

        # ts_treated_veg_layer = rasterio.open('%s/ts_clipped_treatment_layer.tif' % ts_run_dir, 'r+')

        baseline_veg_file.close()
        treatment_veg_file.close()

        # dst_open.close()

    # Remove header from ascii
    ##########################

    try:
        ts_run_inputs_listdir = os.listdir(ts_run_dir_inputs)
    except OSError:
        print(
            "Path not found: %s. Please make a masked directory in your basins inputs directory."
            % ts_run_inputs_listdir
        )
        sys.exit()

    # Off with their heads!
    # Save some info for later too
    for ts_run_input in ts_run_inputs_listdir:
        input_extension = os.path.splitext(ts_run_input)[-1]
        if input_extension == '.asc':
            input_path = os.path.abspath(os.path.join(ts_run_dir_inputs, ts_run_input))
            ascii_file = open(input_path, "r+")
            content_lines = ascii_file.readlines()
            ascii_file.seek(0,0)
            count = 0

            for l in content_lines:

                if count > 5:
                    ascii_file.write(l)
                else:
                    # 4 of the first 5 lines have info we need
                    # removing extra white space then split into [name, value]
                    line = l.strip().split()

                    # Cols
                    if (line[0] == 'ncols'):
                        ncols = int(line[1])
                    # else:
                        # print("desired number of columns not found in veg ascii header")

                    # Rows
                    if (line[0] == 'nrows'):
                        nrows = int(line[1])
                    # else:
                        # print("desired number of rows not found in veg ascii header")

                    # xllcorner
                    if (line[0] == 'xllcorner'):
                        xllcorner = line[1]
                    # else:
                        # print("desired xllcorner not found in veg ascii header")

                    # yllcorner
                    if (line[0] == 'yllcorner'):
                        yllcorner = line[1]
                    # else:
                        # print("desired yllcorner not found in veg ascii header")

                count += 1

            # end loop for ascii header and close ascii file
            ascii_file.close()

    # end loop of input files

    # Convert ascii to bin
    ######################

    # DHSVM path from settings
    dhsvm_build_path = DHSVM_BUILD

    # path to myconvert
    myconvert = os.path.join(dhsvm_build_path, 'DHSVM', 'program', 'myconvert')

    ascii_file_path = None

    for ts_run_input in ts_run_inputs_listdir:
        input_extension = os.path.splitext(ts_run_input)[-1]
        if input_extension == '.asc':
            bin_file_name = os.path.splitext(ts_run_input)[0] + ".asc.bin"
            bin_file_path = os.path.abspath(os.path.join(ts_run_dir_inputs, bin_file_name))
            ascii_file_path = os.path.abspath(os.path.join(ts_run_dir_inputs, ts_run_input))
            use_type = "character"
            os.system(
                "%s ascii %s %s %s %s %s"
                % (myconvert, use_type, ascii_file_path, bin_file_path, nrows, ncols)
            )

    return ascii_file_path


# ======================================
# Identify basin
# ======================================

def getTargetBasin(treatment_scenario):

    target_basin = None

    # Basin will have 1 field which is cat name of superbasin_segmentId
    # Query run against overlapping_pourpoint_basin
    if treatment_scenario.focus_area_input:
        target_basins =  FocusArea.objects.filter(unit_type="PourPointOverlap", geometry__contains=treatment_scenario.focus_area_input.geometry)
        lcd_basin_area = 0
        for tb in target_basins:
            tb_area = tb.geometry.area
            if tb_area > lcd_basin_area:
                target_basin = tb
        # The below would work if FocusArea had a field for area
        # basin =  FocusArea.objects.filter(unit_type="PourPointOverlap", geometry__contains=treatment_scenario.focus_area_input.geometry).order_by('area')
    else:
        print("No TreatmentScenario focus area provided")

    return target_basin


# ======================================
# IDENTIFY SUB BASINS OF LCD / STEAM SEGMENTS
# ======================================

def getTargetStreamSegments(basin):

    try:
        basin_stream_segments = FocusArea.objects.filter(geometry__within=basin.geometry)
    except Exception as e:
        basin_stream_segments = None
        print('No sub basins found within "%s"' % basin)

    return basin_stream_segments



# ======================================
# CREATE INPUT CONFIG FILE
# ======================================

def createInputConfig(ts_superbasin_dict, ts_run_dir, ts_run_dir_inputs):

    import configparser

    # SUPERBASINS = settings.SUPERBASINS
    ts_superbasin_code = ts_superbasin_dict['basin_code']
    ts_superbasin_name = SUPERBASINS[ts_superbasin_code]['name'].lower()

    # Get superbasin input config file
    ts_superbasin_input_template_name = 'INPUT.UCSRB.%s' % ts_superbasin_name
    ts_superbasin_input_template = os.path.join(ts_superbasin_dict['basin_dir'], ts_superbasin_input_template_name)

    # Location for new run input config file
    ts_run_input_file = os.path.join(ts_run_dir, 'INPUT.UCSRB.run')

    # Create new input from superbasin
    shutil.copyfile(ts_superbasin_input_template, ts_run_input_file)

    # Read new input file using configparser so we can replace some info
    input_config = configparser.ConfigParser()
    input_config.optionxform=str
    input_config.read(ts_run_input_file)

    # set timestep
    # input_config.set("TIME", "Time Step", TIMESTEP)

    #  TODO post 3.25
    # input_config.set("TIME", "Model Start", MODELSTART)
    # input_config.set("TIME", "Model End", MODELEND)

    # Location of mask
    # TODO
    # input_config.set("TERRAIN", "Basin Mask File", ts_mask)

    # Output
    ts_output_dir = ts_run_dir + 'output'
    input_config.set("OUTPUT", "Output Directory", ts_output_dir)

    # DEM file
    # input_config.set("TERRAIN", "DEM File", "./inputs/dem.asc.bin")

    # Stream
    # input_config.set("ROUTING", "Stream Map File", "./inputs/stream.map.dat")
    # input_config.set("ROUTING", "Stream Network File", "./inputs/stream.network.dat")
    # input_config.set("ROUTING", "Stream Class File", "./inputs/stream_property.class")

    # Veg
    input_config.set("VEGETATION", "Vegetation Map File", ts_veg_layer_file)

    return ts_run_input_file


# ======================================
# CONFIGURE TREATMENT SCENARIO RUN
# ======================================

def runHarnessConfig(treatment_scenario):

    # identify super dir to copy original files from
    ts_superbasin_dict = getRunSuperBasinDir(treatment_scenario)

    # TreatmentScenario run directory
    ts_run_dir = getRunDir(treatment_scenario, ts_superbasin_dict)

    # Create run layer
    ts_veg_layer_file = setVegLayers(treatment_scenario, ts_superbasin_dict, ts_run_dir)

    # Get LCD basin
    ts_target_basin = getTargetBasin(treatment_scenario)

    # Name for mask
    # ts_mask_name = ts_superbasin_dict['basin_dir'].unit_type + '_' + ts_superbasin_dict['basin_dir'].unit_id
    # Create mask
    # ts_mask = createBasinMask(treatment_scenario, ts_run_dir)

    # Get target stream segments basins
    if ts_target_basin:
        ts_target_streams = getTargetStreamSegments(ts_target_basin)
    else:
        ts_target_streams = None

    ts_run_input_file = createInputConfig(ts_superbasin_dict, ts_run_dir, ts_veg_layer_file)

    # Run DHSVM
    dhsvm_path = DHSVM_BUILD
    num_cores = 2
    os.system("mpiexec -n %s %s %s" % (num_cores, dhsvm_path, ts_run_input_file))

    # TODO: Populate DB

    # TODO: delte run dir
