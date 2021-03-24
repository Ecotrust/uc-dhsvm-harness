# import configparser
from datetime import datetime
from django.conf import settings as ucsrb_settings
from django.template import Template, Context
from django.utils.timezone import get_current_timezone
from functools import partial
import json
# import numpy
import os
import tempfile
import pyproj
import rasterio
from rasterio.mask import mask
from rasterio.merge import merge
import shapely
from shapely.geometry import shape
import shapely.ops
import shutil
import statistics
import sys
from ucsrb.models import StreamFlowReading, TreatmentScenario, FocusArea

from dhsvm_harness.settings import FLOW_METRICS, TIMESTEP, ABSOLUTE_FLOW_METRIC, DELTA_FLOW_METRIC, BASINS_DIR, RUNS_DIR, SUPERBASINS, DHSVM_BUILD, RUN_CORES


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

def importBasinLine(line, basin_name, is_baseline, scenario):
    tz = get_current_timezone()
    # basin = basins[basin_name]
    data = line.split()
    timestamp = data[0]
    reading = data[4]
    # time = tz.localize(datetime.strptime(timestamp, "%m.%d.%Y-%H:%M:%S"))

    value = float(reading)/float(TIMESTEP)

    new_reading = StreamFlowReading.objects.create(
        timestamp=timestamp,
        time=tz.localize(datetime.strptime(timestamp, "%m.%d.%Y-%H:%M:%S")),
        segment_id=basin_name,
        metric=ABSOLUTE_FLOW_METRIC,
        is_baseline=is_baseline,
        treatment=scenario,
        value=value
    )

def readStreamFlowData(flow_file, segment_ids=None, scenario=None, is_baseline=True):

    # readings_per_day = 24/TIMESTEP
    tz = get_current_timezone()

    print("Reading in flow data...")
    with open(flow_file, 'r') as f:
        inlines=f.readlines()

    segment_ids = check_stream_segment_ids(inlines, segment_ids)

    start_timestamp = inlines[0].split()[0]
    start_time = tz.localize(datetime.strptime(start_timestamp, "%m.%d.%Y-%H:%M:%S"))
    end_timestamp = inlines[-1].split()[0]
    end_time = tz.localize(datetime.strptime(end_timestamp, "%m.%d.%Y-%H:%M:%S"))

    print('purging obsolete records...')
    for segment_id in segment_ids:
        StreamFlowReading.objects.filter(time__gte=start_time, time__lte=end_time, segment_id=segment_id, treatment=scenario).delete()
        if scenario.prescription_treatment_selection == 'notr':
            StreamFlowReading.objects.filter(time__gte=start_time, time__lte=end_time, segment_id=segment_id, is_baseline=True, treatment=None).delete()



    if len(inlines) < 10000:
        print('Importing data...')
        for line in inlines:
            basin_name = line.split('"')[1]
            if basin_name in segment_ids:
                if scenario.prescription_treatment_selection == 'notr':
                    importBasinLine(line, basin_name, is_baseline=True, scenario=None)
                importBasinLine(line, basin_name, is_baseline, scenario)

    else:
        # release memory
        inlines = []
        # create split dir
        parent_dir = os.path.dirname(flow_file)
        split_dir = os.path.join(parent_dir, 'flow_split_%s' % datetime.now().timestamp())
        if os.path.isdir(split_dir):
            shutil.rmtree(split_dir)
        os.mkdir(split_dir)
        os.system('split -l 10000 %s %s/' % (flow_file, split_dir))
        filecount = 1
        split_files = os.listdir(split_dir)
        for filename in split_files:
            file_slice = os.path.join(split_dir, filename)
            print('Reading %d (of %d) "%s" at %s' % (filecount, len(split_files), file_slice, str(datetime.now())))
            with open(file_slice, 'r') as f:
                inlines=f.readlines()

            for line in inlines:
                basin_name = line.split('"')[1]
                if basin_name in segment_ids:
                    if scenario.prescription_treatment_selection == 'notr':
                        importBasinLine(line, basin_name, is_baseline=True, scenario=None)
                    importBasinLine(line, basin_name, is_baseline, scenario)
            filecount +=1

        shutil.rmtree(split_dir)

# ======================================
# CREATE TREATMENT SCENARIO RUN DIR
# ======================================

def getRunDir(treatment_scenario, ts_superbasin_dict):

    # Runs directory
    try:
        if not os.path.isdir(RUNS_DIR):
            os.mkdir(RUNS_DIR)
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
    if treatment_scenario.focus_area_input.unit_type == 'PourPointOverlap':
        ts_superbasin_code = treatment_scenario.focus_area_input.unit_id.split('_')[0]
    else:
        overlapping_basins = FocusArea.objects.filter(unit_type='PourPointOverlap', geometry__contains=treatment_scenario.focus_area_input.geometry)
        if len(overlapping_basins) > 1:
            ts_superbasin_code = sorted(overlapping_basins, key=lambda x: x.geometry.area)[1].unit_id.split('_')[0]
        else:
            ts_superbasin_code = overlapping_basins[0].unit_id.split('_')[0]

    # Superbasin dir
    ts_superbasin_dir = SUPERBASINS[ts_superbasin_code]['inputs']

    return {
        'basin_dir': ts_superbasin_dir,
        'basin_code': ts_superbasin_code
    }

# ======================================
# CREATE TREATED VEG LAYER
# ======================================

def setVegLayers(treatment_scenario, ts_superbasin_dict, ts_run_dir):

    ts_superbasin_dir = ts_superbasin_dict['basin_dir']
    ts_superbasin_code = ts_superbasin_dict['basin_code']

    # inputs for TreatmentScenario to feed into DHSVM
    ts_run_dir_inputs = os.path.join('%s/ts_inputs' % ts_run_dir)

    # Prescription ID
    rx_id = treatment_scenario.prescription_treatment_selection

    if rx_id == 'notr':
        # Just return baseline veg file as a bin
        baseline_veg_file = os.path.join("%s/inputs/veg_files/%s_notr.asc.bin" % (ts_superbasin_dir, ts_superbasin_code))
        return baseline_veg_file

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

        baseline_veg_file.close()
        treatment_veg_file.close()

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
    myconvert = os.path.join(DHSVM_BUILD, 'DHSVM', 'program', 'myconvert')

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

    return bin_file_path


# ======================================
# Identify basin
# ======================================

def getTargetBasin(treatment_scenario):

    target_basin = None

    # Basin will have 1 field which is cat name of superbasin_segmentId
    # Query run against overlapping_pourpoint_basin
    if treatment_scenario.focus_area_input:
        target_basins =  FocusArea.objects.filter(unit_type="PourPointOverlap", geometry__contains=treatment_scenario.focus_area_input.geometry)
        if len(target_basins) > 1:
            target_basin = sorted(target_basins, key=lambda x: x.geometry.area)[1]
        else:
            target_basin = target_basins[0]
    else:
        print("No TreatmentScenario focus area provided")

    return target_basin


# ======================================
# IDENTIFY SUB BASINS OF LCD / STEAM SEGMENTS
# ======================================

def getTargetStreamSegments(basin):

    try:
        basin_stream_segments = FocusArea.objects.filter(unit_type='PourPointOverlap', geometry__within=basin.geometry)
    except Exception as e:
        basin_stream_segments = None
        print('No sub basins found within "%s"' % basin)

    return basin_stream_segments

# ======================================
# IDENTIFY SUB BASINS OF LCD / STEAM SEGMENTS
# ======================================

def createTargetStreamNetworkFile(ts_target_streams, ts_run_dir, ts_superbasin_dir):
    # return os.path.join(ts_superbasin_dir, 'inputs', 'stream.network_all.dat')
    infile_name = os.path.join(ts_superbasin_dir, 'inputs', 'stream.network_clean.dat')
    outfile_name = os.path.join(ts_run_dir, 'ts_inputs', 'stream.network.dat')
    if ts_target_streams == None:
        all_segments_file = os.path.join(ts_superbasin_dir, 'inputs', 'stream.network_all.dat')
        shutil.copyfile(all_segments_file, outfile_name)
    else:
        # loop through target segment ids, creating a list of the related integers
        segment_dict = {}
        for basin in ts_target_streams:
            try:
                segment_dict[basin.unit_id.split('_')[1]] = basin.unit_id
            except IndexError as e:
                print("Found basin with unit_id == %s" % basin.unit_id)
                pass
        with open(infile_name, 'r') as infile:
            inlines = infile.readlines()
        with open(outfile_name, 'w') as outfile:
            for index, line_string in enumerate(inlines):
                parsed_line = line_string.split()
                if parsed_line[0] in segment_dict.keys():
                    parsed_line.append('SAVE"%s"' % segment_dict[parsed_line[0]])
                line_string = '\t'.join(parsed_line)
                line_string =  "%s\n" % line_string
                outfile.write(line_string)
    return outfile_name

# ======================================
# CREATE INPUT CONFIG FILE
# ======================================

def createInputConfig(ts_target_basin, ts_superbasin_dict, ts_run_dir, ts_veg_layer_file, ts_network_file, model_year='baseline'):

    # SUPERBASINS = settings.SUPERBASINS
    ts_superbasin_code = ts_superbasin_dict['basin_code']
    ts_superbasin_name = SUPERBASINS[ts_superbasin_code]['name'].lower()

    # Get superbasin input config file
    # ts_superbasin_input_template_name = 'INPUT.UCSRB.%s.bck' % ts_superbasin_name
    ts_superbasin_input_template_name = 'INPUT.UCSRB.%s' % ts_superbasin_name
    ts_superbasin_input_template = os.path.join(ts_superbasin_dict['basin_dir'], ts_superbasin_input_template_name)

    # Location for new run input config file
    ts_run_input_file = os.path.join(ts_run_dir, 'INPUT.UCSRB.run')

    mask_file = os.path.join(ts_superbasin_dict['basin_dir'], 'masks', "%s.asc.bin" % ts_target_basin.unit_id)

    # Create new input from superbasin
    with open(ts_superbasin_input_template, 'r') as file_contents:
        contents = file_contents.read()
    t = Template(contents)
    c = Context({
        'RUN_DIR': ts_run_dir,
        'BASIN_DIR': ts_superbasin_dict['basin_dir'],
        'START': datetime.strftime(ucsrb_settings.MODEL_YEARS[model_year]['start'], "%m/%d/%Y-%H"),
        'STOP': datetime.strftime(ucsrb_settings.MODEL_YEARS[model_year]['end'], "%m/%d/%Y-%H"),
        'VEG_FILE': ts_veg_layer_file,
        'NETWORK_FILE': ts_network_file,
        'MASK': mask_file,
        'TIMESTEP': TIMESTEP
    })
    out_contents = t.render(c)
    with open(ts_run_input_file, 'w') as outfile:
        outfile.write(out_contents)

    return ts_run_input_file


# ======================================
# CONFIGURE TREATMENT SCENARIO RUN
# ======================================

def runHarnessConfig(treatment_scenario):
    # identify super dir to copy original files from
    ts_superbasin_dict = getRunSuperBasinDir(treatment_scenario)        # 2 seconds

    # TreatmentScenario run directory
    ts_run_dir = getRunDir(treatment_scenario, ts_superbasin_dict)      # 0 seconds

    # Create run layer
    ts_veg_layer_file = setVegLayers(treatment_scenario, ts_superbasin_dict, ts_run_dir)    # 2 min, 2 sec

    # Get LCD basin
    ts_target_basin = getTargetBasin(treatment_scenario)                # 2 seconds

    # Get target stream segments basins
    if ts_target_basin:
        ts_target_streams = getTargetStreamSegments(ts_target_basin)    # 0 seconds (???)
    else:
        ts_target_streams = None

    ts_network_file = createTargetStreamNetworkFile(ts_target_streams, ts_run_dir, ts_superbasin_dict['basin_dir'])

    ts_run_input_file = createInputConfig(ts_target_basin, ts_superbasin_dict, ts_run_dir, ts_veg_layer_file, ts_network_file, model_year='baseline')

    # Run DHSVM
    dhsvm_run_path = os.path.join(DHSVM_BUILD, 'DHSVM', 'sourcecode', 'DHSVM')
    num_cores = RUN_CORES
    command = "mpiexec -n %s %s %s" % (num_cores, dhsvm_run_path, ts_run_input_file)
    # print('Running command: %s' % command)
    model_start_time = datetime.now()
    os.system(command)

    read_start_time = datetime.now()
    segment_ids = [x.unit_id for x in ts_target_streams]
    readStreamFlowData(os.path.join(ts_run_dir, 'output', 'Stream.Flow'), segment_ids=segment_ids, scenario=treatment_scenario, is_baseline=False)

    # print("model started at %d:%d:%d" % (model_start_time.hour, model_start_time.minute, model_start_time.second))
    # print("read started at %d:%d:%d" % (read_start_time.hour, read_start_time.minute, read_start_time.second))

    # Remove run dir
    shutil.rmtree(ts_run_dir)
    # print("TODO: Clear cache of report!!!")
