import sys, statistics, shutil, os
from datetime import datetime
from django.utils.timezone import get_current_timezone
from ucsrb.models import PourPointBasin, StreamFlowReading, TreatmentScenario
from .settings import FLOW_METRICS, TIMESTEP, ABSOLUTE_FLOW_METRIC, DELTA_FLOW_METRIC, BASINS_DIR, RUNS_DIR, SUPERBASINS

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

def getRunDir(treatment_scenario):

    # Runs directory
    try:
        os.path.isdir(RUNS_DIR)
    except OSError:
        print("Runs dir not found. Add RUNS_DIR to settings")

    # Create a dir for treatment scenario run using id
    treatment_scenario_id = treatment_scenario.id
    ts_run_dir_name = 'run_' + str(treatment_scenario_id)
    ts_run_dir = os.path.join(RUNS_DIR, ts_run_dir_name)

    # TODO: if does exist delete the run dir
    if not os.path.isdir(ts_run_dir):
        os.mkdir(ts_run_dir)

    return ts_run_dir


# ======================================
# TREATMENT SCENARIO RUN SUPER BASIN
# ======================================

def getRunSuperBasinDir(treatment_scenario):

    # Original basin files directory
    # BASINS_DIR = settings.BASINS_DIR
    try:
        os.path.isdir(BASINS_DIR)
    except OSError:
        print("Basins dir not found. add BASINS_DIR to settings")
        sys.exit()

    # TreatmentScenario superbasin
    ts_superbasin = treatment_scenario.superbasin

    # Superbasin dir
    ts_superbasin_dir = os.path.join(BASINS_DIR, ts_superbasin)

    return ts_superbasin_dir


# ======================================
# CREATE TREATED VEG LAYER
# ======================================

def getVegLayer(ts_superbasin_dir, ts_run_dir):

    # Create sym link for veg layer
    os.system("ln -s %s/veg.asc.bin %s/inputs/veg.asc.bin" % (ts_superbasin_dir, ts_run_dir))


# ======================================
# Identify basin/LCD segment
# ======================================

def getTargetBasin(treatment_scenario):

    # Basin will have 1 field which is cat name of superbasin_segmentId
    # Query run against overlapping_pourpoint_basin
    if treatment_scenario.focus_area_input:
        basin = PourPointBasin.objects.filter(geom__contains=treatment_scenario.focus_area_input.geometry.order_by('area')[0])
    else:
        basin = None
        print("No TreatmentScenario focus area provided")

    return basin


# ======================================
# IDENTIFY SUB BASINS OF LCD / STEAM SEGMENTS
# ======================================

def getTargetStreamSegments(basin):

    try:
        basin_stream_segments = PourPointBasin.objects.filter(geom__within=basin.geometry)
    except Exception as e:
        print('No sub basins found within "%s"' % basin)

    return basin_stream_segments


# ======================================
# CONFIGURE TREATMENT SCENARIO RUN
# ======================================

def runHarnessConfig(treatment_scenario):
    # TreatmentScenario run directory
    ts_run_dir = getRunDir(treatment_scenario)
    ts_superbasin_dir = getRunSuperBasinDir(treatment_scenario)

    # --------------------------------------
    # Create sym links for met_data, shadows
    # --------------------------------------
    # Current location:
    os.system("ln -s %s/../met_data %s/inputs/met_data" % (ts_superbasin_dir, ts_run_dir))
    # Future location:
    # os.system("ln -s %s/../met_data /usr/local/apps/marineplanner-core/runs/%s/inputs/met_data" % (ts_superbasin_dir, ts_run_dir))

    os.system("ln -s %s/shadows %s/inputs/shadows" % (ts_superbasin_dir, ts_run_dir))

    # Create veg layer
    ts_run_veg_layer = getVegLayer(ts_superbasin_dir, ts_run_dir)

    # Get LCD basin
    ts_target_basin = getTargetBasin(treatment_scenario)

    # Get sub LCD basins
    if ts_target_basin:
        ts_target_streams = getTargetStreamSegments(ts_target_basin)
    else:
        ts_target_streams = None




# ======================================
# CREATE INPUT CONFIG FILE
# ======================================

def createInputConfig(treatment_scenario):

    # SUPERBASINS = settings.SUPERBASINS
    ts_superbasin_name = SUPERBASINS[ts_superbasin]['name'].lower()
    # TODO make lowercase name
    superbasin_input_template = os.path.join(ts_superbasin_dir, ts_superbasin_name)

    input_config = configparser.ConfigParser()
    input_config.optionxform=str
    input_config.read(dhsvm_input_config)

    # set timestep
    input_config.set("TIME", "Time Step", TIMESTEP)
