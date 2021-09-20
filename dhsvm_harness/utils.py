# import configparser
from datetime import datetime
from django.conf import settings as ucsrb_settings
from django.template import Template, Context
from django.utils.timezone import get_current_timezone
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon
from django.contrib.gis.gdal.error import GDALException
from django.contrib.gis.db.models.aggregates import Union
from django.db import transaction, connection
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
from ucsrb.models import StreamFlowReading, TreatmentScenario, FocusArea, TreatmentArea, VegPlanningUnit
from ucsrb.views import break_up_multipolygons
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

def getBasinLineInsertTuple(line, basin_name, is_baseline, scenario):
    tz = get_current_timezone()
    data = line.split()
    reading = data[4]
    timestamp = data[0]
    treatment_id = scenario.pk if scenario else "NULL"

    return "('{TIMESTAMP}', '{TIME}'::timestamptz, '{SEGMENT_ID}', '{METRIC}', {IS_BASELINE}, {TREATMENT_ID}, {VALUE})".format(
        TIMESTAMP=timestamp,
        TIME='T'.join(str(tz.localize(datetime.strptime(timestamp, "%m.%d.%Y-%H:%M:%S"))).split(' ')),
        SEGMENT_ID=basin_name,
        METRIC=ABSOLUTE_FLOW_METRIC,
        IS_BASELINE=str(is_baseline).lower(),
        TREATMENT_ID=treatment_id,
        VALUE=float(reading)/float(TIMESTEP)
    )

def importBasinLines(inlines, segment_ids, scenario, is_baseline):
    insert_command_start = ('INSERT INTO '
                        '"ucsrb_streamflowreading" ( '
                        '"timestamp", "time", "segment_id", '
                        '"metric", "is_baseline", '
                        '"treatment_id", "value"'
                        ') VALUES '
    )

    insert_command_end = ''
    insert_command_lines = []

    for line in inlines:
        try:
            basin_name = line.split('"')[1]
            if basin_name in segment_ids:
                insert_command_lines.append(getBasinLineInsertTuple(line, basin_name, is_baseline, scenario))
        except IndexError as e:
            # handle empty lines (why would we have these?)
            pass
        if basin_name in segment_ids:
            insert_command_lines.append(getBasinLineInsertTuple(line, basin_name, is_baseline, scenario))

    connection.cursor().execute("{START} {VALUES} {END}; COMMIT;".format(
        START=insert_command_start,
        VALUES=','.join(insert_command_lines),
        END=insert_command_end
    ))

# RDH 2021-09-03
# Prior to using raw SQL for bulk inserts, 'atomic'' saved a lot of time.
# However, it doesn't seem much faster now we have bulk inserts, and not
# dealing with locking the table seems a good strategy.
# @transaction.atomic
def readStreamFlowData(flow_file, segment_ids=None, scenario=None, is_baseline=True):
    flow_file_dir = os.path.split(flow_file)[0]
    status_log = os.path.join(flow_file_dir, 'dhsvm_status.log')
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

    if scenario:
        old_records = StreamFlowReading.objects.filter(time__gte=start_time, time__lte=end_time, segment_id__in=segment_ids, treatment=scenario)
        print('purging {} obsolete records...'.format(old_records.count()))
        old_records.delete()
    if is_baseline: # if not scenario or scenario.prescription_treatment_selection == 'notr':
        StreamFlowReading.objects.filter(time__gte=start_time, time__lte=end_time, segment_id__in=segment_ids, is_baseline=True, treatment=None).delete()

    inline_count = len(inlines)
    print('Importing {} data records...'.format(inline_count))
    if inline_count < 10000:
        if scenario:
            importBasinLines(inlines, segment_ids, scenario, is_baseline)
        if is_baseline: # if not scenario or scenario.prescription_treatment_selection == 'notr':
            importBasinLines(inlines, segment_ids, None, True)

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
            with open(status_log, "w+") as f:
                progress = int((filecount/len(split_files))*100)
                f.write(str(progress))
            with open(file_slice, 'r') as f:
                inlines=f.readlines()

            if scenario:
                importBasinLines(inlines, segment_ids, scenario, is_baseline)
            if is_baseline: # not scenario or scenario.prescription_treatment_selection == 'notr':
                importBasinLines(inlines, segment_ids, None, True)

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
            command = "chgrp www-data %s" % RUNS_DIR

    except OSError:
        print("Runs dir not found. Add RUNS_DIR to settings")

    # Create a dir for treatment scenario run using id
    if hasattr(treatment_scenario, 'id'):
        treatment_scenario_id = treatment_scenario.id
    else:
        treatment_scenario_id = 'baseline_{}'.format(treatment_scenario)
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

def identifyBestParentBasin(treatment_geometry):
    # Attempting to apply lessons about determining intersection area here: https://stackoverflow.com/a/58818873/706797

    overlapping_basins = FocusArea.objects.filter(unit_type='PourPointOverlap', geometry__contains=treatment_geometry)
    if len(overlapping_basins) > 1:
        # grab the second-smallest containing basin so we get at least 1 downstream ppt
        return sorted(overlapping_basins, key=lambda x: x.geometry.area)[1]
    elif len(overlapping_basins) == 1:
        return overlapping_basins[0]
    else: # none found
        overlapping_basins = FocusArea.objects.filter(unit_type='PourPointOverlap', geometry__intersects=treatment_geometry)
        overlap_scores = []
        for overlapping_basin in overlapping_basins:
            try:
                overlap_area = overlapping_basin.geometry.intersection(treatment_geometry).area
            except Exception as e:
                # GEOS doesn't like the geometries as is. Intersecting them with themselves makes GEOS happy
                clean_basin = overlapping_basin.geometry.intersection(overlapping_basin.geometry)
                overlap_area = clean_basin.intersection(treatment_geometry).area
            overlap_scores.append({
                'id':overlapping_basin.id,
                'unit_id': overlapping_basin.unit_id,
                'area':overlapping_basin.geometry.area,
                'intersection_area':overlap_area
            })
        top_score = 0
        for score in overlap_scores:
            if score['intersection_area'] > top_score:
                top_score = score['intersection_area']
        best_basins = [x for x in overlap_scores if x['intersection_area'] == top_score]
        best_basin = best_basins[0]
        for basin in best_basins:
            if basin['area'] < best_basin['area']:
                best_basin = basin
        best_basin_focus_area = FocusArea.objects.get(id=best_basin['id'])
        return best_basin_focus_area


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
        parent_basin = identifyBestParentBasin(treatment_scenario.focus_area_input.geometry)
        ts_superbasin_code = parent_basin.unit_id.split('_')[0]

    # Superbasin dir
    ts_superbasin_dir = SUPERBASINS[ts_superbasin_code]['inputs']

    return {
        'basin_dir': ts_superbasin_dir,
        'basin_code': ts_superbasin_code
    }

# ======================================
# CONVERT TREATMENT AREAS TO VPU GEOMETRIES
# ======================================

def dissolveTreatmentGeometries(treatment_areas):
    ta_geom_union = treatment_areas.aggregate(Union('geometry'))
    if ta_geom_union:
        ta_geom = ta_geom_union['geometry__union']
        vpus = VegPlanningUnit.objects.filter(geometry__intersects=ta_geom)
        try:
            dissolved_geom = vpus.aggregate(Union('geometry'))
            if dissolved_geom:
                dissolved_geom = dissolved_geom['geometry__union']
                if type(dissolved_geom) == MultiPolygon:
                    return dissolved_geom
                elif type(dissolved_geom) == Polygon:
                    try:
                        return MultiPolygon(dissolved_geom, srid=dissolved_geom.srid)
                    except:
                        return None
        except:
            pass
    return None

# ======================================
# CREATE TREATED VEG LAYER
# ======================================

def setVegLayers(treatment_scenario, ts_superbasin_dict, ts_run_dir):

    ts_superbasin_dir = ts_superbasin_dict['basin_dir']
    ts_superbasin_code = ts_superbasin_dict['basin_code']

    # inputs for TreatmentScenario to feed into DHSVM
    ts_run_dir_inputs = os.path.join('%s/ts_inputs' % ts_run_dir)

    # Start a rasterio environment
    with rasterio.Env():
        # Projection assigned for later use
        # TODO: add to settings
        PROJECTION = 'PROJCS["NAD_1983_USFS_R6_Albers",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",600000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-120.0],PARAMETER["Standard_Parallel_1",43.0],PARAMETER["Standard_Parallel_2",48.0],PARAMETER["Latitude_Of_Origin",34.0],UNIT["Meter",1.0]]'
        # Transform and reproject TS shape/feature to match UCSRB data bin files
        tfm = partial(pyproj.transform, pyproj.Proj("epsg:3857"), pyproj.Proj(PROJECTION))

        # OPEN:
        # Baseline Veg Layer
        baseline_veg_file = rasterio.open("%s/inputs/veg_files/%s_notr.tif" % (ts_superbasin_dir, ts_superbasin_code), "r")

        # create rx dict
        rx_dict = {}
        # collect all features by rx in dict
        if hasattr(treatment_scenario, 'id'):   # otherwise this is a baseline run
            for rx_def in ucsrb_settings.PRESCRIPTION_TREATMENT_CHOICES:
                rx_id = rx_def[0]
                queryset = treatment_scenario.treatmentarea_set.filter(prescription_treatment_selection=rx_id)
                if queryset.count() > 0:

                    rx_dict[rx_id] = {
                    'name': rx_def[1],
                    # convert each group of rx features into single multipolygon
                    'geometry': dissolveTreatmentGeometries(queryset),
                    # Treatment Veg Layer
                    'veg_file': rasterio.open("%s/inputs/veg_files/%s_%s.tif" % (ts_superbasin_dir, ts_superbasin_code, rx_id), "r"),
                    }
                    if rx_dict[rx_id]['geometry']:
                        feature = json.loads(rx_dict[rx_id]['geometry'].json)
                        feature_shape = shape(feature)
                        rx_dict[rx_id]['shape'] = shapely.ops.transform(tfm, feature_shape)
                    else:
                        rx_dict[rx_id]['shape'] = None

        profile = baseline_veg_file.profile

        profile.update(
            dtype=rasterio.uint8,
            driver='AAIGrid'
        )

        datasets = []

        # TERMINOLOGY:
        # Mask -
        #   Noun: a shape/feature area of interest within a "larger area"
        #   Verb: Use an AOI feature to identify the part of the layer we are concerned with
        # Clip -
        #   Noun (uncommon): new dataset with bounds of original "larger area" that preserves only data that intersects the mask area of interest
        #       - Normally this would be called the 'clipped result'
        #   Verb: To remove unwanted data (outside of n. mask), leaving only the data we are interested in.

        # loop through rxs, clipping each related veg layer to be added to the base
        for rx_id in rx_dict.keys():
            # skip notr (it will be the base layer)
            if rx_dict[rx_id]['shape'] and not rx_id == 'notr' :
                tmpfile = tempfile.NamedTemporaryFile()

                # RDH 2021-09-17: `with rasterio.open(tmpfile,...) as foo` gives BuffDataset writer
                # but `foo = rasterio.open(tmpfile,...)` gives _GeneratorContextManager
                # handing the tmpfile name to rasterio fixes the issue.
                dataset = rasterio.open(tmpfile.name, 'w+', **profile)
                clipped_treatment = mask(rx_dict[rx_id]['veg_file'], rx_dict[rx_id]['shape'], nodata=0)
                clipped_treatment_mask = clipped_treatment[0]

                dataset.write(clipped_treatment_mask.astype(rasterio.uint8))
                datasets.append(dataset)

        datasets.append(baseline_veg_file)
        merged_veg = merge(datasets, nodata=0, dtype="uint8")
        merged_veg_layer = merged_veg[0]

        for dataset in datasets:
            dataset.close()

        asc_file = "%s/ts_clipped_treatment_layer.asc" % ts_run_dir_inputs
        with rasterio.open(asc_file, 'w', **profile) as dst:
            dst.write(merged_veg_layer)

        for rx_id in rx_dict.keys():
            rx_dict[rx_id]['veg_file'].close()
        if not 'notr' in rx_dict.keys():
            baseline_veg_file.close()

    return asc_file + '.bin'

def binAsciis(ts_run_dir):

    # Remove header from asciis
    ##########################
    ts_run_dir_inputs = os.path.join('%s/ts_inputs' % ts_run_dir)

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



# ======================================
# Identify basin
# ======================================

def getTargetBasin(treatment_scenario):

    target_basin = None

    # Basin will have 1 field which is cat name of superbasin_segmentId
    # Query run against overlapping_pourpoint_basin
    try:
        if hasattr(treatment_scenario, 'focus_area_input') and treatment_scenario.focus_area_input:
            basin_geometry = treatment_scenario.focus_area_input.geometry
        else:
            basin_geometry = FocusArea.objects.get(unit_type='PourPointOverlap', unit_id=ucsrb_settings.BASIN_RESET_LOOKUP[treatment_scenario.lower()]['UNIT_ID']).geometry

        target_basins =  FocusArea.objects.filter(unit_type="PourPointOverlap", geometry__contains=basin_geometry)
        if len(target_basins) > 1:
            target_basin = sorted(target_basins, key=lambda x: x.geometry.area)[1]
        elif len(target_basins) == 1:
            target_basin = target_basins[0]
        else:
            target_basin = identifyBestParentBasin(treatment_scenario.focus_area_input.geometry)
    except Exception as e:
        print(e)
        print("No TreatmentScenario focus area provided")

    return target_basin


# ======================================
# IDENTIFY SUB BASINS OF LCD / STEAM SEGMENTS
# ======================================

def getTargetStreamSegmentBasins(basin):

    try:
        # 'contained' is used because 'within' is not working with the current matching of Overlapping basins.
        #   While a few extra basins may get caught up in this, there is no real harm done.
        basin_stream_segment_basins = FocusArea.objects.filter(unit_type='PourPointOverlap', geometry__contained=basin.geometry)
    except Exception as e:
        basin_stream_segment_basins = None
        print('No sub basins found within "%s"' % basin)

    return basin_stream_segment_basins

# ======================================
# IDENTIFY SUB BASINS OF LCD / STEAM SEGMENTS
# ======================================

def createTargetStreamNetworkFile(ts_target_stream_basins, ts_run_dir, ts_superbasin_dir):
    # return os.path.join(ts_superbasin_dir, 'inputs', 'stream.network_all.dat')
    infile_name = os.path.join(ts_superbasin_dir, 'inputs', 'stream.network_clean.dat')
    outfile_name = os.path.join(ts_run_dir, 'ts_inputs', 'stream.network.dat')
    if ts_target_stream_basins == None:
        all_segments_file = os.path.join(ts_superbasin_dir, 'inputs', 'stream.network_all.dat')
        shutil.copyfile(all_segments_file, outfile_name)
    else:
        # loop through target segment ids, creating a list of the related integers
        segment_dict = {}
        for basin in ts_target_stream_basins:
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
    ts_superbasin_dir = SUPERBASINS[ts_superbasin_code]['inputs']

    # Get superbasin input config file
    # ts_superbasin_input_template_name = 'INPUT.UCSRB.%s.bck' % ts_superbasin_name
    ts_superbasin_input_template_name = 'INPUT.UCSRB.%s' % ts_superbasin_name
    ts_superbasin_input_template = os.path.join(ts_superbasin_dict['basin_dir'], ts_superbasin_input_template_name)

    # Location for new run input config file
    ts_run_input_file = os.path.join(ts_run_dir, 'INPUT.UCSRB.run')

    # mask_file = os.path.join(ts_superbasin_dict['basin_dir'], 'masks', "%s.asc.bin" % ts_target_basin.unit_id)
    mask_file = os.path.join(ts_run_dir, 'ts_inputs', "mask.asc.bin")

    createMaskFile(ts_superbasin_code, ts_superbasin_dir, ts_target_basin, ts_run_dir)

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
# CREATE MASK FILE
# ======================================
def createMaskFile(basin_id, ts_superbasin_dir, focus_area, ts_run_dir):
    ts_run_dir_inputs = os.path.join('%s/ts_inputs' % ts_run_dir)
    # get focus_area geometry
    # CREATE treatment shape/feature from TreatmentScenario geometry
    try:
        feature = json.loads(focus_area.geometry.json)
    except GDALException as e:
        # There is a GEOS error here, possibly related to Rasterio:
        #   https://github.com/mapbox/rasterio/issues/888
        # However, we can get wkt, if not json, and can create a functional GEOSGeometry from that.
        geos_feature = GEOSGeometry(focus_area.geometry.wkt)
        feature = json.loads(geos_feature.json)
    feature_shape = shape(feature)


    # Transform and reproject TS shape/feature to match UCSRB data bin files
    PROJECTION = 'PROJCS["NAD_1983_USFS_R6_Albers",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",600000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-120.0],PARAMETER["Standard_Parallel_1",43.0],PARAMETER["Standard_Parallel_2",48.0],PARAMETER["Latitude_Of_Origin",34.0],UNIT["Meter",1.0]]'
    tfm = partial(pyproj.transform, pyproj.Proj("epsg:3857"), pyproj.Proj(PROJECTION))
    ts_shape = shapely.ops.transform(tfm, feature_shape)
    # get superbasin bounds/header values
    # rasterize the geometry (to ascii)
    #   Read in full basin mask raster

    # basin_blank_file = rasterio.open("%s/masks/%s_blank.tif" % (ts_superbasin_dir, basin_id), "r")
    basin_mask_file = rasterio.open("%s/masks/%s_mask.tif" % (ts_superbasin_dir, basin_id), "r")
    #   Read in blank (0) raster of basin dimensions
    clipped_treatment = mask(basin_mask_file, ts_shape, nodata=0)
    clipped_treatment_mask = clipped_treatment[0]

    profile = basin_mask_file.profile
    # convert to asc.bin
    # save file to filename
    # with tempfile.NamedTemporaryFile() as tmpfile:
    profile.update(
        dtype=rasterio.uint8,
        driver='AAIGrid'
    )

    with rasterio.open("%s/mask.asc" % ts_run_dir_inputs, 'w', **profile) as dst:
        # dst.write(merged_veg_layer)
        dst.write(clipped_treatment_mask)

    # basin_blank_file.close()
    basin_mask_file.close()

# ======================================
# CONFIGURE TREATMENT SCENARIO RUN
# ======================================

def runHarnessConfig(treatment_scenario, basin=None):
    if treatment_scenario:
        # identify super dir to copy original files from
        ts_superbasin_dict = getRunSuperBasinDir(treatment_scenario)        # 2 seconds

        # TreatmentScenario run directory
        ts_run_dir = getRunDir(treatment_scenario, ts_superbasin_dict)      # 0 seconds

        # Create run layer
        ts_veg_layer_file = setVegLayers(treatment_scenario, ts_superbasin_dict, ts_run_dir)    # 2 min, 2 sec

        # Get LCD basin
        ts_target_basin = getTargetBasin(treatment_scenario)                # 2 seconds
    elif basin:
        basin_code = ucsrb_settings.BASIN_RESET_LOOKUP[basin.lower()]['BASIN_ID']
        ts_superbasin_dict = {
            'basin_dir': SUPERBASINS[basin_code]['inputs'],
            'basin_code': basin_code
        }
        ts_run_dir = getRunDir(basin_code, ts_superbasin_dict)
        ts_veg_layer_file = setVegLayers(basin_code, ts_superbasin_dict, ts_run_dir)
        ts_target_basin = getTargetBasin(basin)

    # Get target stream segments basins
    if ts_target_basin:
        ts_target_stream_basins = getTargetStreamSegmentBasins(ts_target_basin)    # 0 seconds (???)
    else:
        ts_target_stream_basins = None

    ts_network_file = createTargetStreamNetworkFile(ts_target_stream_basins, ts_run_dir, ts_superbasin_dict['basin_dir'])

    # TODO: run for all three weather years: ['wet', 'baseline', 'dry']
    ts_run_input_file = createInputConfig(ts_target_basin, ts_superbasin_dict, ts_run_dir, ts_veg_layer_file, ts_network_file, model_year='baseline')

    # Convert all .asc to .asc.bin
    binAsciis(ts_run_dir)

    # Run DHSVM
    dhsvm_run_path = os.path.join(DHSVM_BUILD, 'DHSVM', 'sourcecode', 'DHSVM')
    num_cores = RUN_CORES

    command = "mpiexec -n %s %s %s" % (num_cores, dhsvm_run_path, ts_run_input_file)
    print('Running command: %s' % command)

    model_start_time = datetime.now()
    os.system(command)

    read_start_time = datetime.now()
    segment_ids = [x.unit_id for x in ts_target_stream_basins]
    flow_file = os.path.join(ts_run_dir, 'output', 'Stream.Flow')
    is_baseline = False if treatment_scenario else True
    readStreamFlowData(flow_file, segment_ids=segment_ids, scenario=treatment_scenario, is_baseline=is_baseline)

    # Remove run dir
    shutil.rmtree(ts_run_dir)

    finish_time = datetime.now()
    print("model started at %d:%d:%d" % (model_start_time.hour, model_start_time.minute, model_start_time.second))
    print("read started at %d:%d:%d" % (read_start_time.hour, read_start_time.minute, read_start_time.second))
    print("harness finished at %d:%d:%d" % (finish_time.hour, finish_time.minute, finish_time.second))
