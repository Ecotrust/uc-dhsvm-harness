import sys, statistics, shutil
from datetime import datetime
from django.utils.timezone import get_current_timezone
from ucsrb.models import PourPointBasin, StreamFlowReading, TreatmentScenario
from .settings import FLOW_METRICS, TIMESTEP, ABSOLUTE_FLOW_METRIC, DELTA_FLOW_METRIC

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
                # segment_readings.append({'timestamp':timestamp, 'reading':reading})

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
                        segment_readings[metric_key].append({
                            'timestamp': timestamp,
                            'value': value
                        })
                    else:
                        relevant_readings = int(FLOW_METRICS[metric_key]['period']*readings_per_day)
                        if not FLOW_METRICS[metric_key]['delta']:
                            readings = [x['value'] for x in segment_readings[ABSOLUTE_FLOW_METRIC][-relevant_readings:]]
                            if FLOW_METRICS[metric_key]['measure'] == 'mean':
                                value = statistics.mean(readings)
                            else:
                                readings.sort()
                                value = readings[0]
                            segment_readings[metric_key].append({
                                'timestamp': timestamp,
                                'value': value
                            })
                        else:
                            try:
                                previous_value = segment_readings[metric_key][-1]['value']
                            except IndexError:
                                previous_value = False
                            if FLOW_METRICS[metric_key]['period'] == 7:
                                period = "Seven"
                            else:
                                period = "One"
                            source_metric = "%s Day %s Flow" % (period, FLOW_METRICS[metric_key]['measure'].title())
                            readings = [x['value'] for x in segment_readings[source_metric][-relevant_readings:]]
                            if FLOW_METRICS[metric_key]['measure'] == 'mean':
                                if previous_value:
                                    value = statistics.mean(readings)-previous_value
                                else:
                                    value = 0
                            else:
                                readings.sort()
                                if previous_value:
                                    value = readings[0]-previous_value
                                else:
                                    value = 0
                            # segment_readings[metric_key].append({
                            #     'timestep': timestamp,
                            #     'value': value
                            # })

                    StreamFlowReading.objects.create(
                        timestamp=timestamp,
                        time=time,
                        basin=basin,
                        metric=metric_key,
                        is_baseline=is_baseline,
                        treatment=scenario,
                        value=value
                    )
