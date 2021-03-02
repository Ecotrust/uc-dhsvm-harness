import os, sys, getopt, shutil, json, statistics

from settings import FLOW_METRICS, TIMESTEP, ABSOLUTE_FLOW_METRIC, DELTA_FLOW_METRIC

def main(argv):
    help_text = "usage: get_flow_metrics.py -s <numeric segment id> -i <path to the raw Stream.Flow file> -o <path to output data file>"
    input_flow = False
    output_flow = False
    segment_id = None

    try:
        opts, args = getopt.getopt(argv, "hs:i:o:", ["segment_id=", "input_flow=", "output_flow="])
    except getopt.GetoptError:
        print(help_text)
        sys.exit(2)

    for (opt, arg) in opts:
        if opt == "-h":
            print(help_text)
            sys.exit()
        elif opt in ("-s", "--segment_id"):
            segment_id = arg
        elif opt in ("-i", "--input_flow"):
            input_flow = arg
        elif opt in ("-o", "--output_flow"):
            output_flow = arg

    if not segment_id:
        print("No 'segment id' provided.")
        confirmation = input('Do you want to interpret ALL stream segments? [y/N]\n')
        if confirmation.lower() in ['y','yes']:
            segment_id = 'all'
        else:
            print("Script terminated by user.")
            sys.exit(0)
    if not input_flow:
        print("Please provide an input (-i) file of flow data (Stream.Flow).")
        print(help_text)
        sys.exit(1)
    if not output_flow:
        print("Please provide an output (-o) file to write the interpreted flow data to.")
        print(help_text)
        sys.exit(1)
    if not os.path.isfile(input_flow):
        print("Provided input directory '%s' not recognized, or is not a directory." % input_flow)
        print(help_text)
        sys.exit(1)

    if os.path.isfile(output_flow):
        confirmation = input('%s exists. Do you want to overwrite this file? [y/N]\n' % output_flow)
        if confirmation.lower() in ['y','yes']:
            os.remove(output_flow)
        else:
            print("Script terminated by user.")
            sys.exit(0)

    input_flow_file = open(input_flow, 'r')
    inlines = input_flow_file.readlines()
    input_flow_file.close()

    if not segment_id == 'all':
        flow_json = aggregate_flow_results(inlines, segment_id)
    else:
        flow_json = aggregate_flow_results(inlines)

    with open(output_flow, 'w') as output_flow_file:
         json.dump(flow_json, output_flow_file)

def get_segment_id_list(inlines):
    segment_id = []
    for line in inlines:
        line_list = line.split()
        if line_list[-1] == '"Totals"':
            return segment_id
        segment_id.append(line_list[-1].split('"')[1])
    return segment_id

def aggregate_flow_results(inlines, segment_ids='all'):
    if not isinstance(segment_ids, list):
        if isinstance(segment_ids, int):
            segment_ids = ["shed_%s" % segment_ids]
        elif isinstance(segment_ids, str):
            if segment_ids == 'all':
                segment_ids = get_segment_id_list(inlines)
            elif isinstance(int(segment_ids.split('shed_')[1]), int):
                segment_ids = [segment_ids]
            else:
                print("Unknown segment ID value: '%s'. Quitting...\n" % segment_ids)
                sys.exit(1)
        else:
            print("Unknown segment ID value: '%s'. Quitting...\n" % segment_ids)
            sys.exit(1)

    # if segment_ids == 'all':
    #     return_val = metrics
    # else:
    #     return_val = {}
    #     for segment in segment_ids:
    #         return_val[segment] = metrics[segment]
    # return return_val
    return get_metric_flow(inlines, segment_ids)

def get_metric_flow(inlines, segment_ids=False):
    readings_per_day = 24/TIMESTEP
    segments = {}
    # Stream Flow Columns:
    #   https://www.pnnl.gov/sites/default/files/media/file/Network%20segment%20output%20file.pdf
    #   0   str     Time Stamp
    #   1   int     Segment ID
    #   2   float   inflow (m^3/timestep)
    #   3   float   lateral inflow
    #   4   float   outflow
    #   5   float   [
    #               single segment line: Delta in segment storage
    #               Total line: total network storage
    #           ]
    #   6   fl/str  [
    #               single segment line: str, segment title
    #               Total line: float, Delta network storage
    #           ]
    #   7   (T)fl   Estimate of mass balance error
    #   8   (T)str  "Totals" identifier
    for line in inlines:
        data = line.split()
        segment_name = data[-1].split('"')[1]
        timestamp = data[0]
        if not segment_ids or segment_name in segment_ids:
            if not segment_name in segments.keys() and segment_ids and segment_name in segment_ids:
                segments[segment_name] = {}     # metric dict for this segment
                for metric_key in FLOW_METRICS.keys():
                    segments[segment_name][metric_key] = []
            for metric_key in FLOW_METRICS.keys():
                if FLOW_METRICS[metric_key]['measure'] == 'abs':    # Establish abs flow rates/deltas for future reference
                    if FLOW_METRICS[metric_key]['delta']:
                        segments[segment_name][metric_key].append({
                            'timestep': timestamp,
                            'value': float(data[5])/float(TIMESTEP)
                        })
                    else:
                        segments[segment_name][metric_key].append({
                            'timestep': timestamp,
                            'value': float(data[4])/float(TIMESTEP)
                        })
                else:
                    relevant_readings = int(FLOW_METRICS[metric_key]['period']*readings_per_day)
                    if not FLOW_METRICS[metric_key]['delta']:
                        readings = [x['value'] for x in segments[segment_name][ABSOLUTE_FLOW_METRIC][-relevant_readings:]]
                        if FLOW_METRICS[metric_key]['measure'] == 'mean':
                            segments[segment_name][metric_key].append({
                                'timestep': timestamp,
                                'value': statistics.mean(readings)
                            })
                        else:
                            readings.sort()
                            segments[segment_name][metric_key].append({
                                'timestep': timestamp,
                                'value': readings[0]
                            })
                    else:
                        try:
                            previous_value = segments[segment_name][metric_key][-1]['value']
                        except IndexError:
                            previous_value = False
                        if FLOW_METRICS[metric_key]['period'] == 7:
                            period = "Seven"
                        else:
                            period = "One"
                        source_metric = "%s Day %s Flow" % (period, FLOW_METRICS[metric_key]['measure'].title())
                        readings = [x['value'] for x in segments[segment_name][source_metric][-relevant_readings:]]
                        if FLOW_METRICS[metric_key]['measure'] == 'mean':
                            if previous_value:
                                value = statistics.mean(readings)-previous_value
                            else:
                                value = 0
                            segments[segment_name][metric_key].append({
                                'timestep': timestamp,
                                'value': value
                            })
                        else:
                            readings.sort()
                            if previous_value:
                                value = readings[0]-previous_value
                            else:
                                value = 0
                            segments[segment_name][metric_key].append({
                                'timestep': timestamp,
                                'value': value
                            })



    return segments


if __name__ == "__main__":
    main(sys.argv[1:])
