import os, sys, getopt, shutil, csv, pathlib

def main(argv):
    help_text = "usage: update_stream_network_files.py -i <path to the pour point csv>"

    CURRENT_DIR = pathlib.Path(__file__).parent.absolute()
    BASINS_DIR = os.path.join(CURRENT_DIR, '..', 'basins')
    DEFAULT_CSV = os.path.join(BASINS_DIR, 'uc_ppts.csv')

    input_csv = DEFAULT_CSV

    try:
        opts, args = getopt.getopt(argv, "hi:", ["input_csv=",])
    except getopt.GetoptError:
        print(help_text)
        sys.exit(2)

    for (opt, arg) in opts:
        if opt == "-h":
            print(help_text)
            sys.exit()
        elif opt in ("-i", "--input_csv"):
            input_csv = arg

    if not input_csv:
        print("Please provide an input (-i) csv file representing all pourpoints across the study area.")
        print(help_text)
        sys.exit(1)
    if not os.path.isfile(input_csv):
        print("Provided input file '%s' not found" % input_csv)
        print(help_text)
        sys.exit(1)

    METW_SOURCE_NETWORK = os.path.join(BASINS_DIR, 'methow', 'inputs', 'stream.network_clean.dat')
    WENA_SOURCE_NETWORK = os.path.join(BASINS_DIR, 'wena', 'inputs', 'stream.network_clean.dat')
    OKAN_SOURCE_NETWORK = os.path.join(BASINS_DIR, 'okan', 'inputs', 'stream.network_clean.dat')
    ENTI_SOURCE_NETWORK = os.path.join(BASINS_DIR, 'entiat', 'inputs', 'stream.network_clean.dat')
    METW_OUTPUT = os.path.join(BASINS_DIR, 'methow', 'inputs', 'stream.network_all.dat')
    WENA_OUTPUT = os.path.join(BASINS_DIR, 'wena', 'inputs', 'stream.network_all.dat')
    OKAN_OUTPUT = os.path.join(BASINS_DIR, 'okan', 'inputs', 'stream.network_all.dat')
    ENTI_OUTPUT = os.path.join(BASINS_DIR, 'entiat', 'inputs', 'stream.network_all.dat')

    # Read the CSV into a series of dictionaries
    ppt_reader = csv.DictReader(open(input_csv))

    # Loop through the list of dictionaries and capture the ppt ids present in each superbasin
    basin_dict = {
        'metw': {
            'input': METW_SOURCE_NETWORK,
            'output': METW_OUTPUT,
            'ids': []
        },
        'wena': {
            'input': WENA_SOURCE_NETWORK,
            'output': WENA_OUTPUT,
            'ids': []
        },
        'okan': {
            'input': OKAN_SOURCE_NETWORK,
            'output': OKAN_OUTPUT,
            'ids': []
        },
        'enti': {
            'input': ENTI_SOURCE_NETWORK,
            'output': ENTI_OUTPUT,
            'ids': []
        },
    }

    for ppt in ppt_reader:
        for basin_key in basin_dict.keys():
            if basin_key in ppt['seg_ID']:
                basin_dict[basin_key]['ids'].append(int(ppt['POINTID']))
                break

    # For each superbasin, go line-by-line through stream.network_clean.dat and
    #   if the line ID is present in the superbasin's basin_dict ids list, add the
    #   'SAVE' flag.

    for basin_key in basin_dict.keys():
        basin = basin_dict[basin_key]
        with open(basin['input'], 'r') as infile:
            with open(basin['output'], 'w') as outfile:
                for line in infile.readlines():
                    ppt_id = line.split()[0]
                    if int(ppt_id) in basin['ids']:
                        outline = '\t'.join([line.split('\n')[0], 'SAVE"%s_%s"' % (basin_key, ppt_id), '\n'])
                    else:
                        outline = line
                    outfile.write(outline)

if __name__ == "__main__":
    main(sys.argv[1:])
