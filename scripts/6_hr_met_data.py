import os, sys, getopt, shutil

def main(argv):
    help_text = "usage: 6_hr_met_data.py -i <path to the source met_data dir> -o <path to output met_data dir>"
    valid_hours = ["00","06","12","18"]
    input_met_dir = False
    output_met_dir = False

    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["input_met_dir=", "output_met_dir="])
    except getopt.GetoptError:
        print(help_text)
        sys.exit(2)

    for (opt, arg) in opts:
        if opt == "-h":
            print(help_text)
            sys.exit()
        elif opt in ("-i", "--input_met_dir"):
            input_met_dir = arg
        elif opt in ("-o", "--output_met_dir"):
            output_met_dir = arg

    if not input_met_dir:
        print("Please provide an input (-i) directory full of met data.")
        print(help_text)
        sys.exit(1)
    if not output_met_dir:
        print("Please provide an output (-o) directory to write new met data to.")
        print(help_text)
        sys.exit(1)
    if not os.path.isdir(input_met_dir):
        print("Provided input directory '%s' not recognized, or is not a directory." % input_met_dir)
        print(help_text)
        sys.exit(1)

    if os.path.isdir(output_met_dir):
        confirmation = input('%s exists. Do you want to overwrite this directory? [y/N]\n' % output_met_dir)
        if confirmation.lower() in ['y','yes']:
            shutil.rmtree(output_met_dir)
        else:
            print("Script terminated by user.")
            sys.exit(0)

    os.mkdir(output_met_dir)

    for inmet_filename in os.listdir(input_met_dir):
        inmet_file = open(os.path.join(input_met_dir, inmet_filename), 'r')
        inlines = inmet_file.readlines()
        inmet_file.close()
        outmet_file = open(os.path.join(output_met_dir, inmet_filename), 'w')
        for line in inlines:
            if line[11:13] in valid_hours:
                outmet_file.write(line)

if __name__ == "__main__":
    main(sys.argv[1:])
