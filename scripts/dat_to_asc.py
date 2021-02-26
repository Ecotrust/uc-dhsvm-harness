import os, sys, getopt

def main(argv):
    help_text = "usage: dat_to_asc.py -i <path to the stream.map.dat> -o <path to output ASCII file> -r <num rows> -c <num cols> -x <xll coord in 102218> -y <yll coord in 102218> -z <0 or 1 indexed>"
    ncols = 277
    nrows = 305
    xllcorner = 550510.7258
    yllcorner = 1577561.1592
    cell_size = 90
    nodata = 0
    indexing = 1

    try:
        opts, args = getopt.getopt(argv, "hi:o:r:c:x:y:z:", ["stream_map_dat=", "stream_map_asc=", "nrows=","ncols=","xllcorner=","yllcorner=","indexing="])
    except getopt.GetoptError:
        print(help_text)
        sys.exit(2)

    for (opt, arg) in opts:
        if opt == "-h":
            print(help_text)
            sys.exit()
        elif opt in ("-i", "--stream_map_dat"):
            stream_map_dat = arg
        elif opt in ("-o", "--stream_map_asc"):
            stream_map_asc = arg
        elif opt in ("-r", "--nrows"):
            nrows = int(arg)
        elif opt in ("-c", "--ncols"):
            ncols = int(arg)
        elif opt in ("-x", "--xll"):
            xllcorner = float(arg)
        elif opt in ("-y", "--yll"):
            yllcorner = float(arg)
        elif opt in ("-z", "--indexing"):
            indexing = int(arg)

    data_grid = []
    for row_id in range(nrows):
        row_data = []
        for col_id in range(ncols):
            row_data.append(0)
        data_grid.append(row_data)

    stream_data = open(stream_map_dat, 'r')
    stream_lines = stream_data.readlines()
    stream_data.close()
    for line in stream_lines:
        if line[0] != '#':
            line_data = line.split('\t')
            dat_x = int(line_data[0])    # [0] should be ''
            dat_y = int(line_data[1])
            segment_id = int(line_data[2])
            try:
                data_grid[dat_y-indexing][dat_x-indexing] = segment_id
            except IndexError as e:
                import ipdb; ipdb.set_trace()
                data_grid[dat_y-indexing][dat_x-indexing] = segment_id

    out_data = open(stream_map_asc, 'w')
    out_data.write('ncols\t%s\n' % ncols)
    out_data.write('nrows\t%s\n' % nrows)
    out_data.write('xllcorner\t%s\n' % xllcorner)
    out_data.write('yllcorner\t%s\n' % yllcorner)
    out_data.write('cellsize\t%s\n' % cell_size)
    out_data.write('NODATA_value\t%s\n' % nodata)
    for row in data_grid:
        try:
            new_line = "%s\n" % "\t".join([str(x) for x in row])
        except TypeError as e:
            import ipdb; ipdb.set_trace()
            new_line = "%s\n" % "\t".join(row)

        out_data.write(new_line)
    out_data.close()

    print('New ASCII file created at %s' % stream_map_asc)

if __name__ == "__main__":
    main(sys.argv[1:])
