import sys
import os
import io
import logging
import getopt
import shutil
import rasterio
import zipfile
import configparser
import json

import settings

def clip_stream_map(mask_dict, source_map_file, out_map_file):
    MASK_HEADER_OFFSET = 6
    MASK_DELIMITER = '\t'
    SOURCE_HEADER_OFFSET = 0
    SOURCE_DELIMITER = '\t'
    # values are 1-indexed, and start in the Upper Left Corner
    try:
        sys.path.append(os.path.join(os.getcwd(), 'masks'))
        from parent_basin import parent_basin
    except Exception as e:
        print("Parent Basin not defined in %s. Please create this file." % os.path.join(os.getcwd(), 'masks', 'parent_basin.py'))
        sys.exit()

    # get row/column diffs
    col_diff = int((float(mask_dict['xllcorner'])-float(parent_basin['xllcorner']))/90)
    row_diff = int((float(parent_basin['extreme_north'])-float(mask_dict['extreme_north']))/90)
    # Get min/max row values
    # min_row_ID = row_diff
    min_row_ID = row_diff + 1 # IDs are 1-indexed!
    max_row_ID = row_diff + mask_dict['nrows']
    # Get min/max column values
    # min_col_ID = col_diff
    min_col_ID = col_diff + 1 # IDs are 1-indexed!
    max_col_ID = col_diff + mask_dict['ncols']
    # read/translate old stream.map.dat into new clipped stream.map.dat
    source_file = open(source_map_file, 'r')
    source_lines = source_file.readlines()
    source_file.close()
    if source_lines[SOURCE_HEADER_OFFSET].count(SOURCE_DELIMITER) < 2:
        if SOURCE_DELIMITER == '\t':
            SOURCE_DELIMITER = ' '
        else:
            SOURCE_DELIMITER = '\t'

    mask_file = open(mask_dict['mask_file'], 'r')
    mask_lines = mask_file.readlines()
    mask_file.close()
    if not mask_lines[MASK_HEADER_OFFSET].count(MASK_DELIMITER) >= (int(mask_dict['ncols'])-1):
        if MASK_DELIMITER == '\t':
            MASK_DELIMITER = ' '
        else:
            MASK_DELIMITER = '\t'

    out_file = open(out_map_file, 'w')
    #       Segment Cut/Bank        Cut     Segment
    #       Col     Row     ID      Length  Height  Width   Aspect  SINK?
    #       (m)     (m)     (m)     (d)     (optional)
    #

    new_row_vals = []
    for line in source_lines:
        vals = line.split(SOURCE_DELIMITER)
        # vals = list(filter((SOURCE_DELIMITER).__ne__, vals))        # remove duplicate delimiter chars
        # vals = [x for x in vals if x != SOURCE_DELIMITER]        # remove duplicate delimiter chars
        vals = [x for x in vals if x != '']                     # remove blank values from back-to-back delimiters
        try:
            if len(vals) >= 3 and len(vals[0]) > 0 and vals[0][0] != '#':   # ['', '503', '20', '1', '90', '0.95', '0.3', '270', '\n']
                # vals[0] == 'Column'
                if int(vals[0]) >= min_col_ID and int(vals[0]) <= max_col_ID:
                    #vals[1] == 'Row'
                    if int(vals[1]) >= min_row_ID and int(vals[1]) <= max_row_ID:
                        new_row_vals = [x for x in vals]
                        # update the column ID
                        new_row_vals[0] = str(int(vals[0]) - col_diff)
                        # update the row ID
                        new_row_vals[1] = str(int(vals[1]) - row_diff)
                        if int(mask_lines[int(new_row_vals[1])-1+MASK_HEADER_OFFSET].split(MASK_DELIMITER)[int(new_row_vals[0])-1]) != mask_dict['NODATA_value']: # 1-indexed
                            out_file.write('\t'.join(new_row_vals))
        except IndexError as e:
            import ipdb; ipdb.set_trace()
            print(e)
        except Exception as e:
            print('Unknown exception "%s" occurred' % e)
            sys.exit()
    if len(new_row_vals) < 1:
        import ipdb; ipdb.set_trace()
    out_file.close()



def main(argv):
    dhsvm_input_config = False
    dhsvm_build_path = settings.DHSVM_BUILD
    basin_name = settings.DEFAULT_BASIN_NAME
    mask = settings.SOURCE_INPUT[basin_name]['inputs']['mask']
    num_cores = 2
    # root_path = os.path.dirname(os.path.abspath(__file__))
    # path = root_path
    help_text = "usage: run_prep_dhsvm_input.py -m <path to the mask shp> -i <absolute path to basin's DHSVM input config file> -d <absolute path to DHSVM build directory>"

    try:
        opts, args = getopt.getopt(argv, "hm:i:d:b:n:", ["mask=", "dhsvm_input_config=", "dhsvm_build_path=","basin_name=","num_cores="])
    except getopt.GetoptError:
        print(help_text)
        sys.exit(2)


    for (opt, arg) in opts:
        if opt == "-h":
            print(help_text)
            sys.exit()
        elif opt in ("-m", "--mask"):
            mask = arg
        elif opt in ("-i", "--dhsvm_input_config"):
            dhsvm_input_config = arg
        elif opt in ("-b", "--basin"):
            basin_name = arg
        elif opt in ("-n", "--cores"):
            num_cores = arg
        # elif opt in ("-w", "--weather"):
        #     weather_type = arg
        # elif opt in ("-p", "--rx", "--prescription"):
        #     prescription = arg
        # elif opt in ("-d", "--dhsvm_build_path"):
        #     dhsvm_build_path = arg


    # Get Input Files and Directories for the Basin
    ###############################################

    # check for the input config file
    try:
        run_dir = os.path.dirname(dhsvm_input_config)
    except FileNotFoundError as e:
        print(
            "Path not found: %s. Please provide an absolute path to the your DHSVM input config file"
        )
        sys.exit()

    # the original basin bin files directory
    basin_orig_input_files_dir = os.path.join(run_dir, "inputs")
    try:
        basin_orig_input_files = os.listdir(basin_orig_input_files_dir)
    except FileNotFoundError as e:
        print(
            "Path not found: %s. Please provide an 'orig_inputs' directory with required original input files for your basin."
            % run_dir
        )
        sys.exit()

    # the inputs directory and files
    #   this is where we store files changed by the rest of this script and feed into dhsvm
    # basin_working_input_file_dir = os.path.join(run_dir, "inputs")
    # try:
    #     basin_working_input_files = os.listdir(basin_working_input_file_dir)
    # except FileNotFoundError as e:
    #     print(
    #         "Path not found: %s. Please provide an 'inputs' directory with required input files for your basin."
    #         % run_dir
    #     )
    #     sys.exit()

    # Is mask provided
    try:
        os.path.isfile(mask)
    except OSError:
        print(help_text)
        sys.exit()

    # get the number or rows and columns from mask for later use
    with open(mask) as desired_mask:
        line_one = desired_mask.readline().strip().split()
        if (line_one[0] == 'ncols'):
            mask_ncols = line_one[1]
        else:
            print("desired number of columns not found in mask header")

        line_two = desired_mask.readline().strip().split()
        if (line_two[0] == 'nrows'):
            mask_nrows = line_two[1]
        else:
            print("desired number of rows not found in mask header")

        # xllcorner
        line_three = desired_mask.readline().strip().split()
        if (line_three[0] == 'xllcorner'):
            mask_xllcorner = line_three[1]
        else:
            print("desired xllcorner not found in mask header")

        # yllcorner
        line_four = desired_mask.readline().strip().split()
        if (line_four[0] == 'yllcorner'):
            mask_yllcorner = line_four[1]
        else:
            print("desired yllcorner not found in mask header")

        # Create the mask polygon
        #########################

        masked_dir = os.path.join(basin_orig_input_files_dir, 'masked')
        if os.path.isdir(masked_dir):
            shutil.rmtree(masked_dir)
        os.mkdir(masked_dir)

        if ".shp" in mask:
            import fiona

            with fiona.open(mask, "r+") as shapefile:
                mask_feature = [feature["geometry"] for feature in shapefile]

            print("use of shp as mask is in active development. please stand by. in the mean time feel free to use an ascii mask with header")
            sys.exit()

        else:
            mask_feature = os.path.join(masked_dir, "basin.geojson")

            # Convert ascii to geojson feature
            os.system("rio shapes --projected %s > %s" % (mask, mask_feature))

            # mask_geojson_dump = {
            #     "type": "FeatureCollection",
            #     "features": []
            # }
            #
            # with open(mask_feature, "r+") as open_mask:
            #     for feat in open_mask:
            #         # store rio output shape in var
            #         mask_feat = json.loads(feat)
            #         # add the rio output to formatted GEOJson
            #         mask_geojson_dump["features"].append(mask_feat)
            #     open_mask.seek(0,0)
            #     open_mask.write(json.dumps(mask_geojson_dump))
            #     open_mask.close()


    # if not dhsvm_build_path:
    #     dhsvm_build_path = '/storage/DHSVM_2020/DHSVM-PNNL/build'


    # Check for DHSVM dependencies
    ##############################

    # Model state
    basin_modelstate_dir = os.path.join(basin_orig_input_files_dir,'modelstate')
    try:
        os.path.isdir(basin_modelstate_dir)
    except OSError:
        print("Path not found: %s. Please provide a modelstate dir" % basin_modelstate_dir)
        sys.exit()

    # Shadows
    basin_shadows_dir = os.path.join(run_dir,'shadows')
    try:
        os.path.isdir(basin_shadows_dir)
    except OSError:
        print("Path not found: %s. Please provide a shadows dir" % basin_shadows_dir)
        sys.exit()

    # Weather data
    met_data_dir = os.path.join(run_dir,'met_data')
    try:
        os.path.isdir(met_data_dir)
    except OSError:
        print("Path not found: %s. Please provide meteorological data directory" % met_data_dir)
        sys.exit()

    # Output dir
    run_output_dir = os.path.join(run_dir, "output")
    if os.path.isdir(run_output_dir):
        shutil.rmtree(run_output_dir)
    os.mkdir(run_output_dir)

    # Parent Basin Info
    try:
        sys.path.append(os.path.join(os.getcwd(), 'masks'))
        from parent_basin import parent_basin
    except Exception as e:
        print("Parent Basin not defined in %s. Please create this file." % os.path.join(os.getcwd(), 'masks', 'parent_basin.py'))
        sys.exit()


    # Parse input config file
    #########################

    input_config = configparser.ConfigParser()
    input_config.optionxform=str
    input_config.read(dhsvm_input_config)

    number_of_rows = input_config.get("AREA", "Number of Rows").split(" ")[0]
    number_of_columns = input_config.get("AREA", "Number of Columns").split(" ")[0]
    xllcorner = input_config.get("AREA", "Extreme West").split(" ")[0]
    yllcorner = input_config.get("AREA", "Extreme North").split(" ")[0]
    cellsize = input_config.get("AREA", "Grid spacing").split(" ")[0]
    NODATA_value = input_config.get("CONSTANTS", "Outside Basin Value").split(" ")[0]

    # Determine SW corner of grid
    yllcorner = float(yllcorner) - (int(number_of_rows) * float(cellsize))


    # Convert bin to ascii
    ######################

    # path to myconvert
    myconvert = os.path.join(dhsvm_build_path, 'DHSVM', 'program', 'myconvert')

    # myconvert conversion type for each data file
    # ASSUPTION: basin name never contains '_'

    data_types = {
        "_dem": {
            "type": "float",
            "nodata": "-9999"
        },
        "_dir": {
            "type": "character",
            "nodata": "0"
        },
        "_mask": {
            "type": "character",
            "nodata": "0"
        },
        "_soild": {
            "type": "float",
            "nodata": "-9999"
        },
        "_soiltype": {
            "type": "character",
            "nodata": "0"
        },
        "_veg": {
            "type": "character",
            "nodata": "0"
        }
    }

    # Now create the rest of the data files
    for input in os.listdir(basin_orig_input_files_dir):
        input_extension = os.path.splitext(input)[-1]
        if input_extension == '.bin':
            # splittext gives us the name bc its form ('<data>.asc', '.bin')
            unmasked_asc_name = os.path.splitext(input)[0]
            unmasked_asc_path = os.path.abspath(os.path.join(basin_orig_input_files_dir, unmasked_asc_name))
            unmasked_bin_path = os.path.abspath(os.path.join(basin_orig_input_files_dir, input))
            for key in data_types:
                if key in input:
                    use_type = data_types[key]
            os.system(
                "%s %s ascii %s %s %s %s"
                % (myconvert, use_type['type'], unmasked_bin_path, unmasked_asc_path, parent_basin['nrows'], parent_basin['ncols'])
            )
            # print("myconvert to ascii \npath: %s \ntype: %s \noutput: %s" % (unmasked_bin_path, use_type, unmasked_asc_path))
            # print("myconvert to ascii INPUT %s" % input)
            print("%s %s ascii %s %s %s %s"
            % (myconvert, use_type['type'], unmasked_bin_path, unmasked_asc_path, parent_basin['nrows'], parent_basin['ncols']))

            # Save full basin mask path for later
            # if "mask" in unmasked_asc_name:
            #     entire_basin_mask = unmasked_asc_path

    # check that entire basin mask file is created
    # try:
    #     os.path.isfile(entire_basin_mask)
    # except OSError:
    #     print("Missing mask of entire basin: %s \nCheck that a mask file is in the orig_inputs directory" % entire_basin_mask)
    #     sys.exit()


    # Add Header to ascii files
    ###########################
    # unmasked_ascii_header = "ncols        %s\nnrows        %s\nxllcorner    %s\nyllcorner    %s\ncellsize     %s\nNODATA_value  %s\n" % (
    #     parent_basin['ncols'], parent_basin['nrows'], xllcorner, yllcorner, cellsize, NODATA_value
    # )
    # ascii_header = "ncols        %s\nnrows        %s\nxllcorner    %s\nyllcorner    %s\ncellsize     %s\nNODATA_value  %s\n" % (number_of_columns, number_of_rows, xllcorner, yllcorner, cellsize, NODATA_value)

    for input_file in os.listdir(basin_orig_input_files_dir):
        for key in data_types:
            if key in input_file:
                use_type = data_types[key]
        unmasked_ascii_header = "ncols        %s\nnrows        %s\nxllcorner    %s\nyllcorner    %s\ncellsize     %s\nNODATA_value  %s\n" % (
            parent_basin['ncols'], parent_basin['nrows'], parent_basin['xllcorner'], parent_basin['yllcorner'], cellsize, use_type['nodata']
        )
        input_extension = os.path.splitext(input_file)[-1]
        if input_extension == '.asc':
            input_path = os.path.abspath(os.path.join(basin_orig_input_files_dir, input_file))
            ascii_file = open(input_path, "r+")
            content = ascii_file.read()
            ascii_file.seek(0,0)
            ascii_file.write(unmasked_ascii_header + content)
            ascii_file.close()

    # Overwrite full basin mask with desired mask
    #############################################

    # os.system('cp %s %s' % (mask, entire_basin_mask))


    # Add crs to ascii files
    ########################

    # The CRS
    # dst_crs = 'PROJCS["NAD_1983_USFS_R6_Albers",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",600000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-120.0],PARAMETER["Standard_Parallel_1",43.0],PARAMETER["Standard_Parallel_2",48.0],PARAMETER["Latitude_Of_Origin",34.0],UNIT["Meter",1.0]]'
    #
    # for input in basin_working_input_files:
    #     input_extension = os.path.splitext(input)[-1]
    #     if input_extension == '.asc':
    #         input_path = os.path.abspath(os.path.join(basin_working_input_file_dir, input))
    #
    #         # Add CRS
    #         #########
    #         os.system("rio edit-info --crs %s %s" % (dst_crs, input_path))




    # Warp the ascii to the bounds of the mask
    # overwrite the data file with warp
    ##########################################
    #
    # for input in basin_working_input_files:
    #     input_extension = os.path.splitext(input)[-1]
    #     if input_extension == '.asc':
    #         input_path = os.path.abspath(os.path.join(basin_working_input_file_dir, input))
    #         warp_path = os.path.abspath(os.path.join(basin_working_input_file_dir, input + "_warp.asc"))
    #
    #         os.system("rio warp %s %s --like %s" % (input_path, warp_path, mask))


    # Mask
    ######
    for unmasked_file in os.listdir(basin_orig_input_files_dir):
        input_extension = os.path.splitext(unmasked_file)[-1]
        print("masking INPUT file %s" % unmasked_file)
        if input_extension == ".asc":
            mask_json = os.path.join(masked_dir, "basin.geojson")
            input_unmasked_ascii = os.path.abspath(os.path.join(basin_orig_input_files_dir, unmasked_file))
            input_unmasked_tiff = "%s.tif" % input_unmasked_ascii
            output_masked_ascii = os.path.abspath(os.path.join(masked_dir, unmasked_file))
            output_masked_tiff = "%s.tiff" % output_masked_ascii
            os.system("gdal_translate -of \"GTiff\" %s %s" % (input_unmasked_ascii,input_unmasked_tiff))
            os.system("rio mask %s %s --crop --overwrite --geojson-mask %s" % (input_unmasked_tiff, output_masked_tiff, mask_json))
            os.system("gdal_translate -of \"AAIGrid\" %s %s" % (output_masked_tiff,output_masked_ascii))
            os.system("sed -i 's/^[ \t]*//g' %s" % output_masked_ascii )
            os.system("sed -i 's/ /\t/g' %s" % output_masked_ascii )
            os.system("sed -i 's/$/\t/g' %s" % output_masked_ascii )
            # Delete tiffs and unmasked ascii
            # print("REMOVING file %s" % input_unmasked_ascii)
            # os.remove(input_unmasked_ascii)
            print("REMOVING file %s" % input_unmasked_tiff)
            os.remove(input_unmasked_tiff)
            print("REMOVING file %s" % output_masked_tiff)
            os.remove(output_masked_tiff)
        elif input_extension == '.dat' and 'stream' in unmasked_file and 'map' in unmasked_file:
            mask_dict = {
                'ncols': int(mask_ncols),
                'nrows': int(mask_nrows),
                'xllcorner': float(mask_xllcorner),
                'yllcorner': float(mask_yllcorner),
                'extreme_north': float(mask_yllcorner)+(90*int(mask_nrows)),
                'cellsize': 90,
                'NODATA_value': 0,
                'mask_file': mask
            }
            out_map_file = os.path.join(masked_dir, unmasked_file)
            in_map_file = os.path.join(basin_orig_input_files_dir,unmasked_file)
            clip_stream_map(mask_dict, in_map_file, out_map_file)

    # Remove header from ascii
    ##########################
    try:
        basin_masked_files = os.listdir(masked_dir)
    except OSError:
        print(
            "Path not found: %s. Please make a masked directory in your basins inputs directory."
            % masked_dir
        )
        sys.exit()

    # Off with their heads!
    for masked_file in os.listdir(masked_dir):
        input_extension = os.path.splitext(masked_file)[-1]
        if input_extension == '.asc':
            input_path = os.path.abspath(os.path.join(masked_dir, masked_file))
            ascii_file = open(input_path, "r+")
            content_lines = ascii_file.readlines()
            ascii_file.seek(0,0)
            count = 0
            for l in content_lines:
                if count > 5:
                    ascii_file.write(l)
                count += 1
            ascii_file.close()

    # Convert ascii to bin
    ######################

    for masked_file in basin_masked_files:
        input_extension = os.path.splitext(masked_file)[-1]
        if input_extension == '.asc':
            masked_bin_name = os.path.splitext(masked_file)[0] + ".asc.bin"
            masked_bin_path = os.path.abspath(os.path.join(masked_dir, masked_bin_name))
            masked_ascii_path = os.path.abspath(os.path.join(masked_dir, masked_file))
            for key in data_types:
                if key in masked_file:
                    use_type = data_types[key]
            os.system(
                "%s ascii %s %s %s %s %s"
                % (myconvert, use_type['type'], masked_ascii_path, masked_bin_path, mask_nrows, mask_ncols)
            )

    # Update INPUT config file
    ##########################

    input_config.set("AREA", "Number of Rows", mask_nrows)
    input_config.set("AREA", "Number of Columns", mask_ncols)
    input_config.set("AREA", "Extreme West", mask_xllcorner)
    input_config.set("AREA", "Extreme North", mask_yllcorner)

    # Run DHSVM
    ###########

    import ipdb; ipdb.set_trace()

    dhsvm_path = os.path.join(dhsvm_build_path, 'DHSVM','sourcecode','DHSVM')
    os.system("mpiexec -n %s %s %s" % (num_cores, dhsvm_path, dhsvm_input_config))


if __name__ == "__main__":
    main(sys.argv[1:])
