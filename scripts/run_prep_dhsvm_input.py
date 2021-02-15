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


def main(argv):
    mask = False
    dhsvm_input_config = False
    dhsvm_build_path = False
    root_path = os.path.dirname(os.path.abspath(__file__))
    path = root_path
    help_text = "usage: run_prep_dhsvm_input.py -m <path to the mask shp> -i <absolute path to basin's DHSVM input config file> -d <absolute path to DHSVM build directory>"

    try:
        opts, args = getopt.getopt(argv, "hm:i:d:", ["mask=", "dhsvm_input_config=", "dhsvm_build_path="])
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
        elif opt in ("-d", "--dhsvm_build_path"):
            dhsvm_build_path = arg


    # Get Input Files and Directories for the Basin
    ###############################################

    # check for the input config file
    try:
        basin_path = os.path.dirname(dhsvm_input_config)
    except FileNotFoundError as e:
        print(
            "Path not found: %s. Please provide an absolute path to the your DHSVM input config file"
        )
        sys.exit()

    # the original basin bin files directory
    basin_orig_input_files_dir = os.path.join(basin_path, "orig_inputs")
    try:
        basin_orig_input_files = os.listdir(basin_orig_input_files_dir)
    except FileNotFoundError as e:
        print(
            "Path not found: %s. Please provide an 'orig_inputs' directory with required original input files for your basin."
            % basin_path
        )
        sys.exit()

    # the inputs directory and files
      # this is where we store files changed by the rest of this script and feed into dhsvm
    basin_working_input_file_dir = os.path.join(basin_path, "inputs")
    try:
        basin_working_input_files = os.listdir(basin_working_input_file_dir)
    except FileNotFoundError as e:
        print(
            "Path not found: %s. Please provide an 'inputs' directory with required input files for your basin."
            % basin_path
        )
        sys.exit()

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


    if not dhsvm_build_path:
        dhsvm_build_path = '/storage/DHSVM_2020/DHSVM-PNNL/build'


    # Check for DHSVM dependencies
    ##############################

    # Model state
    basin_modelstate_dir = os.path.join(basin_working_input_file_dir, "modelstate")
    try:
        os.path.isdir(basin_modelstate_dir)
    except OSError:
        print("Path not found: %s. Please provide a modelstate dir" % basin_modelstate_dir)
        sys.exit()

    # Shadows
    basin_shadows_dir = os.path.join(basin_path, "shadows")
    try:
        os.path.isdir(basin_shadows_dir)
    except OSError:
        print("Path not found: %s. Please provide a shadows dir" % basin_shadows_dir)
        sys.exit()

    # Weather data
    basin_livneh_dir = os.path.join(basin_path, "livneh_1950_2013")
    try:
        os.path.isdir(basin_livneh_dir)
    except OSError:
        print("Path not found: %s. Please provide a livneh_1950_2013 dir" % basin_livneh_dir)
        sys.exit()

    # Output dir
    basin_output_dir = os.path.join(basin_path, "output")
    if not os.path.isdir(basin_output_dir):
        os.system("mkdir %s" % basin_output_dir)


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
    myconvert = os.path.join(dhsvm_build_path, 'DHSVM/program/myconvert')

    # myconvert conversion type for each data file
    data_types = {
        "dem": "float",
        "dir": "character",
        "mask": "character",
        "soild": "character",
        "soiltype": "character",
        "veg": "character",
    }


    # Now create the rest of the data files
    for input in basin_orig_input_files:
        input_extension = os.path.splitext(input)[-1]
        if input_extension == '.bin':
            # splittext gives us the name bc its form ('<data>.asc', '.bin')
            input_asc_name = os.path.splitext(input)[0]
            input_asc_path = os.path.abspath(os.path.join(basin_working_input_file_dir, input_asc_name))
            input_bin_path = os.path.abspath(os.path.join(basin_orig_input_files_dir, input))
            for key in data_types:
                if key in input:
                    use_type = data_types[key]
            os.system(
                "%s %s ascii %s %s %s %s"
                % (myconvert, use_type, input_bin_path, input_asc_path, number_of_rows, number_of_columns)
            )
            print("myconvert to ascii \npath: %s \ntype: %s \noutput: %s" % (input_bin_path, use_type, input_asc_path))

            # Save full basin mask path for later
            if "mask" in input_asc_name:
                entire_basin_mask = input_asc_path

    # check that entire basin mask file is created
    try:
        os.path.isfile(entire_basin_mask)
    except OSError:
        print("Missing mask of entire basin: %s \nCheck that a mask file is in the orig_inputs directory" % entire_basin_mask)
        sys.exit()


    # Add Header to ascii files
    ###########################

    ascii_header = "ncols        %s\nnrows        %s\nxllcorner    %s\nyllcorner    %s\ncellsize     %s\nNODATA_value  %s\n" % (number_of_rows, number_of_columns, xllcorner, yllcorner, cellsize, NODATA_value)

    for input in basin_working_input_files:
        input_extension = os.path.splitext(input)[-1]
        if input_extension == '.asc':
            input_path = os.path.abspath(os.path.join(basin_working_input_file_dir, input))
            ascii_file = open(input_path, "r+")
            content = ascii_file.read()
            ascii_file.seek(0,0)
            ascii_file.write(ascii_header + content)
            ascii_file.close()


    # Overwrite full basin mask with desired mask
    #############################################

    os.system('cp %s %s' % (mask, entire_basin_mask))


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



    # Create the mask polygon
    #########################

    if ".shp" in mask:
        import fiona

        with fiona.open(mask, "r+") as shapefile:
            mask_feature = [feature["geometry"] for feature in shapefile]

        print("use of shp as mask is in active development. please stand by. in the mean time feel free to use an ascii mask with header")
        sys.exit()

    else:
        mask_feature = os.path.join(basin_working_input_file_dir, "basin.geojson")

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

    for input in basin_working_input_files:
        input_extension = os.path.splitext(input)[-1]
        if input_extension == ".asc":
            mask_feature = os.path.join(basin_working_input_file_dir, "basin.geojson")
            input_name = os.path.splitext(input)[0]
            input_path = os.path.abspath(os.path.join(basin_working_input_file_dir, input))
            masked_dir = os.path.join(basin_working_input_file_dir, "masked")
            if not os.path.isdir(masked_dir):
                os.system("mkdir %s" % masked_dir)
            output_masked = os.path.abspath(os.path.join(masked_dir, input))
            os.system("rio mask %s %s --crop --overwrite --geojson-mask %s" % (input_path, output_masked, mask_feature))

    # overwrite the original entire basin mask with the desired mask


    # Remove header from ascii
    ##########################
    basin_masked_input_dir = os.path.abspath(os.path.join(basin_working_input_file_dir, "masked"))
    try:
        basin_masked_files = os.listdir(basin_masked_input_dir)
    except OSError:
        print(
            "Path not found: %s. Please make a masked directory in your basins inputs directory."
            % basin_masked_input_dir
        )
        sys.exit()

    for input in basin_working_input_files:
        input_extension = os.path.splitext(input)[-1]
        if input_extension == '.asc':
            input_path = os.path.abspath(os.path.join(basin_masked_input_dir, input))
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

    for input in basin_masked_files:
        input_extension = os.path.splitext(input)[-1]
        if input_extension == '.asc':
            input_bin_name = os.path.splitext(input)[0] + ".asc.bin"
            input_bin_path = os.path.abspath(os.path.join(basin_working_input_file_dir, input_bin_name))
            input_masked_path = os.path.abspath(os.path.join(basin_masked_input_dir, input))
            for key in data_types:
                if key in input:
                    use_type = data_types[key]
            os.system(
                "%s ascii %s %s %s %s %s"
                % (myconvert, use_type, input_masked_path, input_bin_path, mask_nrows, mask_ncols)
            )


    # Update INPUT config file
    ##########################

    input_config.set("AREA", "Number of Rows", mask_nrows)
    input_config.set("AREA", "Number of Columns", mask_ncols)
    input_config.set("AREA", "Extreme West", mask_xllcorner)
    input_config.set("AREA", "Extreme North", mask_yllcorner)

    import ipdb; ipdb.set_trace()

    # Run DHSVM
    ###########

    dhsvm_path = os.path.join(dhsvm_build_path, 'DHSVM/sourcecode/DHSVM')
    os.system("%s %s" % (dhsvm_path, dhsvm_input_config))


if __name__ == "__main__":
    main(sys.argv[1:])
