import os, pathlib

BASIN_JSON_1 = os.path.join(pathlib.Path(__file__).parent.absolute(), 'test_data', 'basin_3726.geojson')
BASIN_1_ID = "metw_3726"
BASIN_JSON_2 = os.path.join(pathlib.Path(__file__).parent.absolute(), 'test_data', 'basin_3739.geojson')
BASIN_2_ID = "metw_3739"

FLOW_FILE = os.path.join(pathlib.Path(__file__).parent.absolute(), 'test_data', 'Test_Methow_Stream.Flow')

ANONYMOUS_USER_PK = 2
INPUT_TEMPLATE = os.path.join(pathlib.Path(__file__).parent.absolute(), 'test_data', 'INPUT.test.template')

RESET_BASIN_JSON = os.path.join(pathlib.Path(__file__).parent.absolute(), 'test_data', 'enti_1065.geojson')
RESET_BASIN_ID = "enti_1065"

RESET_TEST_BASIN_IDS = [
    'enti_11',      # NW tip
    'enti_270',     # Top of weird Western 'mouth'
    'enti_396',     # Near the center of the basin
    'enti_436',     # Bottom of weird Western 'mouth'
    'enti_464',     # Mid-way down Eastern border
    'enti_1065',    # Mouth
    'enti_1119',    # Southernmost
]

ENTI_OVERLAP_BASINS_FILE = os.path.join(pathlib.Path(__file__).parent.absolute(), 'test_data', 'enti_osheds.zip')
ENTI_DISCRETE_BASINS_FILE = os.path.join(pathlib.Path(__file__).parent.absolute(), 'test_data', 'enti_dsheds.zip')
