import json

from django.test import TestCase
from django.contrib.gis.geos import GEOSGeometry

from ucsrb.models import StreamFlowReading, PourPointBasin
# from django.conf import settings as dj_settings
from dhsvm_harness import settings as harness_settings
from dhsvm_harness.tests import testing_settings as settings
from dhsvm_harness.utils import ReadStreamFlowData

class ImportTestCase(TestCase):
    def setUp(self):
        # create some basin records for 3726 and 3739
        print('=====================')
        print('ImportTestCase: setUp')
        print('=====================')
        with open(settings.BASIN_JSON_1) as f:
            basin_1_json = json.load(f)
        basin_1_geom = GEOSGeometry(json.dumps(basin_1_json['features'][0]['geometry']))

        basin1 = PourPointBasin.objects.create(
            ppt_ID=1,
            segment_ID=settings.BASIN_1_ID,
            area=basin_1_json['features'][0]['properties']['acres'],
            mean_elev=1,
            avg_slp=0,
            slp_gt60=0,
            elev_dif=0,
            mean_shade=0,
            veg_prop=0,
            thc_11=0,
            thc_12=0,
            thc_13=0,
            thc_14=0,
            thc_15=0,
            thc_21=0,
            thc_22=0,
            thc_23=0,
            thc_24=0,
            thc_25=0,
            fc_11=0,
            fc_12=0,
            fc_13=0,
            fc_14=0,
            fc_15=0,
            fc_21=0,
            fc_22=0,
            fc_23=0,
            fc_24=0,
            fc_25=0
        )
        basin1.save()

        with open(settings.BASIN_JSON_2) as f:
            basin_2_json = json.load(f)
        basin_2_geom = GEOSGeometry(json.dumps(basin_2_json['features'][0]['geometry']))

        basin2 = PourPointBasin.objects.create(
            ppt_ID=2,
            segment_ID=settings.BASIN_2_ID,
            area=basin_2_json['features'][0]['properties']['acres'],
            mean_elev=1,
            avg_slp=0,
            slp_gt60=0,
            elev_dif=0,
            mean_shade=0,
            veg_prop=0,
            thc_11=0,
            thc_12=0,
            thc_13=0,
            thc_14=0,
            thc_15=0,
            thc_21=0,
            thc_22=0,
            thc_23=0,
            thc_24=0,
            thc_25=0,
            fc_11=0,
            fc_12=0,
            fc_13=0,
            fc_14=0,
            fc_15=0,
            fc_21=0,
            fc_22=0,
            fc_23=0,
            fc_24=0,
            fc_25=0
        )
        basin2.save()



    def test_import_flow_data(self):
        print('=====================================')
        print('ImportTestCase: test_import_flow_data')
        print('=====================================')
        import random
        BASINS = [settings.BASIN_1_ID, settings.BASIN_2_ID]
        BASIN_READINGS_COUNT = 8*365+1  # daily_timesteps*days_per_year+1
        # read in flow data to DB
        ReadStreamFlowData(settings.FLOW_FILE, segment_ids=BASINS)
        print("Added %d records to StreamFlow Data!" % StreamFlowReading.objects.all().count())
        self.assertTrue(StreamFlowReading.objects.all().count() == 10*2*BASIN_READINGS_COUNT) # metrics*basins*BASIN_READINGS_COUNT

        # Test metrics are stored correctly (values)

        for basin in BASINS:
            basin_obj = PourPointBasin.objects.get(segment_ID=basin)
            reading_index = random.randrange(0,BASIN_READINGS_COUNT-1)
            basin_readings = StreamFlowReading.objects.filter(basin=basin_obj)
            abs_readings = basin_readings.filter(metric=harness_settings.ABSOLUTE_FLOW_METRIC)
            # TODO: derive timestamp from reading_index, or use 'order_by' with your queries



        # Test basin connection (?)

        # Test all 3726 flows are greater than 3729 flows
        # self.assertTrue(False)
