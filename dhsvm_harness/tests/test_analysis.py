import json, datetime

from django.test import TestCase
from django.contrib.gis.geos import GEOSGeometry

from ucsrb.models import StreamFlowReading, PourPointBasin
# from django.conf import settings as dj_settings
from dhsvm_harness import settings as harness_settings
from dhsvm_harness.tests import testing_settings as settings
from dhsvm_harness.utils import readStreamFlowData, cleanStreamFlowData

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
            superbasin=settings.BASIN_1_ID.split('_')[0]
        )
        basin1.save()

        with open(settings.BASIN_JSON_2) as f:
            basin_2_json = json.load(f)
        basin_2_geom = GEOSGeometry(json.dumps(basin_2_json['features'][0]['geometry']))

        basin2 = PourPointBasin.objects.create(
            ppt_ID=2,
            segment_ID=settings.BASIN_2_ID,
            area=basin_2_json['features'][0]['properties']['acres'],
            superbasin=settings.BASIN_2_ID.split('_')[0]
        )
        basin2.save()

    # expected_diff should be either 1 or 7.
    def get_days_diff(dt1, dt2, expected_diff):
        # DST screws with direct comparisons of days
        if dt2-dt1 == datetime.timedelta(days=expected_diff):
            # This could be a false positive if DST strikes, but whatevs.
            return True
        else:
            # We should be safe that no timezone shifts will put us next to a year change
            if dt2.time() == dt1.time() and dt2.year == dt1.year:
                if dt2.month == dt1.month and dt2.day - dt1.day == expected_diff:
                    return True
                else:
                    # handle when timezone shift happens w/in 'expected_diff' days of month change
                    # 2001 is a good example: Apr 1st - 7 days = Mar 25th
                    appx_dt1 = dt2-datetime.timedelta(days=expected_diff)
                    if appx_dt1.month == dt1.month and appx_dt1.day == dt1.day:
                        return True
        # The difference is not expected, and cannot be chalked up to DST.
        return False


    def test_import_flow_data(self):
        print('=====================================')
        print('ImportTestCase: test_import_flow_data')
        print('=====================================')
        import random
        BASINS = [settings.BASIN_1_ID, settings.BASIN_2_ID]
        BASIN_READINGS_COUNT = 8*365+1  # daily_timesteps*days_per_year+1
        SEVEN_DAY_READINGS_COUNT = 7*(24/settings.TIMESTEP)
        # read in flow data to DB
        readStreamFlowData(settings.FLOW_FILE, segment_ids=BASINS)
        print("Added %d records to StreamFlow Data!" % StreamFlowReading.objects.all().count())
        self.assertTrue(StreamFlowReading.objects.all().count() == 10*2*BASIN_READINGS_COUNT) # metrics*basins*BASIN_READINGS_COUNT

        # Test metrics are stored correctly (values)

        for basin in BASINS:
            basin_obj = PourPointBasin.objects.get(segment_ID=basin)
            reading_index = random.randrange(SEVEN_DAY_READINGS_COUNT-1, BASIN_READINGS_COUNT-1)
            basin_readings = StreamFlowReading.objects.filter(basin=basin_obj)
            abs_readings = basin_readings.filter(metric=harness_settings.ABSOLUTE_FLOW_METRIC)
            # TODO: derive timestamp from reading_index, or use 'order_by' with your queries

            flow_readings = basin_readings.filter(metric=ABSOLUTE_FLOW_METRIC).order_by(time)[reading_index-(SEVEN_DAY_READINGS_COUNT-1):reading_index]
            first_time = flow_readings.order_by(time)[0].time
            last_time = flow_readings.order_by(time)[-1].time
            if last_time.time.hour < settings.TIMESTEP:
                hour_diff = last_time.time.hour - settings.TIMESTEP
                previous_timestamp = "%d.%d.%d-%d:%d:%d" % (last_time.time.month, last_time.time.day-1, last_time.time.year, 24+hour_diff, last_time.time.minute, last_time.time.second)
            else:
                previous_timestamp = "%d.%d.%d-%d:%d:%d" % (last_time.time.month, last_time.time.day, last_time.time.year, last_time.time-settings.TIMESTEP, last_time.time.minute, last_time.time.second)
            previous_time = datetime.datetime.strptime(previous_timestamp, "%m.%d.%Y-%H:%M:%S")

            print('Testing time-period %s to %s' % (first_time.strftime("%m.%d.%Y-%H:%M:%S"), last_time.strftime("%m.%d.%Y-%H:%M:%S")))
            flow_values = [x.value for x in flow_readings]

            self.assertTrue(get_days_diff(first_time, last_time, 7))

            seven_low = basin_readings.filter(metric='Seven Day Low Flow', time=last_time)
            seven_low_diff = seven_low - basin_readings.filter(metric='Seven Day Low Flow').order_by(time)[reading_index-1]
            self.assertEqual(seven_low_diff, )
            seven_mean = basin_readings.filter(metric='Seven Day Mean Flow').order_by(time)[reading_index]
            seven_mean_diff = seven_mean - basin_readings.filter(metric='Seven Day Mean Flow').order_by(time)[reading_index-1]

            one_low = basin_readings.filter(metric='One Day Low Flow').order_by(time)[reading_index]
            one_low_diff = one_low - basin_readings.filter(metric='One Day Low Flow').order_by(time)[reading_index-1]
            one_mean = basin_readings.filter(metric='One Day Mean Flow').order_by(time)[reading_index]
            one_mean_diff = one_mean - basin_readings.filter(metric='One Day Mean Flow').order_by(time)[reading_index-1]


        # Test basin connection (?)

        # Test all 3726 flows are greater than 3729 flows
        # self.assertTrue(False)

    ############################################################################
    # RDH: 3/8/2021 - this isn't a real test case, just a way to build test data
    #       ...We probably will want to test this function if we don't export
    #           clean flow data during the run itself
    ############################################################################
    # def test_clean_flow_data(self):
    #     print('====================================')
    #     print('ImportTestCase: test_clean_flow_data')
    #     print('====================================')
    #     flow_file = "/usr/local/apps/snow2flow2021/runs/methow/2002_baseline_NT_output/Stream.Flow"
    #     out_file = "/usr/local/apps/snow2flow/uc-dhsvm-harness/dhsvm_harness/tests/test_data/Test_Methow_Stream.Flow"
    #     BASINS = [settings.BASIN_1_ID, settings.BASIN_2_ID]
    #     cleanStreamFlowData(flow_file, out_file, BASINS)
