import json, datetime, statistics

from django.test import TestCase
from django.contrib.auth.models import User
from django.contrib.gis.geos import GEOSGeometry

from ucsrb.models import StreamFlowReading, FocusArea, TreatmentScenario, ScenarioState
from django.core.management import call_command

from dhsvm_harness import settings as harness_settings
from dhsvm_harness.tests import testing_settings as settings
from dhsvm_harness.utils import readStreamFlowData, cleanStreamFlowData, runHarnessConfig

class ImportTestCase(TestCase):
    def setUp(self):
        # create some basin records for 3726 and 3739
        print('=====================')
        print('ImportTestCase: setUp')
        print('=====================')

        user = User.objects.create(username='user')
        user.save()

        call_command('import_focus_areas', settings.ENTI_OVERLAP_BASINS_FILE, 'PourPointOverlap')
        # call_command('import_focus_areas', settings.ENTI_DISCRETE_BASINS_FILE, 'PourPointDiscrete')

        with open(settings.BASIN_JSON_1) as f:
            basin_1_json = json.load(f)
            basin_1_geom = GEOSGeometry(json.dumps(basin_1_json['features'][0]['geometry']))
            basin_1_geom.srid = 3857

            basin1 = FocusArea.objects.create(
            unit_type='PourPointOverlap',
            unit_id=settings.BASIN_1_ID,
            geometry=basin_1_geom
            # area=basin_1_json['features'][0]['properties']['acres'],
            # superbasin=settings.BASIN_1_ID.split('_')[0]
            )
            basin1.save()

            with open(settings.BASIN_JSON_2) as f:
                basin_2_json = json.load(f)
                basin_2_geom = GEOSGeometry(json.dumps(basin_2_json['features'][0]['geometry']))
                basin_2_geom.srid = 3857

                basin2 = FocusArea.objects.create(
                unit_type='PourPointOverlap',
                unit_id=settings.BASIN_2_ID,
                geometry=basin_2_geom
                # area=basin_2_json['features'][0]['properties']['acres'],
                # superbasin=settings.BASIN_2_ID.split('_')[0]
                )
                basin2.save()

        with open(settings.RESET_BASIN_JSON) as f:
            reset_basin_json = json.load(f)
        reset_basin_geom = GEOSGeometry(json.dumps(reset_basin_json['features'][0]['geometry']))
        reset_basin_geom.srid = 3857

        reset_basin = FocusArea.objects.create(
            unit_type='Drawing',
            unit_id=settings.RESET_BASIN_ID,
            geometry=reset_basin_geom
        )
        reset_basin.save()

        # reset_scenario = ScenarioState.objects.create(
        #     name='reset scenario state',
        #     user=user,
        #     scenario_type="Draw"
        # )
        # reset_scenario.save()

        reset_treatment = TreatmentScenario.objects.create(
            user=user,
            name='Reset Entiat Treatment',
            description="NOTR",
            focus_area=True,
            focus_area_input=reset_basin,
            # scenario=reset_scenario,
            prescription_treatment_selection='notr'
        )

        reset_treatment.save()

    def test_update_baseline_data(self):
        # Get reset_basin
        reset_basin = FocusArea.objects.get(unit_type='Drawing', unit_id=settings.RESET_BASIN_ID)
        # Get reset_treatment using reset_basin
        reset_treatment = TreatmentScenario.objects.get(focus_area_input=reset_basin)
        # call runHarnessConfig
        runHarnessConfig(reset_treatment)
        # Check for baseline values related to each ID in settings.RESET_TEST_BASIN_IDS
        # for basin_id in settings.RESET_TEST_BASIN_IDS:
        #     basin_readings = StreamFlowReading.objects.filter(segment_id=basin_id)
        #     self.assertTrue(StreamFlowReading.objects.filter(segment_id=basin_id, is_baseline=True) >= 365*(24/harness_settings.TIMESTEP)) # basins*BASIN_READINGS_COUNT
        # Check for readings for every 'enti_' overlapping pourpoint
        for basin in FocusArea.objects.filter(unit_type="PourPointOverlap", unit_id__contains="enti_"):
            # basin_readings = StreamFlowReading.objects.filter(segment_id=basin.unit_id)
            self.assertTrue(StreamFlowReading.objects.filter(segment_id=basin.unit_id, is_baseline=True).count() >= 365*(24/harness_settings.TIMESTEP)) # basins*BASIN_READINGS_COUNT

    def test_import_flow_data(self):
        print('=====================================')
        print('ImportTestCase: test_import_flow_data')
        print('=====================================')
        import random

        # expected_diff should be either 1 or 7.
        def get_days_diff(dt1, dt2, expected_diff):
            # DST screws with direct comparisons of days
            if dt2 - dt1 == datetime.timedelta(days=expected_diff):
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


        BASINS = [settings.BASIN_1_ID, settings.BASIN_2_ID]
        # ONE_DAY_READINGS_COUNT = int(24/harness_settings.TIMESTEP)
        ONE_DAY_READINGS_COUNT = 8 # the test data is on a 3 hr timestep
        BASIN_READINGS_COUNT = ONE_DAY_READINGS_COUNT*365+1  # daily_timesteps*days_per_year+1
        SEVEN_DAY_READINGS_COUNT = 7*ONE_DAY_READINGS_COUNT
        # read in flow data to DB
        readStreamFlowData(settings.FLOW_FILE, segment_ids=BASINS)
        print("Added %d records to StreamFlow Data!" % StreamFlowReading.objects.all().count())
        self.assertTrue(StreamFlowReading.objects.all().count() == 2*BASIN_READINGS_COUNT) # basins*BASIN_READINGS_COUNT

        # Test metrics are stored correctly (values)
        random_index = random.randrange(SEVEN_DAY_READINGS_COUNT-1, BASIN_READINGS_COUNT-1)
        STT_index = 217 # Fall back 1 hr -- enter Standard Time (this may not always get the right window outside of 2001)
        DST_index = 1505 # Sping forward 1 hr -- enter Daylight Savings Time (this may not always get the right window outside of 2002)
        for (reading_index, time_type) in [(DST_index, 'Daylight Savings'), (STT_index, 'Standard'), (random_index, 'Random')]:
            for basin in BASINS:
                print("--------------------------------------------------")
                print("Testing Basin ID %s during %s Time" % (basin, time_type))
                print("--------------------------------------------------")
                # basin_obj = FocusArea.objects.get(unit_id=basin, unit_type='PourPointOverlap')
                basin_readings = StreamFlowReading.objects.filter(segment_id=basin)
                abs_readings = basin_readings.filter(metric=harness_settings.ABSOLUTE_FLOW_METRIC)

                flow_readings = basin_readings.filter(metric=harness_settings.ABSOLUTE_FLOW_METRIC).order_by('time')[reading_index-(SEVEN_DAY_READINGS_COUNT):reading_index+1]
                first_time = flow_readings[0].time
                last_time = flow_readings[flow_readings.count()-1].time
                if last_time.hour < harness_settings.TIMESTEP:
                    # Should only be true if last_time.hour == 0
                    hour_diff = last_time.hour - harness_settings.TIMESTEP
                    previous_timestamp = "%d.%d.%d-%d:%d:%d" % (last_time.month, last_time.day-1, last_time.year, 24+hour_diff, last_time.minute, last_time.second)
                else:
                    previous_timestamp = "%d.%d.%d-%d:%d:%d" % (last_time.month, last_time.day, last_time.year, last_time.hour-harness_settings.TIMESTEP, last_time.minute, last_time.second)
                previous_time = datetime.datetime.strptime(previous_timestamp, "%m.%d.%Y-%H:%M:%S")
                if last_time.day == 1:
                    previous_day_time = last_time - datetime.timedelta(days=1)
                    if not last_time.hour == previous_day_time.hour:
                        if last_time.hour == 0: # this should be true 100% of cases that get here
                            if previous_day_time.hour == 23:
                                    previous_day_time = previous_day_time + datetime.timedelta(hours=1)
                            elif previous_day_time.hour == 1:
                                previous_day_time = previous_day_time - datetime.timedelta(hours=1)
                            else:
                                # this should never happen
                                hour_diff = last_time.hour - previous_day_time.hour
                                previous_day_time = previous_day_time + datetime.timedelta(hours=hour_diff)
                        else:
                            # this should never happen
                            hour_diff = last_time.hour - previous_day_time.hour
                            previous_day_time = previous_day_time + datetime.timedelta(hours=hour_diff)
                else:
                    previous_day_timestamp = "%d.%d.%d-%d:%d:%d" % (last_time.month, last_time.day-1, last_time.year, last_time.hour, last_time.minute, last_time.second)
                    previous_day_time = datetime.datetime.strptime(previous_day_timestamp, "%m.%d.%Y-%H:%M:%S")

                print('Testing time-period %s to %s' % (first_time.strftime("%m.%d.%Y-%H:%M:%S"), last_time.strftime("%m.%d.%Y-%H:%M:%S")))

                self.assertTrue(get_days_diff(previous_day_time, last_time, 1))
                self.assertTrue(get_days_diff(first_time, last_time, 7))

                flow_values = [x.value for x in flow_readings]
                prior_reading = flow_values.pop(0)

                # # Test Change In Flow Rate reading
                # self.assertTrue(basin_readings.get(metric=harness_settings.DELTA_FLOW_METRIC,time=last_time).value, flow_values[-1]-flow_values[-2])
                #
                # # seven_low = basin_readings.get(metric='Seven Day Low Flow', time=last_time)
                # seven_low = sorted(flow_values)[0]
                # # Test Seven Day Low Flow reading
                # self.assertTrue(basin_readings.get(metric='Seven Day Low Flow',time=last_time).value, seven_low)
                #
                # # Test Change in Seven Day Low Flow Rate reading
                # seven_low_diff = seven_low - basin_readings.get(metric='Seven Day Low Flow',time=previous_time).value
                # self.assertEqual(seven_low_diff, basin_readings.get(metric='Change in 7 Day Low Flow Rate', time=last_time).value)
                #
                # # Test Seven Day Mean Flow reading
                # seven_mean = statistics.mean(flow_values)
                # self.assertEqual(seven_mean, basin_readings.get(metric='Seven Day Mean Flow',time=last_time).value)
                #
                # # Test Change in Seven Day Mean Flow Rate reading
                # seven_mean_diff = seven_mean - basin_readings.get(metric='Seven Day Mean Flow',time=previous_time).value
                # self.assertEqual(seven_mean_diff, basin_readings.get(metric='Change in 7 Day Mean Flow Rate',time=last_time).value)
                #
                # # Test One Day Low Flow reading
                # one_day_start_index = 0-ONE_DAY_READINGS_COUNT
                # one_day_flow_values = flow_values[one_day_start_index:]
                # one_low = sorted(one_day_flow_values)[0]
                # self.assertEqual(one_low, basin_readings.get(metric='One Day Low Flow',time=last_time).value)
                #
                # # Test Change in One Day Low Flow Rate reading
                # one_low_diff = one_low - basin_readings.get(metric='One Day Low Flow',time=previous_time).value
                # self.assertEqual(one_low_diff, basin_readings.get(metric='Change in 1 Day Low Flow Rate', time=last_time).value)
                #
                # # Test One Day Mean Flow reading
                # one_mean = statistics.mean(one_day_flow_values)
                # self.assertEqual(one_mean, basin_readings.get(metric='One Day Mean Flow',time=last_time).value)
                #
                # # Test Change in One Day Mean Flow Rate reading
                # one_mean_diff = one_mean - basin_readings.get(metric='One Day Mean Flow',time=previous_time).value
                # self.assertEqual(one_mean_diff, basin_readings.get(metric='Change in 1 Day Mean Flow Rate',time=last_time).value)

        # Test all 3726 flows are greater than 3729 flows

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
