import json
import os

from django.test import TestCase
from django.contrib.gis.geos import GEOSGeometry
from django.contrib.auth.models import User, AnonymousUser

from ucsrb.models import TreatmentScenario, FocusArea

from dhsvm_harness import settings as harness_settings
from dhsvm_harness.tests import testing_settings as settings
from dhsvm_harness.utils import getRunDir, runHarnessConfig, getTargetBasin, setVegLayers

class ConfigRunTest(TestCase):

    def setUp(self):
        # configure a DHSVM run
        print('=====================')
        print('ImportTestCase: setUp')
        print('=====================')

        self.user =  User.objects.create_user(username='testy', email='testy@ecotrust.org', password='top_secret')
        user_id = User.objects.get(username='testy').id

        focus_area1 = FocusArea.objects.create(
            unit_id="metw_2114",
            unit_type="PourPointOverlap",
            geometry="SRID=3857;MULTIPOLYGON (((-13404632.90275132 6206835.507293843, -13404567.14376381 6206516.980143133, -13404569.9379526 6206512.010218441, -13404651.54548705 6206587.495385414, -13404713.37956141 6206520.269885256, -13404736.64698542 6206501.496226428, -13404737.90306285 6206506.394523728, -13404708.49654546 6206526.382869873, -13404763.4827089 6206773.57293795, -13404660.39831809 6206850.146972733, -13404632.90275132 6206835.507293843)), ((-13404627.52118118 6207010.663111246, -13404635.1077042 6207008.620792534, -13404664.51565169 6206988.631482083, -13404632.90275132 6206835.507293843, -13404660.39831809 6206850.146972734, -13404763.4827089 6206773.57293795, -13404819.11522982 6207023.661840364, -13404791.97998846 6207128.353333187, -13404841.41544553 6207258.558558098, -13404766.9647465 6207388.176079581, -13404703.40673706 6207407.593457979, -13404707.76377893 6207407.741359051, -13404706.2321753 6207453.126513464, -13404660.9802281 6207451.590259524, -13404569.55104149 6207448.485616824, -13404524.71130773 6207462.183370249, -13404434.26493706 6207489.812379636, -13404422.88258562 6207445.416476022, -13404411.50037194 6207401.020835403, -13404454.82757682 6207389.357866131, -13404443.44518713 6207344.962623181, -13404486.77221378 6207333.299638862, -13404475.3897966 6207288.904805, -13404518.71649418 6207277.241800513, -13404507.33397664 6207232.84729752, -13404550.660496 6207221.184277997, -13404539.27795096 6207176.790184072, -13404582.60414382 6207165.127068744, -13404571.22149589 6207120.733381322, -13404614.54743511 6207109.070248406, -13404591.78219978 6207020.283833805, -13404627.52118118 6207010.663111246)))"
        )

        treatment_scenario1 = TreatmentScenario.objects.create(
            focus_area=True,
            focus_area_input=focus_area1,
            user_id=user_id,
            name="treatment_scenario1",
            date_created="2021-03-07T16:03:28.464",
            date_modified="2021-03-07T16:03:29.082",
            description="A Test TreatmentScenario which was selected by selecting a stream segment near Mazama",
            satisfied=True,
            active=True,
            planning_units="66591,67928,67929",
            geometry_final_area=82629.79011003699,
            geometry_dissolved="SRID=3857;MULTIPOLYGON (((-13404632.90275132 6206835.507293843, -13404567.14376381 6206516.980143133, -13404569.9379526 6206512.010218441, -13404651.54548705 6206587.495385414, -13404713.37956141 6206520.269885256, -13404736.64698542 6206501.496226428, -13404737.90306285 6206506.394523728, -13404708.49654546 6206526.382869873, -13404763.4827089 6206773.57293795, -13404660.39831809 6206850.146972733, -13404632.90275132 6206835.507293843)), ((-13404627.52118118 6207010.663111246, -13404635.1077042 6207008.620792534, -13404664.51565169 6206988.631482083, -13404632.90275132 6206835.507293843, -13404660.39831809 6206850.146972734, -13404763.4827089 6206773.57293795, -13404819.11522982 6207023.661840364, -13404791.97998846 6207128.353333187, -13404841.41544553 6207258.558558098, -13404766.9647465 6207388.176079581, -13404703.40673706 6207407.593457979, -13404707.76377893 6207407.741359051, -13404706.2321753 6207453.126513464, -13404660.9802281 6207451.590259524, -13404569.55104149 6207448.485616824, -13404524.71130773 6207462.183370249, -13404434.26493706 6207489.812379636, -13404422.88258562 6207445.416476022, -13404411.50037194 6207401.020835403, -13404454.82757682 6207389.357866131, -13404443.44518713 6207344.962623181, -13404486.77221378 6207333.299638862, -13404475.3897966 6207288.904805, -13404518.71649418 6207277.241800513, -13404507.33397664 6207232.84729752, -13404550.660496 6207221.184277997, -13404539.27795096 6207176.790184072, -13404582.60414382 6207165.127068744, -13404571.22149589 6207120.733381322, -13404614.54743511 6207109.070248406, -13404591.78219978 6207020.283833805, -13404627.52118118 6207010.663111246)))",
            private_own=False,
            pub_priv_own=False,
            pub_priv_own_input="Bureau of Land Management",
            lsr_percent=False,
            has_critical_habitat=True,
            percent_roadless=False,
            road_distance=False,
            road_distance_max=500,
            percent_wetland=True,
            percent_riparian=False,
            slope=False,
            slope_max=30,
            percent_fractional_coverage=False,
            percent_fractional_coverage_min=0,
            percent_fractional_coverage_max=100,
            percent_high_fire_risk_area=False,
            landform_type=False,
            landform_type_checkboxes_include_north=False,
            landform_type_checkboxes_include_south=False,
            landform_type_checkboxes_include_ridgetop=False,
            landform_type_checkboxes_include_floor=False,
            landform_type_checkboxes_include_east_west=False,
            has_burned=False,
            has_wilderness_area=True,
            prescription_treatment_selection="flow",
        )

        treatment_scenario1.save()

        print("set up complete")

    def test_run_config(self):
        print('=====================================')
        print('ConfigRunTest: test_run_config')
        print('=====================================')

        # TreatmentScenario related items
        treatment_scenario1 = TreatmentScenario.objects.get(name="treatment_scenario1")

        self.assertTrue(treatment_scenario1.active)

        # settings related items
        timestep = harness_settings.TIMESTEP

        # is run directory ready
        # ts_run_dir = getRunDir(treatment_scenario1)
        # self.assertTrue(os.path.isdir(ts_run_dir))

        # Can't write test bc dont know how to add FocusArea object to TreatmentScenario test case focus_area_input
        # ts_target_basin = getTargetBasin(treatment_scenario1)
        # self.assertTrue(ts_target_basin)

        runHarnessConfig(treatment_scenario1)
