BASINS = ['entiat', 'methow', 'okan', 'wena']
DEFAULT_BASIN_NAME = 'entiat'

ABSOLUTE_FLOW_METRIC = 'Absolute Flow Rate'
DELTA_FLOW_METRIC = 'Change in Flow Rate'

FLOW_METRICS = {
    ABSOLUTE_FLOW_METRIC: {'measure': 'abs', 'delta':False, 'period':0 },       # THIS MUST BE FIRST!!!!
    DELTA_FLOW_METRIC: {'measure': 'abs', 'delta':True, 'period':0 },       # THIS MUST BE SECOND!!!!
    'Seven Day Low Flow': {'measure': 'low', 'delta':False, 'period':7 },
    'Seven Day Mean Flow': {'measure': 'mean', 'delta':False, 'period':7 },
    'One Day Low Flow': {'measure': 'low', 'delta':False, 'period':1 },
    'One Day Mean Flow': {'measure': 'mean', 'delta':False, 'period':1 },
    'Change in 7 Day Low Flow Rate': {'measure': 'low', 'delta':True, 'period':7 },
    'Change in 7 Day Mean Flow Rate': {'measure': 'mean', 'delta':True, 'period':7 },
    'Change in 1 Day Low Flow Rate': {'measure': 'low', 'delta':True, 'period':1 },
    'Change in 1 Day Mean Flow Rate': {'measure': 'mean', 'delta':True, 'period':1 }
}

TIMESTEP = 3

from local_settings import *
