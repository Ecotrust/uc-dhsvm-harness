import os

from collections import OrderedDict

RUN_CORES = 2

BASINS = ['entiat', 'methow', 'okan', 'wena']
DEFAULT_BASIN_NAME = 'entiat'
BASINS_DIR='/usr/local/apps/marineplanner-core/apps/uc-dhsvm-harness/basins'
RUNS_DIR='/tmp/runs'
DHSVM_BUILD='SET IN LOCAL SETTINGS'
SUPERBASINS = {
    'enti': {
        'name': 'Entiat',
        'inputs': os.path.join(BASINS_DIR, 'entiat')
    },
    'metw': {
        'name': 'Methow',
        'inputs': os.path.join(BASINS_DIR, 'methow')
    },
    'okan': {
        'name': 'Okanogan',
        'inputs': os.path.join(BASINS_DIR, 'okan')
    },
    'wena': {
        'name': 'Wenatchee',
        'inputs': os.path.join(BASINS_DIR, 'wena')
    }
}


ABSOLUTE_FLOW_METRIC = 'Absolute Flow Rate'
DELTA_FLOW_METRIC = 'Change in Flow Rate'

FLOW_METRICS = OrderedDict()
FLOW_METRICS[ABSOLUTE_FLOW_METRIC] = {'measure': 'abs', 'delta':False, 'period':0 }       # THIS MUST BE FIRST!!!!
FLOW_METRICS[DELTA_FLOW_METRIC] = {'measure': 'abs', 'delta':True, 'period':0 }       # THIS MUST BE SECOND!!!!
FLOW_METRICS['Seven Day Low Flow'] = {'measure': 'low', 'delta':False, 'period':7 }
FLOW_METRICS['Seven Day Mean Flow'] = {'measure': 'mean', 'delta':False, 'period':7 }
FLOW_METRICS['One Day Low Flow'] = {'measure': 'low', 'delta':False, 'period':1 }
FLOW_METRICS['One Day Mean Flow'] = {'measure': 'mean', 'delta':False, 'period':1 }
FLOW_METRICS['Change in 7 Day Low Flow Rate'] = {'measure': 'low', 'delta':True, 'period':7, 'source_metric':'Seven Day Low Flow'}
FLOW_METRICS['Change in 7 Day Mean Flow Rate'] = {'measure': 'mean', 'delta':True, 'period':7, 'source_metric':'Seven Day Mean Flow' }
FLOW_METRICS['Change in 1 Day Low Flow Rate'] = {'measure': 'low', 'delta':True, 'period':1, 'source_metric':'One Day Low Flow' }
FLOW_METRICS['Change in 1 Day Mean Flow Rate'] = {'measure': 'mean', 'delta':True, 'period':1, 'source_metric':'One Day Mean Flow' }

TIMESTEP = 6

from dhsvm_harness.local_settings import *
