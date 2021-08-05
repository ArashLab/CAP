from inspect import Parameter
from munch import Munch, munchify
import jsonschema
import os

import hail as hl
from .logutil import *
from .common import *
from .helper import *
from .decorators import *
from .pyobj import PyObj
from .shared import Shared
from .datafile import DataFile
from . import operation as Operation

supportedFunctions = {
    'addIndex': [hl.Table, hl.MatrixTable], # rc
    'aggregate': [hl.Table, hl.MatrixTable], # rce
    'annotate': [hl.Table, hl.MatrixTable], # rcge
    'antiJoin': [hl.Table, hl.MatrixTable], # rc Function
    'semiJoin': [hl.Table, hl.MatrixTable], # rc Function
    'union': [hl.Table, hl.MatrixTable], # rc Function
    'collect': [hl.Table, hl.MatrixTable], # - ??
    'collectBykey': [hl.Table, hl.MatrixTable], # c ??
    'count': [hl.Table, hl.MatrixTable], # rcx (x:rc together)
    'distinct': [hl.Table, hl.MatrixTable], # rc
    'drop': [hl.Table, hl.MatrixTable],
    'explode': [hl.Table, hl.MatrixTable], # rc ??
    'filter': [hl.Table, hl.MatrixTable], # rce
    'groupBy': [hl.Table, hl.MatrixTable], #rc
    'index': [hl.Table, hl.MatrixTable], # rcge
    'keyBy': [hl.Table, hl.MatrixTable], # rc
    'rename': [hl.Table, hl.MatrixTable],
    'sample': [hl.Table, hl.MatrixTable], #rc
    'select': [hl.Table, hl.MatrixTable], # rcge

    'keyBy': [hl.Table, hl.MatrixTable], # rc

    'repartition': [hl.Table, hl.MatrixTable],
    'persist': [hl.Table, hl.MatrixTable],
    'unpersist': [hl.Table, hl.MatrixTable],

    'addId': [hl.MatrixTable], # rc same as nnotate col and row this is a alisa
    # Genomic ones
    'maf': [hl.MatrixTable],
    'ldPrune': [hl.MatrixTable],
    'splitMulti': [hl.MatrixTable],
    'forVep': [hl.MatrixTable]
}

@D_General
def MicroFunction(data, microFunctions):

    for func in microFunctions:

        mfType = func.get('type')
        if not mfType:
            LogException("XXX")

        supportedDataTypes = supportedFunctions.get(mfType)
        if not supportedDataTypes:
            LogException("XXX")
        if type(data) not in supportedDataTypes:
            print(type(data), supportedDataTypes)
            LogException("XXX")

        param = func.get('parameters', Munch())

        # if mfType == 'addIndex':
        #     mht = data
        #     mht = mht.add

    return data

    # for op in operations:
    #     params = operations[op]
    #     try:
    #         if op=='rename':
    #             mt = mt.rename(params)
    #         elif op=='drop':
    #             mt = mt.drop(*params)
    #         elif op=='gtOnly' and params==True:
    #             mt = mt.select_entries('GT')
    #         elif op=='annotateRows': ### TBF so that the type is mentiond and enough data to form expression
    #             for k in params:
    #                 if isinstance(params[k], dict):
    #                     params[k] = hl.struct(**params[k])
    #                 elif isinstance(params[k], list):
    #                     if len(params[k]) == len(set(params[k])):
    #                         params[k] = hl.set(params[k])
    #                     else:
    #                         params[k] = hl.array(params[k])
    #             mt = mt.annotate_rows(**params)
    #         elif op=='annotateCols':
    #             mt = mt.annotate_cols(**params)
    #         elif op=='annotateGlobals':
    #             mt = mt.annotate_globals(**params)
    #         elif op=='annotateEntries':
    #             mt = mt.annotate_entries(**params)
    #         elif op=='maf':
    #             # Calculate MAF in a coloum (avoid writing on existing cols by using a random col name)
    #             mafColName = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    #             mafExpr = {mafColName : hl.min(hl.agg.call_stats(mt.GT, mt.alleles).AF)}
    #             mt = mt.annotate_rows(**mafExpr)
    #             # Apply filter
    #             mt = mt.filter_rows((mt[mafColName] >= params.min) & (mt[mafColName] <= params.max), keep=True)
    #         elif op=='ldPrune':
    #             prunList = hl.ld_prune(mt.GT, **params)
    #             mt = mt.filter_rows(hl.is_defined(prunList[mt.row_key]))
    #         elif op=='subSample':
    #             mt = SampleRows(mt, params)
    #         elif op=='splitMulti':
    #             mt = SplitMulti(mt, params)
    #         elif op=='addId':
    #             mt = AddId(mt, params)
    #         elif op=='forVep' and params==True:
    #             mt = ForVep(mt)
    #         else:
    #             LogException(f'Something Wrong in the code')
    #     except:
    #         LogException(f'Hail cannot perfom {op} with args: {params}.')
    #     Log(f'{op} done with agrs: {params}.')
