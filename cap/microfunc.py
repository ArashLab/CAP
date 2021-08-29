from cap import stage
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

supportedMicroFunctions = {
    'addIndex': ['ht', 'mt'], # rc
    'aggregate': ['ht', 'mt'], # rce
    'annotate': ['ht', 'mt'], # rcge
    # 'antiJoin': ['ht', 'mt'], # rc Function
    # 'semiJoin': ['ht', 'mt'], # rc Function
    # 'union': ['ht', 'mt'], # rc Function
    'collect': ['ht', 'mt'], # - ??
    'collectByKey': ['ht', 'mt'], # c ??
    'count': ['ht', 'mt'], # rcx (x:rc together)
    'distinct': ['ht', 'mt'], # rc
    'drop': ['ht', 'mt'],
    'explode': ['ht', 'mt'], # rc ??
    'filter': ['ht', 'mt'], # rce
    'groupBy': ['ht', 'mt'], #rc
    'index': ['ht', 'mt'], # rcge
    'keyBy': ['ht', 'mt'], # rc
    'rename': ['ht', 'mt'],
    'sample': ['ht', 'mt'], #rc
    'select': ['ht', 'mt'], # rcge

    'repartition': ['ht', 'mt'],
    'persist': ['ht', 'mt'],
    'unpersist': ['ht', 'mt'],

    'addId': ['mt'], # rc same as nnotate col and row this is a alisa
    # Genomic ones
    'maf': ['mt'], # to be alias
    'ldPrune': ['mt'],
    'splitMulti': ['mt'],
    'forVep': ['mt'],
    'describe': ['ht', 'mt']
}

@D_General
def MicroFunction(data, microFunctions):

    for func in microFunctions:
        dataType = dataTypeMapper[type(data)]

        mfType = func.get('type')

        if not mfType:
            LogException("XXX")

        supportedDataTypes = supportedMicroFunctions.get(mfType)
        if not supportedDataTypes:
            LogException("XXX")
        if dataType not in supportedDataTypes:
            LogException("XXX")

        parameters = func.get('parameters', Munch())

                    
        #############################
        if mfType == 'addIndex':
            name = parameters.pop('name', None)
            axis = parameters.pop('axis', None)
            axis = f'_{axis}' if axis else ''
            name = f'name = {name}' if name else ''
            statement = f'res = data.add{axis}_index({name})'
            ldict = locals()
            exec(statement, globals(), ldict)
            data = ldict['res']

        #############################
        elif mfType == 'aggregate':
            retType = parameters.pop('retType')
            axis = parameters.pop('axis', None)
            expr = parameters.pop('expr', None)
            retType = dataTypeNameMapper[retType]
            axis = f'_{axis}' if axis else ''
            statement = f'res = data.aggregate{axis}({expr})'
            ldict = locals()
            exec(statement, globals(), ldict)
            data = ldict['res']
            data = retType(data)

        #############################
        elif mfType == 'annotate':
            axis = parameters.pop('axis', None)
            axis = f'_{axis}' if axis else ''
            strParameters = ', '.join([f'{k}={v}' for k,v in parameters.get('namedExpr', Munch()).items()])
            statement = f'res = data.annotate{axis}({strParameters})'
            ldict = locals()
            exec(statement, globals(), ldict)
            data = ldict['res']

        #############################
        elif mfType == 'union':
            pass # TBI

        #############################
        elif mfType == 'collect':
            pass # TBI

        #############################
        elif mfType == 'collectByKey':
            pass # TBI

        #############################
        elif mfType == 'count':
            LogPrint(data.count())

        #############################
        elif mfType == 'distinct':
            pass # TBI

        #############################
        elif mfType == 'drop':
            mt = mt.drop(*parameters)

        #############################
        elif mfType == 'explode':
            pass # TBI

        #############################
        elif mfType == 'filter':
            pass # TBI

        #############################
        elif mfType == 'groupBy':
            pass # TBI

        #############################
        elif mfType == 'index':
            pass # TBI

        #############################
        elif mfType == 'keyBy':
            axis = parameters.pop('axis', None)
            axis = f'_{axis}' if axis else ''
            keys = ', '.join([f'\'{k}\'' for k in parameters.get('keys', [])])
            namedKeys = ', '.join([f'{k}={v}' for k,v in parameters.get('namedKeys', {}).items()])
            statement = f'res = data.key{axis}_by({keys}, {namedKeys})'
            ldict = locals()
            exec(statement, globals(), ldict)
            data = ldict['res']

        #############################
        elif mfType == 'rename':
            data = data.rename(parameters)

        #############################
        elif mfType == 'sample':
            axis = parameters.pop('axis', None)
            axis = f'_{axis}' if axis else ''
            strParameters = ', '.join([f'{k}={v}' for k,v in parameters.items()])
            statement = f'res = data.sample{axis}({strParameters})'
            ldict = locals()
            exec(statement, globals(), ldict)
            data = ldict['res']

        #############################
        elif mfType == 'select':
            axis = parameters.pop('axis', None)
            axis = f'_{axis}' if axis else ''
            expr = ', '.join([f'\'{k}\'' for k in parameters.get('expr', [])])
            namedExpr = ', '.join([f'{k}={v}' for k,v in parameters.get('namedExpr', {}).items()])
            statement = f'res = data.select{axis}({expr}, {namedExpr})'
            ldict = locals()
            exec(statement, globals(), ldict)
            data = ldict['res']

        #############################
        elif mfType == 'repartition':
            data = data.repartition(parameters.numPartitions)

        #############################
        elif mfType == 'persist':
            if parameters.persistence not in ['DISK_ONLY', 'DISK_ONLY_2', 'MEMORY_ONLY', 'MEMORY_ONLY_2', 'MEMORY_ONLY_SER', 'MEMORY_ONLY_SER_2', 'MEMORY_AND_DISK', 'MEMORY_AND_DISK_2', 'MEMORY_AND_DISK_SER', 'MEMORY_AND_DISK_SER_2', 'OFF_HEAP']:
                LogException(f'Persistance {parameters.persistence} level not supported')
            mht = mht.persist(parameters.persistence)

        #############################
        elif mfType == 'unpersist':
            data.unpersist()

        #############################
        elif mfType == 'addId':
            data = AddId(data, parameters)

        #############################
        elif mfType == 'maf':
            mt = data
            # Calculate MAF in a coloum (avoid writing on existing cols by using a random col name)
            mafColName = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
            mafExpr = {mafColName : hl.min(hl.agg.call_stats(mt.GT, mt.alleles).AF)}
            mt = mt.annotate_rows(**mafExpr)
            # Apply filter
            mt = mt.filter_rows((mt[mafColName] >= parameters.min) & (mt[mafColName] <= parameters.max), keep=True)

        #############################
        elif mfType == 'ldPrune':
            prunList = hl.ld_prune(data.GT, **parameters)
            data = data.filter_rows(hl.is_defined(prunList[data.row_key]))

        #############################
        elif mfType == 'splitMulti':
            mt = data
            mt = SplitMulti(mt, parameters)

        #############################
        elif mfType == 'forVep':
            data = ForVep(data)

        #############################
        elif mfType == 'describe':
            print("=============")
            print("=============")
            data.describe()
            print("=============")
            print("=============")

    return data

@D_General
def AddId(mt, parameters):

    if 'sampleId' in parameters:
        mt = mt.annotate_cols(sampleId=mt[parameters.sampleId])
        mt = mt.key_cols_by('sampleId')
    
    if 'variantId' in parameters:
        if parameters.variantId=='CHR:POS:ALLELES':
            mt = mt.annotate_rows(variantId=hl.str(':').join(hl.array([mt.locus.contig, hl.str(mt.locus.position)]).extend(mt.alleles)))
        else:
            LogException('VariantId is not Supported')
    
    return mt

@D_General
def ForVep(mt):

    mt = mt.annotate_rows(rsid=hl.str(mt.variantId))
    ht = mt.rows().select('rsid')
    mt = hl.MatrixTable.from_rows_table(ht)
    mt = mt.annotate_cols(sampleId='dummy') # Neded For VCF Export purpose
    mt = mt.key_cols_by(mt.sampleId)
    return mt