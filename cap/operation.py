
from inspect import Arguments
import time

import hail as hl
from hail.expr.expressions.expression_typecheck import T
from hail.methods.impex import export_bgen
from munch import Munch
from .logutil import *
from .common import *
from .helper import *
from .shared import Shared

if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

@D_General
def ByPass(stage):
    specifications, parameters, inouts, runtimes = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = inouts.inData
    outData = inouts.outData

    ##### >>>>>>> Live Input <<<<<<<<
    data = inData.GetData()

    ##### >>>>>>> STAGE Code <<<<<<<<
    # DO Nothing (Just Common Operation)

    ##### >>>>>>> Live Output <<<<<<<<
    outData.SetData(data)

@D_General
def SplitMatrixTable(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = GetFile(io.inData)
    outData = GetFile(io.outData)
    outCol = GetFile(io.outCol)
    outRow = GetFile(io.outRow)


    ##### >>>>>>> Live Input <<<<<<<<
    mt = inData.data

    ##### >>>>>>> STAGE Code <<<<<<<<

    if 'sampleId' not in set(mt.col):
        LogException('sampleId not presented')
    if 'variantId' not in set(mt.row):
        LogException('variantId not presented')

    # extract row and col table and drop them from matrix table
    ht_col = mt.cols()
    ht_col = ht_col.key_by('sampleId')

    ht_row = mt.rows()
    ht_row = ht_row.key_by('variantId')

    dropKeys = (set(mt.row).union(set(mt.col)))-{'alleles', 'locus', 'sampleId', 'variantId'}
    mt = mt.drop(*list(dropKeys))

    ##### >>>>>>> Live Output <<<<<<<<
    outData.SetData(mt)
    outCol.SetData(ht_col)
    outRow.SetData(ht_row)

@D_General
def KeyByTable(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = GetFile(io.inData)
    outData = GetFile(io.outData)
    keyTable = GetFile(io.keyTable)


    ##### >>>>>>> Live Input <<<<<<<<
    ht = inData.data
    htKey = keyTable.data

    ##### >>>>>>> STAGE Code <<<<<<<<
    keyCol = arg.keyCol
    joinColData = arg.joinColData
    joinColKey = arg.joinColKey

    htKey = htKey.key_by(keyCol)
    htKey = htKey.select(joinColKey)

    ht = ht.key_by(joinColData)
    htKey = htKey.key_by(joinColKey)

    ht = ht.join(htKey, how='left')
    ht = ht.key_by(keyCol)

    ##### >>>>>>> Live Output <<<<<<<<
    outData.SetData(ht)

@D_General
def PcaHweNorm(stage):
    specifications, parameters, inouts, runtimes = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = inouts.inData
    outScores = inouts.outScores

    ##### >>>>>>> Live Input <<<<<<<<
    mt = inData.data

    ##### >>>>>>> STAGE Code <<<<<<<<

    try:
        cl = dict()
        if 'outPcaLoading' in inouts:
            cl['compute_loadings'] = True
        else:
            cl['compute_loadings'] = False
        eigenvalues, pcs, loading = hl.hwe_normalized_pca(mt.GT, k=parameters.numPcaVectors, **cl)
        pcs = FlattenTable(pcs)
    except:
        LogException('Hail cannot perform the pca analysis')

    logger.info(f'PCA is computed')

    ##### >>>>>>> Live Output <<<<<<<<
    outScores.SetData(pcs)
    # if 'outPcaEigen' in io:
    #     io.outPcaEigen.data = eigenvalues
    # if 'outPcaLoading' in io:
    #     io.outPcaLoading.data = loading
    # if 'outPcaVarList' in io:
    #     io.outPcaVarList.data = mt.rows().select()

@D_General
def HailAssociation(stage):
    specifications, parameters, inouts, runtimes = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inMt = inouts.inMt
    inTable = Munch()
    for inout in inouts:
        if inout.startswith('inTable_'):
            inTable[inout] = inouts[inout]
    outTable = Munch()
    for inout in inouts:
        if inout.startswith('outTable_'):
            outTable[inout] = inouts[inout]

    ##### >>>>>>> Live Input <<<<<<<<
    mt = inMt.data
    inData = Munch()
    for inout in inouts:
        if inout.startswith('inTable_'):
            inData[inout] = inouts[inout].GetData()
            
    ##### >>>>>>> STAGE Code <<<<<<<<

    outData = Munch()
    for inout in inouts:
        if inout.startswith('outTable_'):
            outData[inout] = None

    for test in parameters.tests:
        htCovar = None
        if 'covariates' in test:
            for cv in test.covariates:
                ht = inData[cv.table]
                ht = ht.select(*cv.cols)
                if htCovar:
                    htCovar = htCovar.join(ht, how='outer')
                else:
                    htCovar = ht

        covars = [1]#, list()
        for field in htCovar.row_value:
            covars.append(htCovar[mt.sampleId][field])

        if 'responseVariables' in test:
            for table in test.responseVariables:
                ht = inData[table.tableId]
                for col in table.cols:

                    if test.type in ['LogReg', 'LinReg']:
                        if test.type == 'LogReg':
                            res = hl.logistic_regression_rows(
                                test=test.subType, y=ht[mt.sampleId][col.colName], x=mt.GT.n_alt_alleles(), 
                                covariates=covars, pass_through=['variantId'])
                        elif test.type == 'LinReg':
                            res = hl.linear_regression_rows(
                                y=ht[mt.sampleId][col.colName], x=mt.GT.n_alt_alleles(), 
                                covariates=covars, pass_through=['variantId'])
                        res = res.key_by('variantId')
                        res = res.drop('locus', 'alleles')
                    
                    elif test.type == 'skat':
                        htKeyExpr = inData[test.keyExpr.tableId]
                        colKeyExpr = test.keyExpr.colName

                        htWeigthExpr = inData[test.weightExpr.tableId]
                        colWeightExpr = test.weightExpr.colName

                        res = hl.skat(
                            key_expr=htKeyExpr[mt.locus, mt.alleles][colKeyExpr],
                            weight_expr=htWeigthExpr[mt.locus, mt.alleles][colWeightExpr],
                            y=ht[mt.sampleId][col.colName], x=mt.GT.n_alt_alleles(), 
                            covariates=covars, **col.param)

                        #res = res.key_by(test.keyExpr.col)
                        #res = res.drop('locus', 'alleles')


                    expr = {col.testName: hl.struct(**dict(res.row_value))}
                    res = res.annotate(**expr)
                    res = res.select(res[col.testName])
                    if outData[col.resTableId]:
                        outData[col.resTableId] = outData[col.resTableId].join(res, how='outer')
                    else:
                        outData[col.resTableId] = res

    ##### >>>>>>> Live Output <<<<<<<<
    for inout in inouts:
        if inout.startswith('outTable_'):
            inouts[inout].SetData(outData[inout])


@D_General
def ImportMatrixTable(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    input = io.input
    output = io.output

    ##### >>>>>>> Live Input <<<<<<<<

    ##### >>>>>>> STAGE Code <<<<<<<<
    # Loading input genotype
    try:
        if 'importParam' not in arg:
            arg.importParam = dict()
        if 'inputFormat' in arg:
            if arg.inputFormat != input.format:
                LogException(f'input format mentioned in arg {arg.inputFormat} is different from input format of the io input {input.format}')

        if input.format == 'vcf':
            mt = hl.import_vcf(input.path, **arg.importParam)
        elif input.format == 'bfile':
            mt = hl.import_plink(bed=f'{input.path}.bed', bim=f'{input.path}.bim', fam=f'{input.path}.fam', **arg.importParam)
        else:
            LogException(f'input.format ({input.format}) is not supported')
    except:
        LogException('Hail cannot read genotype data.')
    Log(f'Genotypes are loaded from "{input.path}".')

   

    ##### >>>>>>> Live Output <<<<<<<<
    output.data = mt


@D_General
def ExportMatrixTable(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    input = io.input
    output = io.output

    ##### >>>>>>> Live Input <<<<<<<<
    mt = Shared.data[input.path]

    ##### >>>>>>> STAGE Code <<<<<<<<


    # If epxorting for VEP Overwire all other parameters
    if 'forVep' in arg and arg.forVep:
        # TBF arg may not present
        # if arg.outputFormat != 'vcf' or output.format != 'vcf' or output.compression != 'bgz':
        #     LogException('When exporting for VEP (forVep=True) arg.outputFormat and output.format must be "vcf" and output.compression must be "bgz"')
        # if 'exportParam' not in arg or 'parallel' not in arg.exportParam or arg.exportParam.parallel != 'separate_header':
        #     LogException('When exporting for VEP (forVep=True) arg.exportParam.parallel must be set to "separate_header"')

        try:
            ht = mt.rows().select('variantId')
            mt = hl.MatrixTable.from_rows_table(ht)
            mt = mt.annotate_cols(sample='x')
            mt = mt.key_cols_by(mt.sample)
            mt = mt.annotate_rows(rsid=hl.str(mt.variantId))
        except:
            LogException('Could not extract variant-only MatrixTable for VEP')
    else:
        mt = mt.annotate_cols(ID=hl.str(mt.sampleId))
        mt = mt.key_cols_by(mt.ID)

    # Export Genotypes
    try:
        if 'exportParam' not in arg:
            arg.exportParam = dict()
        # if arg.outputFormat == 'vcf' and output.format == 'vcf':
        if output.format == 'vcf':
            hl.export_vcf(mt, output.path, **arg.exportParam)
        # elif arg.outputFormat == 'bfile' and output.format == 'bfile':
        elif output.format == 'bfile':
            for k in ['call', 'fam_id', 'ind_id', 'pat_id', 'mat_id', 'is_female', 'pheno', 'varid', 'cm_position']:
                if k in arg.exportParam:
                    arg.exportParam[k] = HailPath([mt]+arg.exportParam[k])
            hl.export_plink(mt, output.path, **arg.exportParam)
        else:
            LogException(f'Output format is not properly set. Make sure arg.outputFormat ({arg.outputFormat}) and output.format ({output.format}) are consistent.')

    except:
        LogException('Hail cannot write genotype data.')
    Log(f'Genotypes are stored from "{output.path}".')

    ##### >>>>>>> Live Output <<<<<<<<


@D_General
def MergeMatrixTables(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = io.inData
    outData = io.outData

    ##### >>>>>>> Live Input <<<<<<<<
    mts = [Shared.data[path] for path in inData.path]

    ##### >>>>>>> STAGE Code <<<<<<<<

    if not mts:
        LogException('No Matrix table is loaded')

    if 'joinType' in arg:
        joinType = arg.joinType
    else:
        joinType = 'inner'

    mt = mts[0]

    if len(mts) > 1:
        if arg.direction == 'row':
            for mt2 in mts[1:]:
                mt = mt.union_rows(mt2)
        elif arg.direction == 'col':
            for mt2 in mts[1:]:
                mt = mt.union_cols(mt2, row_join_type=joinType)


    ##### >>>>>>> Live Output <<<<<<<<
    outData.data = mt

@D_General
def Join(stage):
    specifications, parameters, inouts, runtimes = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = Munch()
    for inout in inouts:
        if inout.startswith('inData_'):
            inData[inout] = inouts[inout]
    outData = inouts.outData

    ##### >>>>>>> Live Input <<<<<<<<
    data = Munch()
    for inout in inouts:
        if inout.startswith('inData_'):
            data[inout] = inouts[inout].GetData()

    ##### >>>>>>> STAGE Code <<<<<<<<

    for i, item in enumerate(parameters.order):
        inTable = data[item.table]

        if not i:
            outTable = inTable
        else:
            inType = dataTypeMapper[type(inTable)]
            outType = dataTypeMapper[type(outTable)]

            kind = item.get('kind')
            how = item.get('how', 'inner')
            axis = item.get('axis')

            if inType=='ht' and outType=='ht' and axis:
                LogException('XXX')
            
            if (inType=='mt' or outType=='mt') and (axis not in ['rows', 'cols']):
                LogException('XXX')

            if inType=='mt':
                if axis=='rows':
                    inTable = inTable.rows()
                elif axis=='cols':
                    inTable = inTable.cols()
                inType = 'ht'


            axisFunc = f'_{axis}' if (axis and inType == 'mt') else ''

            if kind in ['semi', 'anti']:
                func = getattr(outTable, f'{kind}_join{axisFunc}')
                outTable = func(inTable)
            if kind=='annotate':
                strParameters = ', '.join([f'{k}={v}' for k,v in item.get('namedExpr', dict()).items()])
                statement = f'res = outTable.annotate{axisFunc}({strParameters})'
                ldict = locals()
                exec(statement, globals(), ldict)
                outTable = ldict['res']

            if kind=='join':
                if outType=='mt':
                    LogException('Not supported for now')
                if how not in ['inner', 'outer', 'left', 'right']:
                    LogException('XXX')
                outTable = outTable.join(inTable, how=how)

    ##### >>>>>>> Live Output <<<<<<<<
    outData.SetData(outTable)


@D_General
def ImportTable(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inPt = io.inPt
    inS = io.inS  # input samples with sample IDs
    outPt = io.outPt

    ##### >>>>>>> Live Input <<<<<<<<
    htS = Shared.data[inS.path]

    ##### >>>>>>> STAGE Code <<<<<<<<
    try:
        if 'importParam' not in arg:
            arg.importParam = dict()

        if inPt.format in ['tsv', 'tsv.bgz', 'tsv.gz']:
            sep = '\t'
        elif inPt.format in ['csv', 'csv.bgz', 'csv.gz']:
            sep = ','
        else:
            pass  # Already handled in Above

        imputeFlag = True
        if 'types' in arg.importParam:
            imputeFlag = False

        ht = hl.import_table(paths=inPt.path, impute=imputeFlag, delimiter=sep, **arg.importParam)
        ht = ht.key_by(arg.phenoKey)
    except:
        LogException('Hail cannot read phenotype data')
    Log(f'Phenotyps are loaded from "{inPt.path}"')

    htS = htS.key_by(arg.sampleKey)
    htS = htS.select('sampleId')
    ht = ht.join(htS)

    Log('Sample ids are added.')

    ##### >>>>>>> Live Output <<<<<<<<
    outPt.data = ht


@D_General
def CalcQC(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    input = io.input
    outQc = io.outQc

    ##### >>>>>>> Live Input <<<<<<<<
    mt = Shared.data[input.path]

    ##### >>>>>>> STAGE Code <<<<<<<<


    try:
        if arg.axis == 'sample':
            mt = hl.sample_qc(mt, name='qc')
            ht = mt.cols().select('qc')
        elif arg.axis == 'variant':
            mt = hl.variant_qc(mt, name='qc')
            ht = mt.rows()
            ht = ht.key_by('variantId')
            ht = ht.select('qc')
        else:
            pass  # Already handled above
    except:
        LogException(f'Hail cannot compute QC metrics for {arg.axis}')
    Log(f'PCA is computed')

    ##### >>>>>>> Live Output <<<<<<<<
    outQc.data = ht


@D_General
def VepAnnotation(stage):
    specifications, parameters, inouts, runtimes = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = inouts.inData
    outData = inouts.outData

    ##### >>>>>>> Live Input <<<<<<<<

    ##### >>>>>>> STAGE Code <<<<<<<<
    inFile = inData.dataFile.disk
    outFile = outData.dataFile.disk

    if outFile.get('format') != 'dir':
        LogException('Currently not supported')

    inPath = inFile.path[0]
    outPath = outFile.path[0]

    try:
        templateCommand = parameters.vepCli

        Bash(command=['mkdir', outPath], isPath=[False, True])

        if inFile.get('isParallel'):
            vcfList = WildCardPath(inPath + '/part-*.bgz')
        else:
            LogException('Currently not supported')

        numJob = len(vcfList)

        if 'isArrayJob' in parameters and parameters.isArrayJob:

            numSgeJobs = Shared.defaults.numSgeJobs

            if 'numSgeJobs' in parameters:
                if not (numSgeJobs.min <= parameters.numSgeJobs <= numSgeJobs.max):
                    LogException(f'numSgeJobs {parameters.numSgeJobs} must be in range [{numSgeJobs.min},{numSgeJobs.max}]')
            else:
                parameters.numSgeJobs = numSgeJobs.default

            # Get the absolute path to the scripts
            templateCommand[1] = AbsPath(templateCommand[1])
            templateCommand[10] = AbsPath(templateCommand[10])
            # submit the array job

            command = templateCommand
            command = [inPath if p == '__VCF_DIR__' else p for p in command]
            command = [outPath if p == '__JSON_DIR__' else p for p in command]
            command = [outPath if p == '__TBL_DIR__' else p for p in command]
            command = [outPath if p == '__JOB_DIR__' else p for p in command]
            command = [f'CAP' if p == '__JOB_NAME__' else p for p in command]
            command = ['1' if p == '__JOB_START__' else p for p in command]
            command = [str(numJob) if p == '__JOB_END__' else p for p in command]
            command = [str(parameters.numSgeJobs) if p == '__JOB_IN_PARALLEL__' else p for p in command]

            Bash(command, isPath=[False, True, True, True, True, True, False, False, False, False, True])
        else:
            # Get the absolute path to the scripts
            templateCommand[1] = AbsPath(templateCommand[1])
            templateCommand[7] = AbsPath(templateCommand[7])
            # submit a job for each VCF
            for vcf in vcfList:
                fileName = os.path.basename(vcf)
                code = fileName[5:10]
                command = templateCommand
                command = [vcf if p == '__IN_VCF__' else p for p in command]
                command = [os.path.join(outPath, f'part-{code}.json.bgz')if p == '__OUT_JSON__' else p for p in command]
                command = [os.path.join(outPath, f'part-{code}.table') if p == '__OUT_TBL__' else p for p in command]
                command = [os.path.join(outPath, f'part-{code}.job') if p == '__OUT_JOB__' else p for p in command]
                command = [f'CAP-{code}' if p == '__JOB_ID__' else p for p in command]

                Bash(command, isPath=[False, True, True, True, True, False, True, True])

        LogPrint(f'All {numJob} jobs are submitted.')

        # Wait Until all jobs are compeleted
        passed = 0
        while True:
            numCompeleted = 0
            for vcf in vcfList:
                fileName = os.path.basename(vcf)
                code = fileName[5:10]
                doneFile = os.path.join(outPath, f'part-{code}.job.done')
                if FileExist(doneFile, silent=True):
                    numCompeleted += 1
            if numCompeleted != numJob:
                time.sleep(Shared.defaults.vepCheckWaitTime)
                passed += Shared.defaults.vepCheckWaitTime
                LogPrint(f'{numCompeleted} out of {numJob} compeleted in {passed} second ...')
            else:
                break
        LogPrint(f'All {numJob} jobs are compeleted.')
    except:
        LogException(f'Can not extract vcf file list.')
    Log(f'VEP JSON and TBL files are created.')

    ##### >>>>>>> Live Output <<<<<<<<

@D_General
def VepLoadTables(stage):

    specifications, parameters, inouts, runtimes = UnpackStage(stage)
    tables = parameters.tables

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = inouts.inData
    if 'var' in tables:
        outVar = inouts.outVar
    if 'clvar' in tables:
        outClVar = inouts.outClVar
    if 'freq' in tables:
        outFreq = inouts.outFreq
    if 'conseq' in tables:
        outConseq = inouts.outConseq
        outConseqVar = inouts.outConseqVar
        outConseqTerms = inouts.outConseqTerms
        outVarTerms = inouts.outVarTerms

    ##### >>>>>>> Live Input <<<<<<<<

    ##### >>>>>>> STAGE Code <<<<<<<<
    inFile = inData.dataFile.disk
    inPath = inFile.path[0]

    if 'all' in tables:
        tables = ['var', 'clvar', 'freq', 'conseq']

    try:  # TBF it currently check if the folder exist or not. should find a way to check all parquet files
        if 'var' in tables:
            tblList = AbsPath(inPath + '/part-*.var.parquet')
            htVar = ImportMultipleTable(tblList)

        if 'clvar' in tables:
            tblList = AbsPath(inPath + '/part-*.clvar.parquet')
            htClVar = ImportMultipleTable(tblList, addFileNumber=True)

        if 'freq' in tables:
            tblList = AbsPath(inPath + '/part-*.freq.parquet')
            htFreq = ImportMultipleTable(tblList)

        if 'conseq' in tables:
            tblList = AbsPath(inPath + '/part-*.conseq.parquet')
            htConseq = ImportMultipleTable(tblList)
    except:
        LogException(f'Can not read parquet files')

    try:
        pass
        if 'clvar' in tables:
            # Process colocated-variants table
            htClVar = htClVar.annotate(clVarId=(htClVar.fileNumber * 2**32) + htClVar.clVarId)

        if 'conseq' in tables:
            # Process consequences table and group consequences
            # Select all columns except 'varId' to group by (remove duplicate)
            groupBykeys = list(dict(htConseq.row).keys())
            groupBykeys.remove('varId')
            # Put all 'varId' for each uniq consequence into an array
            htConseq = htConseq.group_by(*list(groupBykeys)).aggregate(varIds=hl.agg.collect(htConseq.varId))
            htConseq = htConseq.add_index('conseqId')
            htConseq = htConseq.key_by('conseqId')
            # create a table for many to many relationship between consequences and variants
            htConseqVar = htConseq.select('varIds')
            htConseqVar = htConseqVar.explode('varIds')
            htConseqVar = htConseqVar.rename({'varIds': 'varId'})
            # create a table to list consequence terms per consequence
            htConseqTerms = htConseq.select('consequence_terms', 'varIds')
            htConseqTerms = htConseqTerms.annotate(consequence_terms=hl.array(htConseqTerms.consequence_terms.replace('\[', '').replace('\]', '').replace('\'', '').split(',')))
            htConseqTerms = htConseqTerms.explode('consequence_terms')
            # create a table to list consequence terms per variant (this is for performance only but can be done by linking terms to consequences and then consequences to the variant).
            htVarTerms = htConseqTerms.explode('varIds')
            htVarTerms = htVarTerms.rename({'varIds': 'varId'})
            # drop un-neccessary field of each table
            htConseqTerms = htConseqTerms.select('consequence_terms')
            htVarTerms = htVarTerms.key_by('varId')
            htVarTerms = htVarTerms.select('consequence_terms')
            htConseq = htConseq.drop('consequence_terms', 'varIds')
    except:
        LogException('Cannot process tables.')

    Log(f'VEP parquet files are converted to hail tables.')

    ##### >>>>>>> Live Output <<<<<<<<
    if 'var' in tables:
        outVar.SetData(htVar)
    if 'clvar' in tables:
        outClVar.SetData(htClVar)
    if 'freq' in tables:
        outFreq.SetData(htFreq)
    if 'conseq' in tables:
        outConseq.SetData(htConseq)
        outConseqVar.SetData(htConseqVar)
        outConseqTerms.SetData(htConseqTerms)
        outVarTerms.SetData(htVarTerms)
