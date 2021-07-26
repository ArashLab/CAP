
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
    print('This module is not executable. Import this module in your program.')
    exit(0)

@D_General
def CommonOperations(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    input = io.input
    output = io.output

    ##### >>>>>>> Live Input <<<<<<<<
    mht = Shared.data[input.path]

    ##### >>>>>>> STAGE Code <<<<<<<<
    # DO Nothing (Just Common Operation)

    ##### >>>>>>> Live Output <<<<<<<<
    output.data = mht

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

    # Perform Common Operation if presented
    if 'commonOperations' in arg:
        mt = CommonMatrixTableOperations(mt=mt, operations=arg.commonOperations)

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

    # Perform Common Operation if presented
    if 'commonOperations' in arg:
        mt = CommonMatrixTableOperations(mt=mt, operations=arg.commonOperations)

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
    input = io.input
    output = io.output

    ##### >>>>>>> Live Input <<<<<<<<
    mts = [Shared.data[path] for path in input.path]

    ##### >>>>>>> STAGE Code <<<<<<<<

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

    # Perform Common Operation if presented
    if 'commonOperations' in arg:
        mt = CommonMatrixTableOperations(mt=mt, operations=arg.commonOperations)

    ##### >>>>>>> Live Output <<<<<<<<
    output.data = mt


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
def ToMySql(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inHt = io.inHt

    ##### >>>>>>> Live Input <<<<<<<<
    ht = Shared.data[inHt.path]

    ##### >>>>>>> STAGE Code <<<<<<<<
    ht = FlattenTable(ht)
    try:
        # TBF overwrite? really?
        ht.to_spark().write.format('jdbc').options(**arg.mySqlConfig).mode('overwrite').save()
    except:
        LogException('Hail cannot write data into MySQL database')
    Log(f'Data is exported to MySQL')

    ##### >>>>>>> Live Output <<<<<<<<


@D_General
def ToText(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inHt = io.inHt
    outText = io.outText

    ##### >>>>>>> Live Input <<<<<<<<
    ht = Shared.data[inHt.path]

    ##### >>>>>>> STAGE Code <<<<<<<<
    ht = FlattenTable(ht)
    try:
        if 'exportParam' not in arg:
            arg.exportParam = dict()
        ht.export(outText.path, **arg.exportParam)
    except:
        LogException(f'Hail cannot write data into a file {outText.path}')
    Log(f'Data is exported to {outText.path}')

    ##### >>>>>>> Live Output <<<<<<<<

@D_General
def PcaHweNorm(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    input = io.input
    outPcaScore = io.outPcaScore

    ##### >>>>>>> Live Input <<<<<<<<
    mt = Shared.data[input.path]

    ##### >>>>>>> STAGE Code <<<<<<<<

    # Perform Common Operation if presented
    if 'commonOperations' in arg:
        mt = CommonMatrixTableOperations(mt=mt, operations=arg.commonOperations)

    try:
        cl = dict()
        if 'outPcaLoading' in io:
            cl['compute_loadings'] = True
        else:
            cl['compute_loadings'] = False
        eigenvalues, pcs, loading = hl.hwe_normalized_pca(mt.GT, k=arg.numPcaVectors, **cl)
    except:
        LogException('Hail cannot perform the pca analysis')

    logger.info(f'PCA is computed')

    ##### >>>>>>> Live Output <<<<<<<<
    outPcaScore.data = pcs
    if 'outPcaEigen' in io:
        io.outPcaEigen.data = eigenvalues
    if 'outPcaLoading' in io:
        io.outPcaLoading.data = loading
    if 'outPcaVarList' in io:
        io.outPcaVarList.data = mt.rows().select()

@D_General
def CalcQC(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    input = io.input
    outQc = io.outQc

    ##### >>>>>>> Live Input <<<<<<<<
    mt = Shared.data[input.path]

    ##### >>>>>>> STAGE Code <<<<<<<<

    # Perform Common Operation if presented
    if 'commonOperations' in arg:
        mt = CommonMatrixTableOperations(mt=mt, operations=arg.commonOperations)

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
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inVar = io.inVar
    outData = io.outData  # use the path for the output files

    ##### >>>>>>> Live Input <<<<<<<<

    ##### >>>>>>> STAGE Code <<<<<<<<
    try:
        path = inVar.path
        templateCommand = arg.vepCli

        Bash(command=['mkdir', outData.path], isPath=[False, True])

        vcfList = WildCardPath(path + '/part-*.bgz')
        numJob = len(vcfList)

        if 'isArrayJob' in arg and arg.isArrayJob:

            numSgeJobs = Shared.numSgeJobs

            if 'numSgeJobs' in arg:
                if not (numSgeJobs.min <= arg.numSgeJobs <= numSgeJobs.max):
                    LogException(f'numSgeJobs {arg.numSgeJobs} must be in range [{numSgeJobs.min},{numSgeJobs.max}]')
            else:
                arg.numSgeJobs = numSgeJobs.default

            # Get the absolute path to the scripts
            templateCommand[1] = AbsPath(templateCommand[1])
            templateCommand[10] = AbsPath(templateCommand[10])
            # submit the array job

            command = templateCommand
            command = [path if p == '__VCF_DIR__' else p for p in command]
            command = [outData.path if p == '__JSON_DIR__' else p for p in command]
            command = [outData.path if p == '__TBL_DIR__' else p for p in command]
            command = [outData.path if p == '__JOB_DIR__' else p for p in command]
            command = [f'CAP' if p == '__JOB_NAME__' else p for p in command]
            command = ['1' if p == '__JOB_START__' else p for p in command]
            command = [str(numJob) if p == '__JOB_END__' else p for p in command]
            command = [str(arg.numSgeJobs) if p == '__JOB_IN_PARALLEL__' else p for p in command]

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
                command = [os.path.join(outData.path, f'part-{code}.json.bgz')if p == '__OUT_JSON__' else p for p in command]
                command = [os.path.join(outData.path, f'part-{code}.table') if p == '__OUT_TBL__' else p for p in command]
                command = [os.path.join(outData.path, f'part-{code}.job') if p == '__OUT_JOB__' else p for p in command]
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
                doneFile = os.path.join(outData.path, f'part-{code}.job.done')
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
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    inData = io.inData

    ##### >>>>>>> Live Input <<<<<<<<

    ##### >>>>>>> STAGE Code <<<<<<<<
    if 'all' in arg.tables:
        arg.tables = ['var', 'clvar', 'freq', 'conseq']

    path = inData.path
    try:  # TBF it currently check if the folder exist or not. should find a way to check all parquet files
        if 'var' in arg.tables:
            tblList = AbsPath(path + '/part-*.var.parquet')
            htVar = ImportMultipleTable(tblList)

        if 'clvar' in arg.tables:
            tblList = AbsPath(path + '/part-*.clvar.parquet')
            htClVar = ImportMultipleTable(tblList, addFileNumber=True)

        if 'freq' in arg.tables:
            tblList = AbsPath(path + '/part-*.freq.parquet')
            htFreq = ImportMultipleTable(tblList)

        if 'conseq' in arg.tables:
            tblList = AbsPath(path + '/part-*.conseq.parquet')
            htConseq = ImportMultipleTable(tblList)
    except:
        LogException(f'Can not read parquet files')

    try:
        if 'clvar' in arg.tables:
            # Process colocated-variants table
            htClVar = htClVar.annotate(clVarId=(htClVar.fileNumber * 2**32) + htClVar.clVarId)

        if 'conseq' in arg.tables:
            # Process consequences table and group consequences
            # Select all columns except 'varId' to group by (remove duplicate)
            groupBykeys = list(dict(htConseq.row).keys())
            groupBykeys.remove('varId')
            # Put all 'varId' for each uniq consequence into an array
            htConseq = htConseq.group_by(*list(groupBykeys)).aggregate(varIds=hl.agg.collect(htConseq.varId))
            htConseq = htConseq.add_index('conseqId')
            htConseq = htConseq.key_by('conseqId')
            # create a table for many to many relationship between consequences and variants
            htConseqToVar = htConseq.select('varIds')
            htConseqToVar = htConseqToVar.explode('varIds')
            htConseqToVar = htConseqToVar.rename({'varIds': 'varId'})
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
    if 'var' in arg.tables:
        io.outVar.data = htVar
    if 'clvar' in arg.tables:
        io.outClVar.data = htClVar
    if 'freq' in arg.tables:
        io.outFreq.data = htFreq
    if 'conseq' in arg.tables:
        io.outConseq.data = htConseq
        io.outConseqToVar.data = htConseqToVar
        io.outConseqTerms.data = htConseqTerms
        io.outVarTerms.data = htVarTerms



@D_General
def AddId(stage):
    spec, arg, io = UnpackStage(stage)

    ##### >>>>>>> Input/Output <<<<<<<<
    input = io.input
    output = io.output
    outCol = io.outCol
    outRow = io.outRow

    ##### >>>>>>> Live Input <<<<<<<<
    mt = Shared.data[input.path]

    ##### >>>>>>> STAGE Code <<<<<<<<

    # Perform Common Operation if presented
    if 'commonOperations' in arg:
        mt = CommonMatrixTableOperations(mt=mt, operations=arg.commonOperations)

    # Add indexes
    mt = mt.annotate_rows(variantId=hl.str(':').join(hl.array([mt.locus.contig, hl.str(mt.locus.position)]).extend(mt.alleles)))
    mt = mt.annotate_cols(sampleId=mt[arg.sampleId])

    mt = mt.key_cols_by('sampleId')

    # extract row and col table and drop them from matrix table
    ht_col = mt.cols()
    ht_col = ht_col.key_by('sampleId')

    ht_row = mt.rows()
    ht_row = ht_row.key_by('variantId')

    dropKeys = (set(mt.row).union(set(mt.col)))-{'alleles', 'locus', 'sampleId', 'variantId'}
    mt = mt.drop(*list(dropKeys))

    ##### >>>>>>> Live Output <<<<<<<<
    output.data = mt
    outCol.data = ht_col
    outRow.data = ht_row
