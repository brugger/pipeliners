#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (02 Mar 2018), contact: kim@brugger.dk

import sys
import os
import argparse
import pprint
import tempfile
import glob
pp = pprint.PrettyPrinter(indent=4)

sys.path.append("/mnt/storage/home/brugger//projects/ccbg_pipelines/")

from ccbg_pipeline import *

P = None
outfiles = {}
sample_name = None

VERSION        = '3.00-alpha, rushed bandit';
PARTITION      = " -p long ";
reference      = '/mnt/scratch/refs/human_1kg/human_g1k_v37.fasta';
targets_100bp  = '/mnt/scratch/refs/human_1kg/refseq_nirvana_203_100bp_flank.bed';
targets_5bp    = '/mnt/scratch/refs/human_1kg/refseq_nirvana_203_5bp_flank.bed';
manifest       = '/refs/human_1kg/TruSightOne/TSOE.list'

def tmpfile_name(suffix=""):
    tmpfile = tempfile.NamedTemporaryFile(dir='tmp/', suffix=suffix, delete=False)
    return tmpfile.name

def setup_directory():
    """ Creates the various output files needed by the analysis pipeline 

    Args:
      None

    Returns:
      None

    """

    subdirs = ['bams', 'vcfs', 'stats', 'logs', 'tmp']

    for subdir in subdirs:
        if not os.path.exists(subdir):
            
            try:
                os.makedirs(subdir, 0775)
            except:
                pass    


def setup_outfiles( sample_name ):
    """ configure the outfiles based on the sample name

    This furthermore opens up the logs by hijacking sys.stderr and sys.stdout

    Args:
      sample_name (str): the prefix of all the outfiles

    Returns:
      dict of file names (str)

    """

    outfiles[ 'bam_file' ]        = "bams/{name}.bam".format(name=sample_name)
    outfiles[ 'gvcf_file' ]       = "vcfs/{name}.refseq_nirvana_203.g.vcf".format(name=sample_name)
    outfiles[ 'vcf_file' ]        = "vcfs/{name}.refseq_nirvana_203.vcf".format(name=sample_name)
    outfiles[ 'flagstat_file' ]   = "stats/{name}.flagstat".format(name=sample_name)
    outfiles[ 'isize_file' ]      = "stats/{name}.isize".format(name=sample_name)
    outfiles[ 'vcf_QC_file' ]     = "stats/{name}.vcf.QC".format(name=sample_name)
    outfiles[ 'CalcHs_file' ]     = "stats/{name}.CalcHs".format(name=sample_name)
    outfiles[ 'exon_depth_file' ] = "stats/{name}.refseq_nirvana_203".format(name=sample_name)


    # open logfiles
#    sys.stdout = open("logs/{}.log".format( sample_name), 'a', buffering=1)
#    sys.stderr = open("logs/{}.error.log".format( sample_name), 'a', buffering=1)



    return outfiles



def samtools_version():
    """ Extract and returns the samtools version available

    returns None if not available

    Returns:
      samtools version (str)
    
    """
    p = subprocess.Popen(shlex.split("samtools"), shell=False, 
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    
    (stdout, stderr) = p.communicate();
    status = p.wait()

    m = re.findall('^Version: (.*?) ', stderr, re.MULTILINE)

    if ( m is not None and len(m) >= 1):
        return m[0]
        
    return None


def bwa_version():
    """ Extract and returns the bwa version available

    returns None if not available

    Returns:
      bwa version (str)
    
    """

    p = subprocess.Popen(shlex.split("bwa"), shell=False, 
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    
    (stdout, stderr) = p.communicate();
    status = p.wait()

    m = re.findall('^Version: (.*)', stderr, re.MULTILINE)
    if ( m is not None and len(m) >= 1):
        return m[0]
        
    return None


def gatk_version():
    """ Extract and returns the gatk version available

    returns None if not available

    Returns:
      gatk version (str)
    
    """

    p = subprocess.Popen(shlex.split("gatk --version"), shell=False, 
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    
    (stdout, stderr) = p.communicate();
    status = p.wait()

    m = re.findall('^GATK (.*)', stdout, re.MULTILINE)
    if ( m is not None and len(m) >= 1):
        return m[0]
        
    return None



def r_version():
    """ Extract and returns the R version available

    returns None if not available

    Returns:
      R version (str)
    
    """

    p = subprocess.Popen(shlex.split("R --version"), shell=False, 
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    
    (stdout, stderr) = p.communicate();
    status = p.wait()

    m = re.findall('^R version (.*?) ', stdout, re.MULTILINE)
    if ( m is not None and len(m) >= 1):
        return m[0]
        
    return None



def bwa_mem( inputs ):
    """ run bwa mem on a set of fastq files glob'ed from the input samplename

    Args:
      inputs (str): samplename to get the fastq files from

    Returns:
      the submit_job passes on the tmp bamfile
      

    """

    tmpfile = tmpfile_name('.bam')

    fwd = glob.glob("{sample}.1.fq.gz".format(sample = inputs)) + glob.glob("{sample}*_R1*.fastq.gz".format(sample = inputs))
    rev = glob.glob("{sample}.2.fq.gz".format(sample = inputs)) + glob.glob("{sample}*_R2*.fastq.gz".format(sample = inputs))

    for index, first_file in enumerate(fwd):
        cmd = "bwa mem -t 32 -M {reference} {first_file} {second_file} | samtools view -Sb -o {tmpfile} -  "
        cmd = cmd.format( reference=reference, first_file=first_file, second_file=rev[ index ], tmpfile=tmpfile)
        P.submit_job( cmd, output=tmpfile, limit=" -c 32 --mem=9G  {partition}".format( partition=PARTITION ))
   


def bam_merge(inputs):
    """merges input bam files into a single bamfile

    The inputs can either be a single bamfile, in this case the files
    is just renamed, alternatively merge the input files


    Args:
      inputs (list of str): list of bamfiles

    Returns:
      passes on the merged bamfile name

    """

    tmpfile = tmpfile_name('.merged.bam')

    if (len(inputs) == 1 ):
        P.system_job("mv {} {}".format( inputs[0], tmpfile), output=tmpfile )
    else:
        cmd = "gatk MergeSamFiles USE_THREADING=true O={output} I= {infiles} VALIDATION_STRINGENCY=SILENT CREATE_INDEX=false";
        cmd = cmd.format( output=tmpfile, infiles = " ".join(inputs))
        P.submit_job( cmd, output=tmpfile, limit=" -c 2 {partition}".format( partition=PARTITION ) )

def bam_sort( inputs ):
    """ add readgroups and sort the bamfile
    
    The function takes the global defined sample_name and sets it as the run_id

    Args:
      inputs (str): name of bam file to sort

    Returns:
      name of sorted bam file name

    """

    global sample_name
    run_id    = sample_name
    platform  = 'ILLUMINA'

    tmpfile = tmpfile_name(".bam")

    cmd = "gatk AddOrReplaceReadGroups -I {infile} -O {output} -SO coordinate -CN CTRU -PL {platform} -LB {run_id} -PU {run_id} -SM {run_id} --MAX_RECORDS_IN_RAM 1000000 --TMP_DIR /mnt/scratch/tmp/"
    cmd = cmd.format( infile=inputs, output=tmpfile, platform=platform, run_id=run_id)

    P.submit_job( cmd, limit=" -c 6 --mem=11G {partition}".format( partition=PARTITION ), output=tmpfile )



def mark_dups( inputs ):
    """ Mark duplicates in the bamfile, at the same time index and create md5 sum for the input bamfile

    Args:
      inputs (str): name of bamfile

    Returns:
      project bamfile

    """


    tmpfile = tmpfile_name(".bam")
    mtxfile = tmpfile_name(".mtx")
    pp.pprint( inputs )
    cmd = "gatk MarkDuplicates  -I {inputs} -O  {output}   -M  {mtxfile} -ASO coordinate".format( inputs=inputs, output=outfiles[ 'bam_file' ], mtxfile=mtxfile)

    cmd = "gatk MarkDuplicates  -I {inputs} -O  {output}   -M  {mtxfile} -ASO coordinate --CREATE_MD5_FILE --CREATE_INDEX"
    cmd = cmd.format( inputs=inputs, output=outfiles[ 'bam_file' ], mtxfile=mtxfile)



    P.submit_job( cmd, limit=" -c 2 --mem=5G {partition}".format( partition=PARTITION ), output=outfiles[ 'bam_file' ] )


def bam_clean( inputs ):
    """runs cleanSam from picard/gatk on the bamfile to ensure the
       integrity of the bamfile. This was initially done due to a bug in bwa,
       so might not be needed later

    Args:
      inputs (str): name of bamfile

    Returns:
      pass the inputs on to the next step

    """

    tmpfile = tmpfile_name(".bam")
    cmd = "gatk  CleanSam -I {infile} -O {outfile} ".format(infile=inputs, outfile = tmpfile)
    P.submit_job( cmd, limit=" -c 2 --mem=5G {partition}".format( partition=PARTITION ), output=tmpfile )



def bam_reheader( inputs ):

    tmp_headerfile  = tmpfile_name(".sam.header")

    
    cmd = "samtools view -H {infile}".format(infile=inputs)

    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    (stdout, stderr) = p.communicate();
    p.wait()


    fh = open( tmp_headerfile, 'a')
    fh.write( stdout )
    version_line = "@PG\tID:{name}\tVN:{version}\n"
    fh.write( version_line.format( name = 'samtools',      version = samtools_version()))
    fh.write( version_line.format( name = 'gatk',          version = gatk_version()    ))
    fh.write( version_line.format( name = 'r',             version = r_version()       ))
    fh.write( version_line.format( name = 'ccbg_pipeline', version = VERSION ))
    fh.write( version_line.format( name = 'pipeliners',    version = VERSION ))

    fh.close()

    tmpfile  = tmpfile_name(".bam")

    cmd = "samtools reheader -P {headerfile} {infile} > {outfile}".format( headerfile=tmp_headerfile, infile=inputs, outfile=tmpfile)
    P.submit_job( cmd, limit=" -c 2 --mem=1G {partition}".format( partition=PARTITION ), output=tmpfile )



def bam_index(inputs):
    """ index'es a bamfile with samtools

    Args:
      inputs (str): name of bam file

    Returns:
      pass on the inputs to the next step

    """

    cmd = "samtools index {}".format(inputs)
    P.submit_job( cmd, limit=" -c 2 --mem=1G {partition}".format( partition=PARTITION ), output=inputs )



def haplotypecaller( inputs ):
    """ Call variants, only regions in target_100 are called, output type is in gvcf

    We call as gvcf so the files can be used later on for trio/duo calling

    Args:
      inputs (str): project bamfile 

    Returns:
      passes on the gvcf project filename

    """

  
    cmd = "gatk HaplotypeCaller -R {reference} -I {infile} -O {outfile} -L {targets_100bp}  --ERC GVCF"
    cmd = cmd.format( reference=reference, infile=outfiles[ 'bam_file' ], outfile=outfiles[ 'gvcf_file' ], targets_100bp=targets_100bp)
    P.submit_job( cmd, limit=" -c 4 --mem=7G {partition}".format( partition=PARTITION ), output=outfiles[ 'gvcf_file' ] )


def extract_variants( inputs ):
    """Converts gvcf file into vcf 

    Args:
      inputs (str): name of the gvcf file

    Returns:
      passes on the vcf project filename

    """

    cmd = "gatk  GenotypeGVCFs -R {reference} -V {infile} -O {outfile}"
    cmd = cmd.format( infile=outfiles[ 'gvcf_file' ], reference=reference, outfile=outfiles[ 'vcf_file' ])
    P.submit_job( cmd, limit=" -c 2 --mem=1G {partition}".format( partition=PARTITION ), output=outfiles[ 'vcf_file' ] )


def flagstats( inputs=None ):
    """ calculates the flagstat for the project bamfile

    Args:
      inputs (str): name of bamfile (ignored)

    Returns:
      passes on project flagstat filename

    """

    cmd = "samtools flagstat {infile} > {outfile}".format(infile=outfiles[ 'bam_file' ], outfile=outfiles[ 'flagstat_file' ])
    P.submit_job( cmd, limit=" -c 1 --mem=1G {partition}".format( partition=PARTITION ), output=outfiles[ 'flagstat_file' ] )


def capture_stats(inputs=None):
    """ calculates the flagstat for the project bamfile

    Args:
      inputs (str): name of bamfile (ignored)

    Returns:
      passes on project flagstat filename

    """

    cmd = "gatk CollectHsMetrics -BI {manifest} -TI /refs/human_1kg/TruSightOne/TSO.list -I {infile} -O {outfile} "
    cmd = cmd.format( manifest=manifest, infile=outfiles[ 'bam_file' ], outfile=outfiles[ 'CalcHs_file' ])

    P.submit_job( cmd, limit=" -c 1 --mem=1G {partition}".format( partition=PARTITION ), output=outfiles[ 'CalcHs_file' ] )


def isize_stats(inputs=None):
    """ calculates the isize stats for the project bamfile
    Args:
      inputs (str): name of bamfile (ignored)

    Returns:
      passes on project isize metrix filename

    """

    cmd = "gatk CollectInsertSizeMetrics -I {infile} -O {outfile} -H {outfile}.pdf"
    cmd = cmd.format( infile=outfiles[ 'bam_file' ], outfile=outfiles[ 'isize_file' ])

    P.submit_job( cmd, limit=" -c 1 --mem=1G {partition}".format( partition=PARTITION ), output=outfiles[ 'isize_file' ] )



def vcf_qc( inputs=None ):
    """ calculates rough vcf QC  for the project vcf file

    Args:
      inputs (str): name of vcf file (ignored)

    Returns:
      passes on project vcf QC filename

    """
    pass



def import_qc_stats( inputs=None ):
    """ calculates the flagstat for the project bamfile

    Args:
      inputs (str): name of bamfile (ignored)

    Returns:
      passes on project flagstat filename

    """
    pass



def finished( inputs=None ):
    """ All done, this makes a [SAMPLE-NAME].done file to show we are all done here

    """
    pass






def setup_pipeline(pipeline):

#    primary = P.start_step( bwa_mem ).merge( bam_merge ).next( bam_sort )

    primary = P.start_step( bwa_mem ).merge( bam_merge ).next( bam_sort ).next( bam_clean ).next( bam_reheader ).next( mark_dups )#.next( bam_index )
    primary.next( haplotypecaller).next( extract_variants).next( vcf_qc ).merge( import_qc_stats )
    primary.next( flagstats ).merge( import_qc_stats )
    primary.next( isize_stats ).merge( import_qc_stats )
    primary.next( capture_stats ).merge( import_qc_stats ).next( finished )



    return pipeline






if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='vcf_compare.py: compare variants between two vcf files ')

    parser.add_argument('sample', metavar='sample', nargs=1,  help="sample to analyse")
    args = parser.parse_args()

    

    P = Pipeline()
    #P.backend( Local() )
    P.backend( Slurm() )
    
    setup_pipeline( P )
    setup_directory()    
    sample_name = args.sample[0]
    outfiles = setup_outfiles( sample_name )

    
    P.run( args=sample_name )
