#!/usr/bin/python

import glob
import subprocess
import os
import sys

os.chdir('/net/virgo01/data/users/irisarri/CombinedCats/')

os.environ["AWEPIPE"] = "/Users/users/irisarri/awe"

i = sys.argv[1]

filename_log_al = glob.glob(
  '/net/virgo01/data/users/irisarri/CombinedCats/*%s-make_associatelist_multicat.log' % i
)[0]

with open(filename_log_al, 'r') as fp:
    alid = fp.read().split('\n')
    
alid = [j for j in list(alid) if '4-band ALID' in j][0]
alid = alid.split()[-1]

subprocess.call(
  'mv %s /net/virgo01/data/users/irisarri/CombinedCats/logs/' % filename_log_al,
  shell=True
)
    
try:        
    subprocess.call(
      'awe /Users/users/irisarri/awe/astro/experimental/kids/Convert4band.py %s fits' %\
      alid,
      shell=True
    )
except:
    raise SystemExit

try:
    subprocess.call(
      'rsync /net/virgo01/data/users/irisarri/CombinedCats/%s-1.0arcsec-SEflag3.fits\
       irisarri@schipbeek.strw.leidenuniv.nl:/data1/irisarri/CombinedCats/catalogs/kids_dr1/' %\
      i, 
      shell=True
    )
except:
    print '\nrsync to my Leiden dir failed'

try:
    subprocess.call(
      'rsync /net/virgo01/data/users/irisarri/CombinedCats/%s-1.0arcsec-SEflag3.fits\
       irisarri@schipbeek.strw.leidenuniv.nl:/disks/shear10/KiDS/Awe/catalogs_4band/' %\
      i, 
      shell=True
    )
except:
    print '\nrsync to Leiden shared dir failed'

subprocess.call(
  'mv /net/virgo01/data/users/irisarri/CombinedCats/%s-1.0arcsec-SEflag3.fits\
   /net/virgo01/data/users/irisarri/CombinedCats/multicats/' % i,
  shell=True
)

subprocess.call(
  'mv /net/virgo01/data/users/irisarri/CombinedCats/%s-1.0arcsec-SEflag3.log\
   /net/virgo01/data/users/irisarri/CombinedCats/multicats/' % i,
  shell=True
)
