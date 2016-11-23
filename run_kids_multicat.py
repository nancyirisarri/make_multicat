#!/usr/bin/python

import glob
import subprocess
import os

os.chdir('/net/virgo01/data/users/irisarri/CombinedCats/')

os.environ["AWEPIPE"] = "/Users/users/irisarri/awe"

# Find all the objects.
dir_objs = glob.glob(
  '/net/virgo01/data/users/irisarri/CombinedCats/logs/*-make_sourcelist_high_s_n.log'
)

objs = [
  i[i.find('KIDS'):i.find('-make')] for i in dir_objs
]

# Find all the objects that have been done.
objs_done = glob.glob(
  '/net/virgo01/data/users/irisarri/CombinedCats/multicats/*-1.0arcsec-SEflag3.pkl'
  ) +\
  glob.glob(
    '/net/virgo01/data/users/irisarri/CombinedCats/multicats/*-1.0arcsec-SEflag3.fits'
  ) 
  
objs_done = [
  i[i.find('KIDS'):i.find('-1.0')] for i in objs_done
]

# Remove the objects that have been done.
for i in objs_done:
    if i in objs:
        indx = objs.index(i)
        curr = objs.pop(indx)

objs = sorted(objs)
        
print objs

# Loop through the remaining objects.
for i in objs:
    filename_log_sl = glob.glob(
      '/net/virgo01/data/users/irisarri/CombinedCats/logs/*%s-make_sourcelist_high_s_n.log' % i
    )[0]

    al_type = 2
    
    matching_distance = 1.0
    
    flag_mask = 3
    
    try:    
        subprocess.call(
          'awe /net/virgo01/data/users/irisarri/CombinedCats/make_associatelist_multicat.py %s %s %s %s' %\
          (filename_log_sl, al_type, matching_distance, flag_mask),
          shell=True
        )
    except:
        continue
    
    try:
        subprocess.call(
          '/usr/bin/python /net/virgo01/data/users/irisarri/CombinedCats/convert_kids_multicat.py %s' % i,
          shell=True
        )
    except:
        continue
