'''
USAGE:

> awe make_kids_pickle.py DATE-KIDS_RA_DEC-make_associatelist_multicat.log

with arguments:
1) locally stored log file of the script make_associatelist_multicat.py, which
contains the ALID from which the multi-band catalog will be made. If a log file
is not available but you know the ALID, then make a text file with a line
followed by a newline containing '4-band ALID' and the number. E.g. text
file contents:
4-band ALID 1779021

To run as module it will need an ALID as the instance variable alid. EXAMPLE:

catalog = Multicat(
  filename_log_al=None,
  alid=1779021
)
catalog.make_catalog_pkl()
'''

from astro.main.SourceList import SourceList
from astro.main.AssociateList import AssociateList

from common.log.Message import Message
from common.database.Context import context

import subprocess
import sys
from datetime import datetime
import numpy as np
import pickle
import glob
import traceback
import numpy as np

class Multicat:
    '''Make a pickle file that contains desired columns from a 4-band
    AssociateList.

    To run as script, needs as argument a log file of the script
    make_associatelist_multicat.py, which contains the ALID from which the
    multi-band catalog will be made,.
    '''

    def __init__(self, filename_log_al, alid, filename_catalog=None):
        '''Necessary variables if run as script:

        filename_log_al: log file of the script make_associatelist_multicat.py,
        which contains the ALID from which the multi-band catalog will be made.

        Necessary variables if run as module:

        alid: 4-band AssociateList ALID from which the multi-band catalog will
        be made.

        To keep less or more SourceList columns in the catalog, modify
        attr_list. Accordingly modify line_source and line_nines for the
        number of columns and the formatting in method append_to.

        For a source with data, line_source will be concatenated with its data
        and eventually dumped to the catalog. If no data available, then
        line_nines is dumped.
        '''
        self.filename_log_al = filename_log_al
        self.alid = alid
        self.attr_list = [
          'SLID', 'SID', 'RA', 'DEC', 'MAG_AUTO', 'MAGERR_AUTO', 'KRON_RADIUS',
          'MAG_ISO', 'MAGERR_ISO', 'MAG_ISOCOR', 'MAGERR_ISOCOR',
          'FLUX_APER_10', 'FLUXERR_APER_10', 'FLUX_APER_20', 'FLUXERR_APER_20',
          'FLUX_APER_40', 'FLUXERR_APER_40', 'FLUX_APER_100',
          'FLUXERR_APER_100', 'Flag', 'IMAFLAGS_ISO', 'NIMAFLAGS_ISO',
          'FLUX_RADIUS', 'FWHM_IMAGE', 'ELLIPTICITY', 'POSANG', '2DPHOT',
          'CLASS_STAR'
        ]
        self.line_source = '%i %i %12.6f %12.6f %8.3f %7.4f %1.1f %8.3f %7.4f %8.3f %7.4f %s %s %s %s %s %s %s %s %i %i %i %8.3f %8.3f %8.3f %8.3f %i %8.3f\t'
        self.line_nines = 'nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan nan\t'
        self.filename_catalog = filename_catalog

    def get_alid(self, filename_log_al):
        '''Read the log file and keep the ALID as integer.
        '''
        with open(filename_log_al, 'r') as fp:
            lines = fp.read()

        lines = [
          i for i in lines.split('\n') if
          '4-band ALID' in i
        ]

        return [int(i.split()[-1]) for i in lines]

    def do(self):
        '''Try to have a method with this name, in order to be consistent
        between scripts.
        '''
        pass

    def do_all(self):
        '''Try to have a method with this name, in order to be consistent
        between scripts.
        '''
        pass

    def set_filename_catalog(self, filename_catalog):
        '''Set instance variable of the filename of the catalog. Need to do in
        order to use in function rsync_catalog.
        '''
        self.filename_catalog = filename_catalog
        return

    def rsync_catalog(self):
        '''Use rsync to have the catalog in Leiden.
        '''
        try:
            subprocess.call(
              'rsync %s irisarri@schipbeek.strw.leidenuniv.nl:/data1/irisarri/CombinedCats/catalogs/kids_dr1/' %\
              self.filename_catalog, shell=True
            )
        except:
            traceback.print_exc()

        # Rsync also the log file.
        try:
            subprocess.call(
              'rsync %s irisarri@schipbeek.strw.leidenuniv.nl:/data1/irisarri/CombinedCats/catalogs/kids_dr1/' %\
              self.filename_catalog.replace('.pkl', '.log'), shell=True
            )
        except:
            traceback.print_exc()

    def set_indices(self):
        '''Get index of each of the attributes to be used later when looping
        over the sources in the AssociateList. This in order to make sure we
        are writing the columns in the correct order in the catalog, so run
        preferably after calling the AssociateList.associates method get_data.
        '''
        return [self.attr_list.index(i) for i in self.attr_list]

    def append_to(self, r_key_ctr):
        '''First get the indices of the attributes to keep from the instance
        variable attr_list. Then split the variable holding the string
        formatting for sources with data. Use that and the band data in
        r_key_ctr to build the string row and return it.
        '''
        indices = [self.attr_list.index(i) for i in self.attr_list]
        line_source = self.line_source.split()

        row = ''
        for i in range(len(line_source)):
            row += line_source[i] % r_key_ctr[indices[i]] + ' '

        row += '\t'

        return row

    def make_catalog_pkl(self):
        '''Principal method of the class.
        '''
        # Lines at the top of the file with some parameters and column names.
        # Is filled-in with info from the tile after querying for the
        # AssociateList and setting a main SourceList.
        cat_header = '''# KIDS tile: %s
# SExtractor parameters:
# DETECT_THRESH: %f
# Extraction date: %s
# columns:
# 0: iSLID
# 1: iSID
# 2: iRA
# 3: iDEC
# 4: iMAG_AUTO
# 5: iMAGERR_AUTO
# 6: iKRON_RADIUS
# 7: iMAG_ISO
# 8: iMAGERR_ISO
# 9: iMAG_ISOCOR
# 10: iMAGERR_ISOCOR
# 11: iFLUX_APER_10
# 12: iFLUXERR_APER_10
# 13: iFLUX_APER_20
# 14: iFLUXERR_APER_20
# 15: iFLUX_APER_40
# 16: iFLUXERR_APER_40
# 17: iFLUX_APER_100
# 18: iFLUXERR_APER_100
# 19: iFlag
# 20: iIMAFLAGS_ISO
# 21: iNIMAFLAGS_ISO
# 22: iFLUX_RADIUS
# 23: iFWHM_IMAGE
# 24: iELLIPTICITY
# 25: iPOSANG
# 26: i2DPHOT
# 27: iCLASS_STAR
# 28: rSLID
# 29: rSID
# 30: rRA
# 31: rDEC
# 32: rMAG_AUTO
# 33: rMAGERR_AUTO
# 34: rKRON_RADIUS
# 35: rMAG_ISO
# 36: rMAGERR_ISO
# 37: rMAG_ISOCOR
# 38: rMAGERR_ISOCOR
# 39: rFLUX_APER_10
# 40: rFLUXERR_APER_10
# 41: rFLUX_APER_20
# 42: rFLUXERR_APER_20
# 43: rFLUX_APER_40
# 44: rFLUXERR_APER_40
# 45: rFLUX_APER_100
# 46: rFLUXERR_APER_100
# 47: rFlag
# 48: rIMAFLAGS_ISO
# 49: rNIMAFLAGS_ISO
# 50: rFLUX_RADIUS
# 51: rFWHM_IMAGE
# 52: rELLIPTICITY
# 53: rPOSANG
# 54: r2DPHOT
# 55: rCLASS_STAR
# 56: gSLID
# 57: gSID
# 58: gRA
# 59: gDEC
# 60: gMAG_AUTO
# 61: gMAGERR_AUTO
# 62: gKRON_RADIUS
# 63: gMAG_ISO
# 64: gMAGERR_ISO
# 65: gMAG_ISOCOR
# 66: gMAGERR_ISOCOR
# 67: gFLUX_APER_10
# 68: gFLUXERR_APER_10
# 69: gFLUX_APER_20
# 70: gFLUXERR_APER_20
# 71: gFLUX_APER_40
# 72: gFLUXERR_APER_40
# 73: gFLUX_APER_100
# 74: gFLUXERR_APER_100
# 75: gFlag
# 76: gIMAFLAGS_ISO
# 77: gNIMAFLAGS_ISO
# 78: gFLUX_RADIUS
# 79: gFWHM_IMAGE
# 80: gELLIPTICITY
# 81: gPOSANG
# 82: g2DPHOT
# 83: gCLASS_STAR
# 84: uSLID
# 85: uSID
# 86: uRA
# 87: uDEC
# 88: uMAG_AUTO
# 89: uMAGERR_AUTO
# 90: uKRON_RADIUS
# 91: uMAG_ISO
# 92: uMAGERR_ISO
# 93: uMAG_ISOCOR
# 94: uMAGERR_ISOCOR
# 95: uFLUX_APER_10
# 96: uFLUXERR_APER_10
# 97: uFLUX_APER_20
# 98: uFLUXERR_APER_20
# 99: uFLUX_APER_40
# 100: uFLUXERR_APER_40
# 101: uFLUX_APER_100
# 102: uFLUXERR_APER_100
# 103: uFlag
# 104: uIMAFLAGS_ISO
# 105: uNIMAFLAGS_ISO
# 106: uFLUX_RADIUS
# 107: uFWHM_IMAGE
# 108: uELLIPTICITY
# 109: uPOSANG
# 110: u2DPHOT
# 111: uCLASS_STAR
'''

        # Holds info that is dumped at a later point.
        line = []

        start_time = datetime.now()

        if not self.alid:
            als = AssociateList.ALID == self.get_alid(self.filename_log_al)
        else:
            als = AssociateList.ALID == self.alid

        if len(als) == 1:
            al = als[0]
        else:
            al_dates = [i.creation_date for i in als]
            al_dates.sort()
            al = [
              i for i in als if i.creation_date == al_dates[-1]
            ][0]

        al_object = al.OBJECT.split(';')[0]

        # Variable of catalog's filename, so that it can be written to.
        # Has AssociateList's parameters for reference.
        filename_catalog = '%s-%sarcsec-SEflag%s.pkl' % (
          al_object,
          al.process_params.SEARCH_DISTANCE,
          al.process_params.SEXTRACTOR_FLAG_MASK
        )
        subprocess.call('rm -rf %s' % filename_catalog, shell=True)
        # Function to set as instance variable.
        self.set_filename_catalog(filename_catalog)

        filename_log = filename_catalog.replace('.pkl', '.log')
        subprocess.call('rm -rf %s' % filename_log, shell=True)

        # Try to set a main SourceList, and point variables to SLID of each of
        # the 4 bands.
        # Except exits since we require 4 SourceLists.
        try:
            sl_main = al.sourcelists[0]

            i_slid = [
              i.SLID for i in al.sourcelists if i.filter.name == 'OCAM_i_SDSS'
            ][0]

            r_slid = [
              i.SLID for i in al.sourcelists if i.filter.name == 'OCAM_r_SDSS'
            ][0]

            g_slid = [
              i.SLID for i in al.sourcelists if i.filter.name == 'OCAM_g_SDSS'
            ][0]

            u_slid = [
              i.SLID for i in al.sourcelists if i.filter.name == 'OCAM_u_SDSS'
            ][0]
        except:
            traceback.print_exc()
            exit()

        # Append top lines of catalog containing some parameters and column
        # names.
        line.append(
          cat_header % (al_object, sl_main.sexconf.DETECT_THRESH,
          str(datetime.now()))
        )

        # Top line of log file.
        with open(filename_log, 'w') as fp:
            fp.write(
              "%s.cat, ALID = %i, i-SLID = %i, r-SLID = %i, g-SLID = %i, u-SLID = %i\n" %\
              (al_object, al.ALID, i_slid, r_slid, g_slid, u_slid)
            )

        # Returns a dictionary of lists. Each of the lists contains list(s)
        # corresponding to the source(s) found in one to a maximum
        # of four filters.
        r = al.associates.get_data(self.attr_list, mask=0, mode='ALL')

        # necessary?
        #self.attr_list = self.set_indices(self.attr_list)

        for key in r.keys():
            ctr = 0
            with open(filename_log, 'a') as fp:
                fp.write(
                  '%i, %i\n' % (key, len(r[key]))
                )

            for slid in i_slid, r_slid, g_slid, u_slid:
                try:
                    if r[key][ctr][0] == slid:
                        line.append(
                          self.append_to(r[key][ctr])
                        )
                        ctr += 1
                    else:
                        line.append(self.line_nines)

                except:
                    line.append(self.line_nines)

            line.append('\n')

        #with open(filename_catalog, 'wb') as fp:
        pickle.dump(line, open(filename_catalog, 'wb'))

        print "%s took" % al_object, (datetime.now()-start_time)

if __name__ == '__main__':

    try:
        #filename_log_al = sys.argv[1].split(',')[0] #to do for many AssociateLists
        filename_log_al = sys.argv[1]
    except:
        traceback.print_exc()

        print __doc__

        exit()

    catalog = Multicat(
      filename_log_al,
      alid=None
    )
    catalog.make_catalog_pkl()

    catalog.rsync_catalog()
