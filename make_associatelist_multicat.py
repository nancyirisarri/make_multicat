'''
USAGE:

> awe make_associatelist_multicat.py DATE-KIDS_RA_DEC-make_sourcelist_high_s_n.log 2 1.0 3

with arguments:
1) Locally stored log file of the script make_sourcelist_high_s_n.py, which
contains the SLID's of the Astro-WISE SourceList's to include in the
AssociateList.
2) AssociateList type where 1 is for chain, 2 for master, and 3 for matched
type. Script has only been tested to work with type 2!
3) Matching distance in arcsec.
4) SExtractor flag mask of sources to keep.
If a log file is not available, then make a text file with 4 newline-separated
lines, where each is for each of the bands to associate, and contains 'SLID'
and this number, e.g. text file contents:
SLID 16390931
SLID 16406191
SLID 16406201
SLID 16406211
'''

from astro.main.SourceList import SourceList
from astro.main.AssociateList import AssociateList

import sys
import subprocess
from datetime import date
import traceback

class AssociateListMulticat:
    '''Used to facilitate making 4-band AssociateLists, which are input for
    KiDS 4-band catalogs (i.e. multicat).
    '''

    def __init__(self, filename_log_sl, al_type, matching_distance, flag_mask):
        '''Necessary variables:

        filename_log_sl: log file of the script make_sourcelist_high_s_n.py,
        which contains the SLID's of the SL's to include in the AssociateList.

        al_type: AssociateList type where 1 is for chain, 2 for master, and 3
        for matched type.

        matching_distance: radius of search for associates [arcsec].

        flag_mask: in default mode all objects in the to-be associated
        SourceLists which have SExtractor Flag > 0 are filtered out. To keep
        objects which have Flag > 0 set this variable.

        main_filter: filter of 1st SourceList that will be input to
        AssociateList.

        TODO: allow user to set order of filters in AssociateList.
        '''
        self.filename_log_sl = filename_log_sl
        self.al_type = al_type
        self.matching_distance = matching_distance
        self.flag_mask = flag_mask
        self.main_filter = 'OCAM_i_SDSS'
        self.kids_object = filename_log_sl[
          filename_log_sl.find('KI'):filename_log_sl.find('-make')
        ]
        self.filename_log = '%s-%s-make_associatelist_multicat.log' % (
          date.today().isoformat().replace('-', ''),
          filename_log_sl[filename_log_sl.find('KI'):filename_log_sl.find('-make')]
        )

    def do(self, sl_1, sl_2):
        '''Make AssociateLists, of type self.al_type, including sources with
        SExtractor flag self.flag_mask, and matching distance of
        self.matching_distance.

        Script takes i-band SourceList as the 1st to be input of AssociateList.
        '''
        al = AssociateList()
        al.input_lists.append(sl_1)
        al.input_lists.append(sl_2)
        al.associatelisttype = self.al_type
        al.set_search_distance(self.matching_distance)
        al.process_params.SEXTRACTOR_FLAG_MASK = self.flag_mask

        # Name the first AssociateList with the OBJECT and filter i-r.
        # Subsequent names just append the current filter being associated.
        try:
            if sl_1.filter.name == self.main_filter:
                al.name = '%s-test-type%s-i-r' % (self.kids_object, self.al_type)
        except:
            al.name = sl_1.name + '-%s' % sl_2.filter.name[5]

        al.make()
        al.commit()

        message = 'Commited AssociateList name %s, ALID %i\n' % (al.name, al.ALID)
        print message
        with open(self.filename_log, 'a') as fp:
            fp.write(message)

        return al

    def do_all(self, slids):
        '''Query AWE for the SourceLists, and assign each of the four needed to
        a variable indicating its filter. Subsequently make AssociateLists
        in the order i-r, i-r-g, and i-r-g-u.

        TODO: allow user to set order of filters in AssociateList.
        '''
        for slid in slids:
            sl = (SourceList.SLID == slid)[0]

            if sl.filter.name == self.main_filter:
                sl_i = sl

            elif sl.filter.name == 'OCAM_r_SDSS':
                sl_r = sl

            elif sl.filter.name == 'OCAM_g_SDSS':
                sl_g = sl

            elif sl.filter.name == 'OCAM_u_SDSS':
                sl_u = sl

        try:
            message = 'Making AssociateList i-r'
            print message
            al_i_r = self.do(sl_i, sl_r)

        except:
            traceback.print_exc()

            with open(self.filename_log, 'a') as fp:
                traceback.print_exc(file=fp)
            exit()

        try:
            message = 'Making AssociateList i-r-g'
            print message
            al_i_r_g = self.do(al_i_r, sl_g)

        except:
            traceback.print_exc()

            with open(self.filename_log, 'a') as fp:
                traceback.print_exc(file=fp)
            exit()

        try:
            message = 'Making AssociateList i-r-g-u'
            print message
            al_i_r_g_u = self.do(al_i_r_g, sl_u)
            return al_i_r_g_u.ALID

        except:
            traceback.print_exc()

            with open(self.filename_log, 'a') as fp:
                traceback.print_exc(file=fp)
            exit()

        return

    def get_slids(self, filename_log_sl):
        '''Read the log file containing SLID's and keep them as integers.
        '''

        with open(filename_log_sl, 'r') as fp:
            lines = fp.read()

        lines = [
          i for i in lines.split('\n') if
          'SLID' in i
        ]

        return [int(i.split()[-1]) for i in lines]

    def make_associatelist_multicat(self):
        '''Principal method of the class.
        '''
        # Remove older log files before starting.
        subprocess.call('rm -rf %s' % self.filename_log, shell=True)

        slids = self.get_slids(self.filename_log_sl)

        alid = self.do_all(slids)

        message = '\n4-band ALID %s\n' % str(alid)
        print message
        with open(self.filename_log, 'a') as fp:
            fp.write(message)

        return alid

if __name__ == '__main__':
    try:
        filename_log_sl = sys.argv[1]
        al_type = int(sys.argv[2]) #master is 2
        matching_distance = float(sys.argv[3]) #in arcseconds, e.g. 1.0
        flag_mask = int(sys.argv[4]) #see SExtractor manual, e.g. 3
    except:
        traceback.print_exc()

        print __doc__

        exit()

    alid = AssociateListMulticat(filename_log_sl, al_type, matching_distance, flag_mask)
    alid.make_associatelist_multicat()
