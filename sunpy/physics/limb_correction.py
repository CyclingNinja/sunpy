from __future__ import division, print_function
#import pytest
import numpy as np
import astropy.units as u
import sunpy.map
import sunpy.data.test
import os



#@pytest.fixture
def hsi_image():
    testpath = sunpy.data.test.rootdir
    test_file = sunpy.map.Map( os.path.join(testpath, 'HinodeSOT.fits'))
    return test_file.Waves[4]



def test_limb_correct():
    assert isinstance(limb_correct(hsi_image()), type(hsi_image()))



def limb_correct(amap):
    """
    This function takes in a map cube, in the range 400 - 1600 angstroms.

    """
    return amap
