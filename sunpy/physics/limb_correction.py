from __future__ import division, print_function
import pytest
import numpy as np
import astropy.units as u
import sunpy.map
import sunpy.data.test
import os



@pytest.fixture
def hsi_image():
    testpath = sunpy.data.test.rootdir
    return sunpy.map.Map(os.path.join(testpath, 'hsi_image_20101016_191218.fits'))



def test_limb_correct():
    assert isinstance(limb_correct(hsi_image()), type(hsi_image()))



def limb_correct(amap):
    return amap
