from __future__ import division, print_function
import pytest
import numpy as np
import astropy.units as u
import sunpy.map




@pytest.fixture
def hsi_image():
    testpath = sunpy.data.test.rootdit
    return sunpy.map.Map(os.path.join(testpath, 'hsi_image_20101016_191218.fits'))

def test_limb_correct():
    assert isinstace(limb_correct(hsi_image), sunpy.map)



def limb_correct():
