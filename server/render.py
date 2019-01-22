"""
Copyright (C) 2016 The HDF Group
Copyright (C) 2015 John Evans

This example code illustrates how to access and visualize a MERRA-2 L3 HDF5
file in Python.

If you have any questions, suggestions, or comments on this example, please use
the HDF-EOS Forum (http://hdfeos.org/forums).  If you would like to see an
example of any other NASA HDF/HDF-EOS data product that is not listed in the
HDF-EOS Comprehensive Examples page (http://hdfeos.org/zoo), feel free to
contact us at eoshelp@hdfgroup.org or post it at the HDF-EOS Forum
(http://hdfeos.org/forums).

Usage:  save this script and run

    python MERRA2_400.tavg1_2d_slv_Nx.20140101.nc4.py

The HDF file must either be in your current working directory or in a
directory specified by the environment variable HDFEOS_ZOO_DIR.

Tested under: Anaconda 2.2.0 python 2.7.10
Last updated: 2016-04-21

"""

import os

import h5py
import matplotlib as mpl
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np

def render_hdf5_dataset(hdf5Path, pathInHdf5):

    with h5py.File(hdf5Path, mode='r') as f:

        data = f[pathInHdf5][0,:,:]
        units = f[pathInHdf5].attrs['units']
        long_name = f[pathInHdf5].attrs['long_name']
        _FillValue = f[pathInHdf5].attrs['_FillValue']
        data[data == _FillValue] = np.nan
        data = np.ma.masked_where(np.isnan(data), data)

        # Get the geolocation data.
        latitude = f['/lat'][:]
        longitude = f['/lon'][:]

        # Get the time data.
        time = f['/time'][:]
        time_units = f['/time'].attrs['units']
        time_long_name = f['/time'].attrs['long_name']

    m = Basemap(projection='cyl', resolution='l',
                llcrnrlat=-90, urcrnrlat=90,
                llcrnrlon=-180, urcrnrlon=180)
    m.drawcoastlines(linewidth=0.5)
    m.drawparallels(np.arange(-90, 91, 45))
    m.drawmeridians(np.arange(-180, 180, 45), labels=[True,False,False,True])
    m.pcolormesh(longitude, latitude, data, latlon=True)
    cb = m.colorbar()
    cb.set_label(units)


    basename = os.path.basename(hdf5Path)
    tstr = time_long_name+' = '+str(time[0])+' '+time_units
    plt.title('{0}\n{1}\n{2}'.format(basename, long_name, tstr))
    fig = plt.gcf()
    return fig
