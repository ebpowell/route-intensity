#!/usr/bin/python

import os, sys, thread
import subprocess
lstFileNames = []
#Simple script to automatically load e00 files into the POstGIS database
def getFileNames(strLayerString, strPath):
	lstFiles = os.listdir(strPath)
	for strFile in lstFiles:
		if strFile.count(strLayerString) > 0:
			lstFileNames.append(strFile)
	return lstFileNames

if __name__ == '__main__':
	#Path = sys.argv[1]
	Path = "/media/sda4/Versioned/grad_school/Thesis/SCDNR_Data"
	lstLayers = [['soils', 'sls']]
#	lstLayers = [['hydrography', 'hyd'],['railroads', 'rrs'],['pipeline', 'ptl'],['wetlands', 'nwi'],['soils', 'sls']]

	for lstLayer in lstLayers:
	#next, walk the specified directory tree to find the appropriate files
		lstFileNames = getFileNames(lstLayer[1], Path)
		#once the file list is generate, iterate through the list
		for strFileName in lstFileNames:
			strDBLayer = lstLayers[0][0]
			print strDBLayer
			print Path
			print strFileName
			subprocess.Popen([r"./dataloader.sh", strFileName, Path, strDBLayer]).wait()
		lstFileNames = []

