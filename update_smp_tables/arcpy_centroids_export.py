# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# arcpy_centroids_export.py
# Created on: 2018-11-23 13:52:43.00000
#   (generated by ArcGIS/ModelBuilder)
# Description: 
# ---------------------------------------------------------------------------

# Set the necessary product code
# import arcinfo


# Import arcpy module
import arcpy, datetime, os, re

#Since it's not going to be run in interactive mode, we need to load PYTHONSTARTUP 
if os.path.isfile(os.environ['PYTHONSTARTUP']):
   execfile(os.environ['PYTHONSTARTUP'])
else:
   sys.exit("You don't have a .pythonrc file in your PYTHONSTARTUP environment variable.")

print(arcpy.CheckOutExtension("Spatial"))

# Local variables:
current_date = datetime.datetime.now()

#We must round the datestring because it's possible for the arcpy script to take more than 60 seconds to execute
#In which case getting the current datetime at a minute-scale resolution won't get the same number as it did in that script
def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time lapse in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

rounddate = roundTime(current_date, roundTo = 60 * 15) #Round to the nearest 15 minutes
datestring = rounddate.strftime("%Y%m%dT%H%M")

destinationfolder = MAINTENANCEFOLDER + "\\update_smp_tables\\centroids_folder"


print "Datestring " + datestring

Wetlands = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiWetland"
Dissolved_Wetlands = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\wetlands_dissolve_" + datestring

Trenches = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiTrench"
Dissolved_Trenches = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\trenches_dissolve_" + datestring

Tree_Trenches = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiTreeTrench"
Dissolved_Tree_Trenches = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\treetrenches_dissolve_" + datestring

Swales = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiSwale"
Dissolved_Swales = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\swales_dissolve_" + datestring

Rain_Gardens = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiRainGarden"
Dissolved_Rain_Gardens = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\raingardens_dissolve_" + datestring

Planters = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiPlanter"
Dissolved_Planters = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\planters_dissolve_" + datestring

Permeable_Pavement = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiPermeablePavement"
Dissolved_Pavements = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\pavements_dissolve_" + datestring

Green_Roofs = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiGreenRoof"
Dissolved_Green_Roofs = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\greenroofs_dissolve_" + datestring

DrainageWells = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiDrainageWell"
Dissolved_DrainageWells = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\drainagewells_dissolve_" + datestring

Cisterns = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiCistern"
Dissolved_Cisterns = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\cisterns_dissolve_" + datestring

Bumpouts = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiBumpout"
Dissolved_Bumpouts = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\bumpouts_dissolve_" + datestring

Blue_Roofs = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiBlueRoof"
Dissolved_Blue_Roofs = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\blueroofs_dissolve_" + datestring

Basins = "Database Connections\\DATACONV.sde\\DataConv.GISAD.Green_Stormwater_Infrastructure_Feature\\DataConv.GISAD.gswiBasin"
Dissolved_Basins = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\basins_dissolve_" + datestring

Merged_SMPs = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\dissolve_merge" + datestring

raster_dem = "Database Connections\\RASTER.sde\\RASTER.GISAD.DEM"

centroids = os.path.expanduser("~") +"\\Documents\\ArcGIS\\Default.gdb\\centroids_" + datestring

finalshapefile = destinationfolder + "\\centroids_dem_" + datestring + ".shp"

#If the resultant file exists already, purge the entire destination folder
if os.path.isfile(finalshapefile):
	print "Deleting duplicate centroid files"
	for file in os.listdir(destinationfolder):
		the_file = os.path.join(destinationfolder, file)
		print "Deleting " + the_file
		os.unlink(the_file)

print "Dissolving Wetlands"

# Process: Dissolve Wetlands
arcpy.Dissolve_management(Wetlands, Dissolved_Wetlands, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Trenches"

# Process: Dissolve Trenches
arcpy.Dissolve_management(Trenches, Dissolved_Trenches, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Tree Trenches"

# Process: Dissolve Tree Trenches
arcpy.Dissolve_management(Tree_Trenches, Dissolved_Tree_Trenches, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Swales"

# Process: Dissolve Swales
arcpy.Dissolve_management(Swales, Dissolved_Swales, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Rain Gardens"

# Process: Dissolve Rain Gardens
arcpy.Dissolve_management(Rain_Gardens, Dissolved_Rain_Gardens, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Planters"

# Process: Dissolve Planters
arcpy.Dissolve_management(Planters, Dissolved_Planters, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Pavement"

# Process: Dissolve Pavement
arcpy.Dissolve_management(Permeable_Pavement, Dissolved_Pavements, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Green Roofs"

# Process: Dissolve Green Roofs
arcpy.Dissolve_management(Green_Roofs, Dissolved_Green_Roofs, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Drainage Wells"

# Process: Dissolve DrainageWells
arcpy.Dissolve_management(DrainageWells, Dissolved_DrainageWells, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Cisterns"

# Process: Dissolve Cisterns
arcpy.Dissolve_management(Cisterns, Dissolved_Cisterns, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Bumpouts"

# Process: Dissolve Bumpouts
arcpy.Dissolve_management(Bumpouts, Dissolved_Bumpouts, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Blue Roofs"

# Process: Dissolve Blue Roofs
arcpy.Dissolve_management(Blue_Roofs, Dissolved_Blue_Roofs, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")

print "Dissolving Basins"

# Process: Dissolve Basins
arcpy.Dissolve_management(Basins, Dissolved_Basins, "SMP_ID", "", "MULTI_PART", "Dissolve_LINES")



# Compose merge input string
merge_input_string = Dissolved_Wetlands + ";" + Dissolved_Trenches + ";" + Dissolved_Tree_Trenches + ";" + Dissolved_Swales + ";" + Dissolved_Rain_Gardens + ";" + Dissolved_Planters + ";" + Dissolved_Pavements + ";" + Dissolved_Green_Roofs + ";" + Dissolved_DrainageWells + ";" + Dissolved_Cisterns + ";" + Dissolved_Bumpouts + ";" + Dissolved_Blue_Roofs + ";" + Dissolved_Basins

# Compose merge output string
merge_output_string = "SMP_ID \"SMP_ID\" true true false 50 Text 0 0 ,First,#," + Dissolved_Wetlands + ",SMP_ID,-1,-1," + Dissolved_Trenches + ",SMP_ID,-1,-1," + Dissolved_Tree_Trenches + ",SMP_ID,-1,-1," + Dissolved_Swales + ",SMP_ID,-1,-1," + Dissolved_Rain_Gardens + ",SMP_ID,-1,-1," + Dissolved_Planters + ",SMP_ID,-1,-1," + Dissolved_Pavements + ",SMP_ID,-1,-1," + Dissolved_Green_Roofs + ",SMP_ID,-1,-1," + Dissolved_DrainageWells + ",SMP_ID,-1,-1," + Dissolved_Cisterns + ",SMP_ID,-1,-1," + Dissolved_Bumpouts + ",SMP_ID,-1,-1," + Dissolved_Blue_Roofs + ",SMP_ID,-1,-1," + Dissolved_Basins + ",SMP_ID,-1,-1" #No terminal comma on the last one

print "Merging dissolved polygons"

# Process: Merge
arcpy.Merge_management(merge_input_string, Merged_SMPs, merge_output_string)

print "Converting dissolved polygons to centroids"

# Process: Feature To Point
arcpy.FeatureToPoint_management(Merged_SMPs, centroids, "CENTROID")

print "Intersecting the centroids with the DEM"

# Process: Intersect Centroids with DEM to get point elevations
arcpy.gp.ExtractValuesToPoints_sa(centroids, raster_dem, finalshapefile, "NONE", "VALUE_ONLY")


print "Deleting intermediate feature classes"

# Delete intermediate feature classes
arcpy.Delete_management(Dissolved_Wetlands)
arcpy.Delete_management(Dissolved_Trenches)
arcpy.Delete_management(Dissolved_Tree_Trenches)
arcpy.Delete_management(Dissolved_Swales)
arcpy.Delete_management(Dissolved_Rain_Gardens)
arcpy.Delete_management(Dissolved_Planters)
arcpy.Delete_management(Dissolved_Pavements)
arcpy.Delete_management(Dissolved_Green_Roofs)
arcpy.Delete_management(Dissolved_DrainageWells)
arcpy.Delete_management(Dissolved_Cisterns)
arcpy.Delete_management(Dissolved_Bumpouts)
arcpy.Delete_management(Dissolved_Blue_Roofs)
arcpy.Delete_management(Dissolved_Basins)
arcpy.Delete_management(Merged_SMPs)
arcpy.Delete_management(centroids)


