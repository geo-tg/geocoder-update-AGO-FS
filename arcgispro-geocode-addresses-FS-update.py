# -*- coding: utf-8 -*-
''' Fort Pierce Geocode Addresses
Uses a custom locator to geocode
address from a csv file

Locator: 
    Style: US Address—Single House Subaddress
    Reference data: Fort Pierce Address Point // Primary Table
    Field map:  Point Address ID // OBJECTID
                House Number // HouseNumber
                Prefix Direction // Prefix
                Street Name // StreetName
                Suffix Type // Suffix
                Suffix Direction // Direction
                SubAddr Unit // Unit
                Sity or Place // CityState
                ZIP Code // ZipCode

Updates published service, AnimalControl_CodeEnforcement,
in the Fort Pierce AGO

   Python 3.x
'''

import arcpy
import arcgis
from arcgis.gis import GIS
import logging
import json
import os
from os import path
import zipfile


arcpy.env.overwriteOutput = True

def geocodeCSV(csv, loc, out_ws):

    pub_geocode_add = out_ws + r'\published\GeocodedAddresses.gdb\AnimalControl_geocoded'

    # convert csv to table for field calculation
    logging.info('Converting table...')
    add_table = arcpy.TableToTable_conversion(csv, 'in_memory', 'address_table')

    # remove records that are already geocoded
    pub_incidentid = list(set([x[0] for x in arcpy.da.SearchCursor(pub_geocode_add, ['Incidentid'])]))
    new_incidentid = list(set([x[0] for x in arcpy.da.SearchCursor(add_table, ['Incidentid'])]))

    del_ids = [n for n in new_incidentid if n in pub_incidentid]

    q = '"Incidentid" IN ({})'.format(str(del_ids).replace('[','').replace(']',''))
    del_rows = arcpy.SelectLayerByAttribute_management(add_table, 'NEW_SELECTION', q)
    in_rows = arcpy.DeleteRows_management(del_rows)

    if arcpy.GetCount_management(in_rows)[0] != '0':

        arcpy.AddField_management(in_rows, 'full_add_temp', 'TEXT')

        add_num = 'Locationstreetnumber'
        st_dir = 'Locationstreetdir'
        st_name = 'Locationstreetname'
        st_type = 'Locationstreettype'

        logging.info('Calculating field...')
        exp = "' '.join([str(!{0}!),str(!{1}!),str(!{2}!), str(!{3}!)]).replace('None', '').replace('  ', ' ').replace('  ', ' ').strip()".format(add_num, st_dir, st_name, st_type)
        arcpy.CalculateField_management(in_rows, 'full_add_temp', exp, 'PYTHON')

        # geocode
        field_match = "'Address or Place' full_add_temp VISIBLE NONE;" \
                        + "'City' Locationcity VISIBLE NONE;" \
                        + "'County' Locationcounty VISIBLE NONE;" \
                        + "'State' Locationstate VISIBLE NONE;" \
                        + "'ZIP' Locationpostal VISIBLE NONE;" \
                        + "'Country' Locationctry VISIBLE NONE"

        logging.info('Geocoding...')
        geocode_add = arcpy.GeocodeAddresses_geocoding(in_rows, loc, field_match, out_ws + r'\to_append\GeocodedAddresses_append.gdb\GeocodedAddresses', output_fields='LOCATION_ONLY')
      
        # clean up
        arcpy.DeleteField_management(geocode_add, 'full_add_temp')

        logging.info('Updating Animal Control feature with new incidents...')
        arcpy.Append_management(geocode_add, pub_geocode_add, 'NO_TEST')

        logging.info('Clean up, aisle 9')
        arcpy.Delete_management('in_memory')
        del(add_table, pub_incidentid, new_incidentid, del_ids, q, 
            del_rows, in_rows, add_num, st_dir, st_name, 
            st_type, exp, field_match)

        logging.info('Sweet success! Ready to publish...')
        return(1, geocode_add, pub_geocode_add)

    else:

        logging.info('No new cases to geocode...')
        return(0, '', '')

def updateService(un, pw, org_url, fs_url, uploads, local):

    # connect to org/portal
    gis = GIS(org_url, un, pw)
    logging.info('Logged in as {}'.format(un))

    fl = arcgis.features.FeatureLayer(fs_url, gis)

    hosted_count = fl.query(return_count_only=True)
    local_count = int(arcpy.GetCount_management(local)[0])

    if hosted_count < local_count:

        logging.info('New rows found, creating json...')
        json_local = out_data_ws + r'\updates.json'
        json_update = arcpy.FeaturesToJSON_conversion(uploads, json_local)
        fs = arcgis.features.FeatureSet.from_json(open(json_local).read())

        logging.info('Appending new rows...')
        fl.edit_features(adds=fs)
        logging.info('Success!')

        logging.info('Removing json file...')
        os.remove(json_local)

        logging.info('Done!')
    
    elif hosted_count == local_count:
        logging.info('Feature service already has same record count as local AnimalControl_geocoded feature class... investigate!')

    else:
        logging.info('Feature service has MORE records than local AnimalControl_geocoded feature class... PROBLEM!')

    # cleanup
    del(gis, hosted_count, local_count, fl, json_update, fs)
        

if __name__ == "__main__":

    # logging
    logfile = (path.abspath(path.join(path.dirname(__file__), '..', r'logs\geocode-addresses.txt')))
    logging.basicConfig(filename=logfile,
                        level=logging.INFO,
                        format='%(levelname)s: %(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S')

    # inputs for geocoding
    workspace = r'C:\data\gtg-data\projects\fl-fort-pierce\geocode-address-task'
    csv = workspace + r'\orig-data\testForAddresses.csv'
    locator = workspace + r'\locator\FtPierceLocator_AP_PRO'
    out_data_ws = workspace + r'\data'

    # get creds
    text = open(workspace + r'\data\ago-creds.json').read()
    json_array = json.loads(text)
    # inputs for update service
    orgURL = json_array['orgURL']
    # Username of an account in the org/portal that can access and edit all services listed below
    username = json_array['username']
    # Password corresponding to the username provided above
    password = json_array['password']

    # AnimalControl_CodeEnforcement
    featureservice = 'https://services1.arcgis.com/oDRzuf2MGmdEHAbQ/ArcGIS/rest/services/AnimalControl_CodeEnforcement/FeatureServer/0'
    # itemid = 'd596511e92e94a61870265df538664e4'

    try: 
        logging.info('\n\n-----------------------------------------------------------------')
        logging.info('Starting run...')
        publish, upload, local_fc = geocodeCSV(csv, locator, out_data_ws)
        if publish == 1:
            logging.info('Beginning to update service...')
            updateService(username, password, orgURL, featureservice, upload, local_fc)

    except Exception as e:
        logging.error("EXCEPTION OCCURRED", exc_info=True)
        logging.error(e)
