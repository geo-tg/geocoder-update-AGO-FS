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

   Python 2.7
   ArcREST 3.0.1
'''
import arcpy
import logging
from os import path

import arcrest
from arcresthelper import featureservicetools
from arcresthelper import common
import json


arcpy.env.overwriteOutput = True

def geocodeCSV(csv, loc, fgdb):

    pub_geocode_add = fgdb + r'\AnimalControl_geocoded'

    # convert csv to table for field calculation
    logging.info('Converting table...')
    add_table = arcpy.TableToTable_conversion(csv, 'in_memory', 'address_table')

    # remove records that are already geocoded
    pub_incidentid = list(set([x[0] for x in arcpy.da.SearchCursor(pub_geocode_add, ['Incidentid'])]))
    new_incidentid = list(set([x[0] for x in arcpy.da.SearchCursor(add_table, ['Incidentid'])]))

    del_ids = [n for n in new_incidentid if n in pub_incidentid]

    q = '"Incidentid" IN ({})'.format(str(del_ids).replace('[','').replace(']',''))
    add_tab_view = arcpy.MakeTableView_management(add_table, 'add_tab_view')
    del_rows = arcpy.SelectLayerByAttribute_management(add_tab_view, 'NEW_SELECTION', q)
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
        field_match = "'Street or Intersection' full_add_temp VISIBLE NONE;" \
                    + "'City or Placename' Locationcity VISIBLE NONE;" \
                    + "'ZIP Code' Locationpostal VISIBLE NONE"

        logging.info('Geocoding...')
        geocode_add = arcpy.GeocodeAddresses_geocoding(in_rows, loc, field_match, fgdb + r'\GeocodedAddresses')

        # clean up
        logging.info('Clean up fields...')
        orig_flds = [str(m.name) for m in arcpy.ListFields(in_rows)]
        geocode_flds = arcpy.ListFields(geocode_add)

        # Remove 'Status' if they want to keep it
        del_flds = ['full_add_temp']

        for f in geocode_flds:
            name = str(f.name)
            if name not in orig_flds and name != 'Status_1':
                if not f.required:
                    del_flds.append(name)

        arcpy.DeleteField_management(geocode_add, del_flds)

        # Geocode field 'Status' is created and required, rename orig 'Status' to 'AC_Status'
        arcpy.AlterField_management(geocode_add, 'Status_1', 'AC_Status')

        logging.info('Updating Animal Control feature with new incidents...')
        arcpy.Append_management(geocode_add, pub_geocode_add, 'NO_TEST')

        logging.info('Clean up, aisle 9')
        arcpy.Delete_management('in_memory')
        del(add_table, pub_incidentid, new_incidentid, del_ids, q, 
            add_tab_view, del_rows, in_rows, add_num, st_dir, st_name, 
            st_type, exp, field_match, orig_flds, geocode_flds)

        logging.info('Sweet success! Ready to publish...')
        return(1, geocode_add, pub_geocode_add)

    else:

        logging.info('No new cases to geocode...')
        return(0, '', '')

def updateService(security, un, pw, url, itemid, itemname, uploads, local_fc):

    logging.info('Generating token...')
    sh = arcrest.AGOLTokenSecurityHandler(un, pw, url)
    token_url = sh.token_url

    logging.info('Loading security handler...')
    securityinfo = {}
    securityinfo['security_type'] = security
    securityinfo['username'] = un
    securityinfo['password'] = pw
    securityinfo['org_url'] = url
    securityinfo['token_url'] = token_url

    logging.info('Accessing content')
    fst = featureservicetools.featureservicetools(securityinfo)
    if fst.valid == False:
        logging.info(fst.message)

    else:
        logging.info('Getting Animal Control feature service...')
        fs = fst.GetFeatureService(itemId=itemid,returnURLOnly=False)

        if not fs is None:
            logging.info('Getting layer...')
            fl = fst.GetLayerFromFeatureService(fs=fs,layerName=itemname,returnURLOnly=False)

            # check that no manual updates have happend to service between csv update and script run
            q = fl.query()
            service_count = len(q.features)
            local_count = int(arcpy.GetCount_management(local_fc)[0])
            if service_count < local_count:

                if not fl is None:
                    logging.info('Adding new rows...')
                    results = fl.addFeatures(fc=uploads)
                    logging.info(json.dumps(results))

                    logging.info('Clean up temp fgdb...')
                    arcpy.Delete_management(uploads)

                    logging.info('New rows appended, fgdb cleaned!')
                    logging.info('Success!!')

                else:
                    logging.info('Layer {} was not found, please check your credentials and layer name'.format(itemname))
            
            elif service_count == local_count:
                logging.info('Feature service already has same record count as local AnimalControl_geocoded feature class... investigate!')

            else:
                logging.info('Feature service has MORE records than local AnimalControl_geocoded feature class... PROBLEM!')

        else:
            logging.info('Feature Service with id {} was not found'.format(itemid))
        

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
    locator = workspace + r'\locator\FtPierceLocator_AP_arcmap'
    out_fgdb = workspace + r'\data\GeocodedAddresses.gdb'

    # inputs for update service
    security_type = 'Portal'
    username = ''
    password = ''
    url = 'https://arcgis.com/'
    # AnimalControl_CodeEnforcement
    itemid = ''
    layer = ''

    try: 
        logging.info('\n\n-----------------------------------------------------------------')
        logging.info('Starting run...')
        publish, upload, service_source = geocodeCSV(csv, locator, out_fgdb)
        if publish == 1:
            logging.info('Beginning to publish service...')
            updateService(security_type, username, password, url, itemid, layer, upload, service_source)

    except Exception as e:
        logging.error("EXCEPTION OCCURRED", exc_info=True)
        logging.error(e)
        
