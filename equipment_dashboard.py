import warnings
import re
from datetime import date
from datetime import date, timedelta,datetime
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

current_time=str(datetime.now().month)+'-'+str(datetime.now().day)+'-'\
    +str(datetime.now().year)+'_'+str(datetime.now().hour)+'-'+str(datetime.now().minute)

# File locations
data_location_1 = '//ponchielli/QlikView/Production/Data/Qlik Sense/Locus Container/'
data_location_2 = '//ponchielli/QlikView/Production/Data/Qlik Sense/Machine Learning/'
save_location = '//ponchielli/QlikView/Production/Data/Qlik Sense/Locus Container/'

# Doris MCL
mcl_base_file = data_location_1 + '#1_doris_smcl.csv'
mcl_event_file = data_location_1 + '#3_doris_smcl_all.csv'

# Doris Iceland
ice_base_file = data_location_1 + 'DORIS#ICELAND_2.csv'
ice_event_file = data_location_1 + 'DORIS#ICELAND_1.csv'

# GOS
gos_base_file = data_location_1 + 'GOS#EQMASTP.csv'
gos_event_file = data_location_1 + 'GOS#EQPUNI1P.csv'
gos_workshop_file = data_location_1 + 'EQHIST2P.csv'

# Greencat
gc_base_file = data_location_1 + 'Greencat_Master_Container.csv'
gc_event_file = data_location_1 + 'Activity.csv'
gc_workshop_file = data_location_1 + 'WorkshopEquipment.csv'

# Known problem containers iceland (storage etc)
special_flag_containers = ['TCLU5173774', 'TTNU9791488', 'SANU7631671', 'TCLU6065769', 'BMOU4063082', 'EMCU9342085',
                           'TCNU9572065']

ice_leased_flag_dic = {'C': 'Cabotage', 'F': 'Flexi-Lease', 'L': 'Long Term Lease', 'M': 'Master', 'O': 'Owned',
                       'Q': 'Shippers Owned', 'S': 'Sub Lease', 'X': 'Bremen-Oslo loop'}

gos_event_flag_dic = {'AS': 'Laden: awaiting shipment', 'BK': 'Laden: booked', 'DE': 'Empty: available',
                      'DF': 'Laden: gate in', 'EM': 'Empty: available', 'FS': 'Empty: for sale',
                      'GE': 'Empty: gate out', 'GF': 'Laden: gate out', 'LD': 'Laden: drop off',
                      'OE': 'Empty: gate out', 'OF': 'Laden: gate out', 'RE': 'Empty: damaged', 'RX': 'Laden: gate in',
                      'SH': 'Laden: shipped', 'ST': 'Empty: empty on chassis', 'TR': 'Subleased',
                      'VD': 'Subleased van Dieren'}

gos_leased_flag_dic = {'Leased': 'Shippers Owned'}

try:
    ice_b = pd.read_csv(ice_base_file)
    gos_b = pd.read_csv(gos_base_file)
    svdm_b = pd.read_csv(gc_base_file)[
        ['CONTAINERNO', 'EQUIPMENT_TYPE','TARE_WEIGHT', 'EQUIPMENT_SEQ', 'OPERATOR', 'IN_SERVICE_DATE',
         'OUT_OF_ORDER_DATE', 'OWNER_YN', 'ACTIVE_YN']]

    # Fixing container format
    svdm_b['CONTAINERNO'] = svdm_b['CONTAINERNO'].str[:4] + svdm_b['CONTAINERNO'].str[5:11] + svdm_b['CONTAINERNO'].str[-1]

    # DateTime fix
    ice_b['OFF_HIRE'] = ice_b['OFF_HIRE'].str[:10]
    ice_b['ON_HIRE'] = ice_b['ON_HIRE'].str[:10]
    ice_b['OFF_HIRE'] = pd.to_datetime(ice_b['OFF_HIRE'], format='%d-%m-%Y')
    ice_b['ON_HIRE'] = pd.to_datetime(ice_b['ON_HIRE'], format='%d-%m-%Y')

    gos_b['ON_FLEED_DATE'] = '0' + gos_b['DATE ON FLEET'].astype(str)
    gos_b['OFF_FLEED_DATE'] = '0' + gos_b['DATE OFF FLEET'].astype(str)
    gos_b['ON_FLEED_DATE'] = pd.to_datetime('20' + gos_b['ON_FLEED_DATE'].str[-6:], format='%Y%m%d', errors='coerce')
    gos_b['OFF_FLEED_DATE'] = pd.to_datetime('20' + gos_b['OFF_FLEED_DATE'].str[-6:], format='%Y%m%d', errors='coerce')
    gos_b = gos_b[(gos_b['OPERATIONAL TYPE'].str[0] != 'F') & (~gos_b['OPERATIONAL TYPE'].isin(['MTR', 'TRL', 'CHA']))]
    gos_b = gos_b[(gos_b['ON_FLEED_DATE'].notnull())]

    # Getting right selection
    ice_b = ice_b[(ice_b['OFF_HIRE'].isnull()) | (ice_b['OFF_HIRE'] > pd.to_datetime(date.today()))]
    ice_b = ice_b[ice_b['LEASED_FLAG'].isin(['F', 'L', 'M', 'O', 'S'])]
    gos_b = gos_b[(gos_b['OFF_FLEED_DATE'].isnull()) | (gos_b['OFF_FLEED_DATE'] > pd.to_datetime(date.today()))]
    gos_b = gos_b[gos_b['EQUIPMENT_OWNER'].isin(['Hired', 'Owned'])]
    svdm_b = svdm_b[(svdm_b['ACTIVE_YN'] == 'Y')]

    svdm_b['IN_SERVICE_DATE'] = svdm_b['IN_SERVICE_DATE'].str[:10]
    svdm_b['OUT_OF_ORDER_DATE'] = svdm_b['OUT_OF_ORDER_DATE'].str[:10]
    svdm_b['IN_SERVICE_DATE'] = pd.to_datetime(svdm_b['IN_SERVICE_DATE'], format='%Y-%m-%d')
    svdm_b['OUT_OF_ORDER_DATE'] = pd.to_datetime(svdm_b['OUT_OF_ORDER_DATE'], format='%Y-%m-%d')

    # adding source
    gos_b['ACTIVE_GOS'] = 'Y'
    ice_b['ACTIVE_ICE'] = 'Y'
    svdm_b['ACTIVE_GC'] = 'Y'

    # replacing ownership columns
    ice_b['LEASED_FLAG'] = ice_b['LEASED_FLAG'].replace(ice_leased_flag_dic)
    gos_b['LEASED_FLAG'] = gos_b['EQUIPMENT_OWNER'].replace(gos_leased_flag_dic)
    svdm_b['LEASED_FLAG'] = svdm_b['OWNER_YN'].replace({'Y': 'Owned', 'N': 'Hired'})

    # renaming columns
    gos_b = gos_b.rename(
        columns={'EQUIPMENT NUMBER': 'CONTAINER_NUMBER', 'ON_FLEED_DATE': 'ON_HIRE', 'OFF_FLEED_DATE': 'OFF_HIRE'})
    svdm_b = svdm_b.rename(
        columns={'CONTAINERNO': 'CONTAINER_NUMBER', 'TARE_WEIGHT': 'tare weight', 'OPERATOR': 'operator',
                 'IN_SERVICE_DATE': 'ON_HIRE', 'OUT_OF_ORDER_DATE': 'OFF_HIRE'})

    # grouping them to 1 DF
    df_base = pd.concat([ice_b, gos_b, svdm_b])

    del ice_b
    del gos_b
    del svdm_b

    df_base = df_base[
        ['CONTAINER_NUMBER', 'ISO_CODE', 'LEASED_FLAG', 'ON_HIRE', 'OFF_HIRE', 'ACTIVE_GOS', 'ACTIVE_ICE', 'ACTIVE_GC']]
    df_base['CONTAINER_NUMBER'] = df_base['CONTAINER_NUMBER'].str.upper()
    df_base = df_base[~df_base['CONTAINER_NUMBER'].isnull()]
    container_list = []
    for i in set(df_base['CONTAINER_NUMBER']):
        if re.match(r'^[A-Z]{4}\d{7}$', i) is not None and (i[3] == 'U') & (i[:4] != 'DUMU') & (i[:4] != 'XXXU') & (
                i[:4] != 'PINU'):
            container_list.append(i)

    df_base = df_base[df_base['CONTAINER_NUMBER'].isin(container_list)]
    df_base = df_base.rename(columns={'CONTAINER_NUMBER': 'equipment_id', 'ISO_CODE': 'iso_code', 'ON_HIRE': 'begin_date',
                                      'OFF_HIRE': 'end_date'})

    df_base = df_base[~df_base['equipment_id'].isin(['Special_flag_containers'])][
        ['equipment_id', 'ACTIVE_GOS', 'ACTIVE_ICE', 'ACTIVE_GC']]
    temp_df1 = df_base[df_base['ACTIVE_ICE'] == 'Y'][['equipment_id', 'ACTIVE_ICE']]
    temp_df2 = df_base[df_base['ACTIVE_GOS'] == 'Y'][['equipment_id', 'ACTIVE_GOS']]
    temp_df3 = df_base[df_base['ACTIVE_GC'] == 'Y'][['equipment_id', 'ACTIVE_GC']]

    df_base = pd.merge(temp_df1, temp_df2, on='equipment_id', how='outer')
    df_base = pd.merge(df_base, temp_df3, on='equipment_id', how='outer').fillna('N')

    del temp_df1,temp_df2, temp_df3


    con_list = list(set(df_base['equipment_id']))

    df_bookings = pd.read_csv(data_location_2 + 'MachineLearningBookings.csv')
    mcl_event = pd.read_csv(mcl_event_file)
    ice_event = pd.read_csv(ice_event_file)
    svdm_event = pd.read_csv(gc_event_file)
    svdm_work = pd.read_csv(gc_workshop_file)
    gos_event = pd.read_csv(gos_event_file)
    gos_event = pd.merge(gos_event, df_bookings[['Delivery country', 'Booking reference']], left_on='BOOKING REFERENCE',
                         right_on='Booking reference', how='left')
    del df_bookings

    gos_workshop = pd.read_csv(gos_workshop_file,
                               usecols=['EQUIPMENT NUMBER', 'STAGE','CURRENT LOCATION', 'TO COUNTRY', 'REPORT DATE YYMMDD', 'NARRATIVE'])

    gos_workshop['STAGE'] = np.where(gos_workshop['NARRATIVE'] == 'ReturnLease', 'ReturnLease', gos_workshop['STAGE'])

    gos_workshop = gos_workshop[gos_workshop['REPORT DATE YYMMDD'] < 200000]
    gos_workshop['STATUS_DATE'] = pd.to_datetime(gos_workshop['REPORT DATE YYMMDD'], format='%y%m%d')
    gos_workshop = gos_workshop[gos_workshop['STATUS_DATE'] <= pd.to_datetime(date.today())]
    gos_workshop['DESTINATION'] = gos_workshop['TO COUNTRY']
    gos_workshop['STAGE'].replace(gos_event_flag_dic, inplace=True)
    gos_workshop.rename(columns={'EQUIPMENT NUMBER': 'CONTAINER_NUMBER', 'STAGE': 'DESCRIPTION', 'CURRENT LOCATION' : 'DEPOT'}, inplace=True)
    gos_workshop = gos_workshop[gos_workshop['DESCRIPTION'] != 'CR']


    gos_event['SEQUENCE DATE'] = gos_event['Sequence date'].astype(str)
    gos_event['TIME'] = '00000' + gos_event['Time'].astype(str)
    gos_event['TIME'] = gos_event['TIME'].str[-6:-2]
    gos_event['STATUS_DATE'] = pd.to_datetime(gos_event['SEQUENCE DATE'] + ['-'] + gos_event['TIME'], format='%Y%m%d-%H%M')
    gos_event = gos_event[gos_event['STATUS_DATE'] < (pd.to_datetime(date.today()) + pd.to_timedelta(7, 'd'))]
    gos_event = gos_event[gos_event['Laden Stage'].isin(['BK', 'AS', 'SH'])]
    gos_event['DESTINATION'] = gos_event['Delivery country']
    gos_event = gos_event[['DESTINATION', 'Equip Number', 'Laden Stage', 'Source', 'STATUS_DATE','Location Depot']]
    gos_event['Laden Stage'].replace(gos_event_flag_dic, inplace=True)
    gos_event.rename(columns={'Equip Number': 'CONTAINER_NUMBER', 'Laden Stage': 'DESCRIPTION', 'Source': 'INPUT_TYPE','Location Depot':'DEPOT'},
                     inplace=True)
    gos_event = pd.concat([gos_event, gos_workshop])
    gos_event['CONTAINER_NUMBER'] = gos_event['CONTAINER_NUMBER'].str.upper()
    gos_event = gos_event[gos_event['CONTAINER_NUMBER'].isin(con_list)]
    del gos_workshop

    svdm_work['EQUIPMENT_NO'] = svdm_work['EQUIPMENT_NO'].str[:4] + svdm_work['EQUIPMENT_NO'].str[5:]
    svdm_work = svdm_work[(svdm_work['EQUIPMENT_MASTER_TYPE'] == 'CTR') & (svdm_work['EQUIPMENT_NO'].isin(con_list))]
    svdm_work = svdm_work[['EQUIPMENT_NO', 'EQUIPMENT_SEQ']]

    svdm_df = pd.merge(svdm_event, svdm_work, on='EQUIPMENT_SEQ', how='inner')
    del svdm_work
    del svdm_event

    svdm_df = svdm_df[svdm_df['ACTIVITY_STATUS'] != 'PLANNED']
    svdm_df['SORT_START_DATETIME'] = svdm_df['SORT_START_DATETIME'].str[:10]
    svdm_df['STATUS_DATE'] = pd.to_datetime(svdm_df['SORT_START_DATETIME'], format='%Y-%m-%d')
    svdm_df = svdm_df[
        ['EQUIPMENT_NO', 'ACTIVITY_TYPE', 'STATUS_DATE', 'COUNTRY_CODE', 'CITY_NAME', 'NAME_1', 'ADDRESS_CODE']]
    svdm_df.rename(columns={'EQUIPMENT_NO': 'CONTAINER_NUMBER', 'ACTIVITY_TYPE': 'DESCRIPTION','NAME_1':'DEPOT'}, inplace=True)
    svdm_df['CONTAINER_NUMBER'] = svdm_df['CONTAINER_NUMBER'].str.upper()
    svdm_df = svdm_df[svdm_df['CONTAINER_NUMBER'].isin(con_list)]


    svdm_df['DESTINATION'] = svdm_df['COUNTRY_CODE'] + ' , ' + svdm_df['CITY_NAME']

    mcl_event['STATUS_TIME'] = [('000' + str(i))[-4:-2] + ':' + ('000' + str(i))[-2:] for i in mcl_event['STATUS_TIME']]
    mcl_event['STATUS_DATE'] = mcl_event['STATUS_DATE'].str[:11]
    mcl_event['STATUS_DATE'] = mcl_event['STATUS_DATE'] + mcl_event['STATUS_TIME']
    mcl_event['STATUS_DATE'] = pd.to_datetime(mcl_event['STATUS_DATE'], format='%d-%m-%Y %H:%M')
    mcl_event['DESTINATION'] = np.where(mcl_event['DESTINATION'].isin(['STREETPOOLEUR', 'SEAPOOL', 'STREETPOOLIS']),
                                        mcl_event['CON_LOCATION'], mcl_event['DESTINATION'])
    mcl_event['DEPOT'] = mcl_event['DESTINATION']

    mcl_event['CONTAINER_NUMBER'] = mcl_event['CONTAINER_NUMBER'].str.upper()
    mcl_event = mcl_event[mcl_event['CONTAINER_NUMBER'].isin(con_list)]

    ice_event['STATUS_TIME'] = [('000' + str(i))[-4:-2] + ':' + ('000' + str(i))[-2:] for i in ice_event['STATUS_TIME']]
    ice_event['STATUS_DATE'] = ice_event['STATUS_DATE'].str[:11]
    ice_event['STATUS_DATE'] = ice_event['STATUS_DATE'] + ice_event['STATUS_TIME']
    ice_event['STATUS_DATE'] = pd.to_datetime(ice_event['STATUS_DATE'], format='%d-%m-%Y %H:%M')
    ice_event['DESTINATION'] = np.where(ice_event['DESTINATION'].isin(['STREETPOOLEUR', 'SEAPOOL', 'STREETPOOLIS']),
                                        ice_event['CON_LOCATION'], ice_event['DESTINATION'])
    ice_event['DEPOT'] = ice_event['DESTINATION']

    ice_event['CONTAINER_NUMBER'] = ice_event['CONTAINER_NUMBER'].str.upper()
    ice_event = ice_event[ice_event['CONTAINER_NUMBER'].isin(con_list)]

    ice_event['SOURCE'] = 'DORIS_ICE'
    mcl_event['SOURCE'] = 'DORIS_MCL'
    svdm_df['SOURCE'] = 'GREENCAT'
    gos_event['SOURCE'] = 'GOS'

    df = pd.concat([mcl_event, ice_event, svdm_df, gos_event])

    del mcl_event
    del ice_event
    del svdm_df
    del gos_event

    df['CONTAINER_NUMBER'] = df['CONTAINER_NUMBER'].str.upper()

    df = df[df['CONTAINER_NUMBER'].isin(con_list)]

    keep_list = []
    for i in set(df['CONTAINER_NUMBER']):
        if re.match(r'^[A-Z]{4}\d{7}$', i) is not None and (i[3] == 'U') & (i[:4] != 'DUMU') & (i[:4] != 'XXXU') & (
                i[:4] != 'PINU'):
            keep_list.append(i)
    df = df[df['CONTAINER_NUMBER'].isin(keep_list)]

    df = df[(~df['RES_GRADE'].isin(['SC', 'FS']))]

    last = df.sort_values('STATUS_DATE').drop_duplicates('CONTAINER_NUMBER', keep='last')
    last['TEXT_LINE'] = last['TEXT_LINE'].astype(str)
    last['TEXT_LINE'] = last['TEXT_LINE'].str.lower()

    LAST_containers = list(last[~last['TEXT_LINE'].str.contains('on rent')]['CONTAINER_NUMBER'])

    df = df[df['CONTAINER_NUMBER'].isin(LAST_containers)]

    df = df[['CONTAINER_NUMBER', 'DESCRIPTION', 'DESTINATION', 'SOURCE', 'STATUS_DATE', 'RES_GRADE', 'TEXT_LINE','DEPOT']]

    last_event = df.sort_values('STATUS_DATE').drop_duplicates('CONTAINER_NUMBER', keep='last')

    del df

    last_event['days_since_use'] = (pd.to_datetime(date.today()) - last_event['STATUS_DATE'])
    combined = pd.merge(last_event, df_base, left_on='CONTAINER_NUMBER', right_on='equipment_id', how='outer')
    del df_base

    combined_unique = combined.drop_duplicates()
    combined_unique['DESTINATION'].replace({'STREETPOOLEUR': 'XX', 'SEAPOOL': 'ZZ', 'STREETPOOLIS': 'YY', 'RF': 'RU'},
                                           inplace=True)
    combined_unique['DESTINATION2'] = combined_unique['DESTINATION'].str[:2]

    export = combined_unique[
        ['equipment_id', 'DESCRIPTION', 'SOURCE', 'STATUS_DATE', 'days_since_use', 'ACTIVE_ICE', 'ACTIVE_GOS', 'ACTIVE_GC',
         'DESTINATION2','DEPOT']]

    export.fillna('No_record_found', inplace=True)
    export.rename(columns={'equipment_id': 'CONTAINER_ID', 'DESCRIPTION': 'LAST_EVENT', 'SOURCE': 'EVENT_SOURCE',
                           'STATUS_DATE': 'LAST_EVENT_DATE', 'days_since_use': 'DAYS_SINCE_LAST_EVENT',
                           'DESTINATION2': 'LOCATION'}, inplace=True)

    export1 = export[export['DAYS_SINCE_LAST_EVENT'] != 'No_record_found']
    export1['DAYS_SINCE_LAST_EVENT'] = [i.days for i in export1['DAYS_SINCE_LAST_EVENT']]
    export = pd.concat((export1, export[export['DAYS_SINCE_LAST_EVENT'] == 'No_record_found']))

    export.to_csv(save_location + 'Equipment_data_test.csv')

    log=open(save_location+'Equipment_log.txt','a')
    log.write(current_time+"\t"+'Successful'+'\n')
    log.close()

except Exception as error:

    log=open(save_location+'Equipment_log.txt','a')
    log.write(current_time+"\t"+str(error)+'\n')
    log.close()

    print(str(error))