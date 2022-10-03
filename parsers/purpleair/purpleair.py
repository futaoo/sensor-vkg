# from urllib import request
from tabnanny import check
from country_bounding_boxes import country_subunits_containing_point, country_subunits_by_iso_code
from geopy.geocoders import Nominatim
from numpy import average
import requests, csv, os, time
from datetime import datetime
from functions import gen_range
# import countries

cfd = os.path.dirname(os.path.abspath(__file__))
os.chdir(cfd)

f_sensors = 'sensors.csv'
f_version = 'versions.csv'
f_countries_stack = 'latest-country-stack.txt'
f_data = 'data-2'
f_task = 'finished_tasks.txt'


urls = {
    'sensors':'https://api.purpleair.com/v1/sensors',
    'sensor_history':'https://api.purpleair.com/v1/sensors/%s/history/csv'
}
headers = {
    'X-API-Key':'C7D7011A-68C9-11EC-B9BF-42010A800003'
}

country_code = 'IE'
fields_data = ['pm2.5_atm_a', 'pm2.5_atm_b', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 'humidity_a', 'humidity_b',
                'temperature_a', 'temperature_b', 'pressure_a', 'pressure_b']
fields_sensor = ['sensor_index','name','latitude','longitude','altitude','location_type','date_created','last_seen']

params_sensor = ['fields', 'nwlng', 'nwlat', 'selng', 'selat', 'max_age']
params_data = ['fields', 'average', 'start_timestamp', 'end_timestamp']


f_sensors_exists = os.path.exists(f_sensors)

if not f_sensors_exists:
    fields_sensor = fields_sensor #sensor parameters
    # bounding box as per country: can be more than one bbox divided from a country
    lst_bbox = [c.bbox for c in country_subunits_by_iso_code(country_code)]

    lst_params_sensor = [] # a list of [ params as per bbox]
    for bbox in lst_bbox:
        params_sensor = dict(zip(params_sensor, [','.join(fields_sensor), bbox[0], bbox[3], bbox[2], bbox[1], 0]))
        lst_params_sensor.append(params_sensor) 

    sensors = [] # a list of [ {sensor info} ]
    country_key = 'country' # used to split data as per country
    versions = [] # version control as per sensor

    geolocator = Nominatim(user_agent="geoapiExercises") # to determine the country of a lat/long coordinate (online)
    # cc = countries.CountryChecker('./WORLD_BORDERS/TM_WORLD_BORDERS-0.3.shp') # to determine the country of a lat/long coordinate (offline)
    for params_sensor in lst_params_sensor:
        # print(params_sensor)
        r = requests.get(url = urls['sensors'], headers = headers, params = params_sensor)
        keys = r.json()['fields']
        lst_values = r.json()['data']
        for values in lst_values:
            sensor = dict(zip(keys, values))
            # print(sensor)
            lat = sensor['latitude']
            lng = sensor['longitude']
            # code = cc.getCountry(countries.Point(lat,lng)).iso
            code = geolocator.reverse([str(lat),str(lng)]).raw['address']['country_code'].upper()
            if code == country_code:
                
                #create version control for each sensor
                version = dict()
                version['sensor_index'] = sensor['sensor_index']
                version['date_created'] = sensor['date_created']
                version['last_seen'] = sensor['last_seen']
                version['terminate'] = ''
                latest_unix_timestamp = int(time.time())
                if latest_unix_timestamp - int(version['last_seen']) > 604800:
                    version['active'] = 'no'
                else:
                    version['active'] = 'yes'
                versions.append(version)


                sensor[country_key] = code
                del sensor['last_seen']
                sensors.append(sensor)


    with open(f_sensors, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = sensors[0].keys())
        writer.writeheader()
        writer.writerows(sensors)

    with open(f_version, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = versions[0].keys())
        writer.writeheader()
        writer.writerows(versions)

       
average = 60
if not os.path.exists(f_data):  # initialize the csv files storage folder and checkpoint for log
    print('No data record is found. Start retrieving now...')

    #load sensor versions
    versions = []
    with open(f_version) as f:
        versions = [{k:v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]

    os.mkdir(f_data)

    if average == 60:
        step = 14 * 24 * 60 * 60 # 14 days
    else:
        step = 2 * 24 * 60 * 60 # 2 days
    

    # checkpoint_key = ['sensor_index', 'terminate', 'current_at', 'task_version']
    
    latest_unix_timestamp = int(time.time())
    version['version'] = latest_unix_timestamp
    for version in versions:
        if not version['sensor_index']:
            print('No sensor_index found. Please check the returned data format from remote')
            exit()
        checkpoint = dict()
        checkpoint['sensor_index'] = version['sensor_index']
        checkpoint['terminate'] = version['date_created']
        checkpoint['version'] = latest_unix_timestamp
        btime = int(checkpoint['terminate'])
        etime = checkpoint['version']
        if version['active'] == 'no':
            etime = int(version['last_seen'])

        for block in gen_range(btime, etime, step):
            #request data from the remote for the first time
            print('Downloading for PA: %s for Dates: %s and %s.' 
                    %(version['sensor_index'],datetime.fromtimestamp(block[0]),datetime.fromtimestamp(block[1])))
            r_params = dict(zip(params_data,[','.join(fields_data), average, block[1], block[0]])) # make up the request params
            url = urls['sensor_history'] % version['sensor_index']
            r = requests.get(url=url, headers=headers, params=r_params)
            with open('%s/%s_%s_%s.csv' % (f_data, version['sensor_index'], str(block[1]), str(block[0])), 'w') as csvfile:
                csvfile.write(r.text)
        
            checkpoint['current_at'] = block[1]
            with open('checkpoint.txt', 'a+') as f:
                f.write('sensor_index:%s,terminate:%s,current_at:%s,task_version:%s\n' % (checkpoint['sensor_index'],
                checkpoint['terminate'], checkpoint['current_at'],  checkpoint['version']))
else:
    # load checkpoints and versions
    versions = []
    with open(f_version) as f:
        versions = [{k:v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]
    
    # seek the last line for the most recent record
    with open("checkpoint.txt", "rb") as file:
        try:
            file.seek(-2, os.SEEK_END)
            while file.read(1) != b'\n':
                file.seek(-2, os.SEEK_CUR)
        except OSError:
            file.seek(0)
        last_line = file.readline().decode()[:-1]
    
    infos = last_line.split(',')
    recent_check = dict()
    for info in infos:
        key_value = info.split(':')
        recent_check[key_value[0]] = key_value[1]


    count = 0
    while True:
        if versions[count]['sensor_index'] == recent_check['sensor_index']:
            break
        else:
            count+=1

    flag =0

    if average == 60:
        step = 14 * 24 * 60 * 60 # 14 days
    else:
        step = 2 * 24 * 60 * 60 # 2 days

    latest_unix_timestamp = None

    if recent_check['terminate'] != recent_check['current_at']:
        rest_versions = versions[count:]
    elif (count+1)<len(versions):
        rest_versions = versions[count+1:]
    else:
        # with open(f_task, 'w') as f:
        #     f.write(recent_check['task_version'])
        rest_versions = versions
        latest_unix_timestamp = int(time.time())

    for version in rest_versions:
        if not version['sensor_index']:
            print('No sensor_index found. Please check the returned data format from remote')
            exit()
        checkpoint = dict()
        checkpoint['sensor_index'] = version['sensor_index']
        checkpoint['terminate'] = version['date_created']

        if version['terminate'] != '':
            checkpoint['terminate'] = version['terminate']


        # if os.path.exists(f_data):
        #     with open(f_task, 'r') as f:
        #         finished_task = f.readline()
        #     checkpoint['terminate'] = finished_task
        
        checkpoint['version'] = recent_check['task_version']

        btime = int(checkpoint['terminate'])
        etime = int(checkpoint['version'])
        
        if version['active'] == 'no':
            etime = int(version['last_seen'])

        if version['sensor_index'] == recent_check['sensor_index']:
            etime = int(recent_check['current_at'])

        #automatically set the timestamp as the task version when one round (all sensors) is finished
        if latest_unix_timestamp is not None:
            if version['active'] == 'no':
                continue
            checkpoint['terminate'] = recent_check['task_version']
            checkpoint['version'] = latest_unix_timestamp
            btime = int(checkpoint['terminate'])
            etime = int(checkpoint['version'])
        
        for block in gen_range(btime, etime, step): #request data from the remote for the first time
            print('Downloading for PA: %s for Dates: %s and %s.' 
                    %(version['sensor_index'],datetime.fromtimestamp(block[0]),datetime.fromtimestamp(block[1])))
            r_params = dict(zip(params_data,[','.join(fields_data), average, block[1], block[0]])) # make up the request params
            url = urls['sensor_history'] % version['sensor_index']
            r = requests.get(url=url, headers=headers, params=r_params)
            with open('%s/%s_%s_%s.csv' % (f_data, version['sensor_index'], str(block[1]), str(block[0])), 'w') as csvfile:
                csvfile.write(r.text)
        
            checkpoint['current_at'] = block[1]
            # everytime when one sensor finishes a task version, the finished task version will be recorded for next round as new terminate time
            if str(checkpoint['current_at']) == str(checkpoint['terminate']):
                version['terminate'] = checkpoint['version']
                with open(f_version, 'w') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames = versions[0].keys())
                    writer.writeheader()
                    writer.writerows(versions)

            with open('checkpoint.txt', 'a+') as f:
                f.write('sensor_index:%s,terminate:%s,current_at:%s,task_version:%s\n' % (checkpoint['sensor_index'],
                checkpoint['terminate'], checkpoint['current_at'],  checkpoint['version']))






    




        
        
        
           

        


    

        








