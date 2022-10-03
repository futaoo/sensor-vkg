#fetch ghcnd records according to a list of countries

import ftplib, os, wget, io, csv, time, fnmatch
from datetime import datetime, timedelta

cfd = os.path.dirname(os.path.abspath(__file__))
os.chdir(cfd)


code_country = ['EI']
ftp_server = 'ftp.ncdc.noaa.gov'
ftp_dir = 'pub/data/ghcn/daily'
ftp_dir_diff = 'superghcnd'
f_station = 'ghcnd-stations.txt'
f_station_csv = 'target-stations.csv'
ghcnd_version = 'ghcnd-version.txt'
dir_diff = 'diffghcnd'
f_countries_stack = 'latest-country-stack.txt'


f_station_exists = os.path.exists(f_station)
version_exists = os.path.exists(ghcnd_version)
f_countries_exists = os.path.exists(f_countries_stack)
code_station = []


heads_station = ['ID', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'STATE', 'NAME', 'GSN FLAG', 'HCN/CRN FLAG', 'WMO ID']
stations_new = []


def get_remote_version():
    with ftplib.FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_dir)
        ftp.cwd(ftp_dir_diff)
        ftp.sendcmd("TYPE i")

        latest = datetime.now()
        latest_next = (latest + timedelta(days=1)).strftime("%Y%m%d")
        latest = latest.strftime("%Y%m%d")

        try:
            url = 'superghcnd_diff_%s_to_%s.tar.gz' % (latest, latest_next)
            fileSize = ftp.size(url)
            while True:
                try:
                    latest = datetime.strptime(latest_next,"%Y%m%d")
                    latest_next = (latest + timedelta(days=1)).strftime("%Y%m%d")
                    latest = latest.strftime("%Y%m%d")
                    url = 'superghcnd_diff_%s_to_%s.tar.gz' % (latest, latest_next)
                    fileSize = ftp.size(url)
                except:
                    break
            return latest
        except:
            flag = 1
            while flag == 1 :
                try:
                    latest_next = datetime.strptime(latest,"%Y%m%d")
                    latest = (latest_next - timedelta(days=1)).strftime("%Y%m%d")
                    latest_next = latest_next.strftime("%Y%m%d")
                    url = 'superghcnd_diff_%s_to_%s.tar.gz' % (latest, latest_next)
                    fileSize = ftp.size(url)
                    flag = 0
                except:
                    pass
            return latest_next


def get_newest_periods(current):
    with ftplib.FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_dir)
        ftp.cwd(ftp_dir_diff)
        # starttimer = time.time()
        list_of_files = ftp.nlst()
        list_periods = []
        
        # current = '20220810'
        flag = 1
        while flag == 1:
            for name in list_of_files:
                flag = 0
                if fnmatch.fnmatch(name,"*%s_to*" % (current)):
                    latest = name.split('_')[-1]
                    latest = latest.split('.')[0]
                    list_periods.append([current, latest])
                    current = latest
                    flag = 1
                    break
        # print(list_periods)
        # endtimer = time.time()
        # print(endtimer-starttimer)
        return list_periods

def get_newest_version():
    with ftplib.FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_dir)
        ftp.cwd(ftp_dir_diff)
        starttimer = time.time()
        list_of_files = ftp.nlst()
        list_periods = []
        
        latest = datetime.now().strftime('%Y%m%d')
        while True:
            for name in list_of_files:
                if fnmatch.fnmatch(name,"*to_%s*" % (latest)):
                    return latest
            latest = datetime.strptime(latest,"%Y%m%d") - timedelta(days=1)
            latest = latest.strftime("%Y%m%d")

# to ensure we don't duplicate any data from the country that has been queried historically
if f_countries_exists:
    with open(f_countries_stack) as f:
        for line in f:
            words = line.split()
            code_country.append(words[0])
code_country = list(set(code_country))
print(code_country)


print('Fetching remote ghcnd station list and updating the selected stations in %s ...' % f_station_csv)
with ftplib.FTP(ftp_server) as ftp:
    ftp.login()
    ftp.cwd(ftp_dir)
    # ftp.dir()
    with open(f_station, 'wb') as fb:
        print ('Downloading ' + f_station + '...\n')
        ftp.retrbinary('RETR %s' % f_station, fb.write)
    with open(f_station) as f:
        for line in f:
            words = [line[0:11],line[12:20],line[21:30],line[31:37],line[38:40],line[41:71],line[72:75],line[76:79],line[80:85]]
            if words[0][:2] in code_country:
                code_station.append(words[0])
                stations_new.append(dict(zip(heads_station,words)))

with open(f_station_csv, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames = heads_station)
    writer.writeheader()
    writer.writerows(stations_new)

#version control
current = ''
latest  = ''
# record the version for every update; the version code is the "modified date" of the remote superghcnd folder
if not version_exists:
    print('Initializing version control...')
    current = get_newest_version()
    with open(ghcnd_version, 'w') as f:
        print('Writing version %s into ' % current + ghcnd_version + '...')
        f.write(current)
    print('Version added!!')
    latest = current
else:
    with open(ghcnd_version) as f:
        current = f.readline()
    
    try:
        latest = get_newest_periods(current)[-1][-1]

        print('Your dataset is current at version %s' % current)
        print('The latest version of ghcnd is %s' % latest)
        
    
        answer = input('A new version of ghcnd dataset is found.\nEnter yes or no to proceed the update %s -> %s:' % (current, latest))
        if answer == 'yes':
            with open(ghcnd_version, 'w') as f:
                print('Writing lastest version %s into ' % latest + ghcnd_version + '...')
                f.write(latest)
        else:
            exit()
    except:
        latest = current
        print('Your current dataset is up to date!')


#poll the all the ghcnd data as per country and store them into the folder for each country; since countries may be
#added at different time, we track each country in terms of when the data is polled first time           
for country in code_country:
    out_folder = 'ghcnd-%s' % country
    if not os.path.exists(out_folder): 
        print('Data not found for country %s . Start retrieving now...' % country)
        with open(f_countries_stack,'a+') as f:
            f.write(country + ' ' + latest + '\n')
        
        base_dir = out_folder + '/baseDB' + '-' + latest
        os.makedirs(base_dir)
        print('A new storage folder %s has been created for country %s ' % (out_folder, country))

        for station in code_station:
            if station[:2] == country:
                print('\nRetrieving data from ' + station)
                url = 'ftp://' + ftp_server + '/' + ftp_dir + '/' + 'by_station/' + '%s.csv.gz' % station
                wget.download(url, out=base_dir)
        print('\nData retrieving for empty country completed!! Check latest-country-stack.txt for the most recent version as per country')
    
if latest != current:
    print('\nStart updating data for all countries')
    # start = datetime.strptime(current,"%Y%m%d")
    # end = datetime.strptime(latest,"%Y%m%d")
    # daily_periods = [[(start+ timedelta(days=i)).strftime("%Y%m%d"),(start+ timedelta(days=i+1)).strftime("%Y%m%d")] for i in range(0,(end-start).days,1)]
    periods = get_newest_periods(current)
    if not os.path.exists(dir_diff):
        os.mkdir(dir_diff)
    for period in periods:
            url = 'ftp://' + ftp_server + '/' + ftp_dir + '/' + ftp_dir_diff + '/' + 'superghcnd_diff_%s_to_%s.tar.gz' % (period[0], period[1])
            print('\nRetrieving diff data from remote ' + 'superghcnd_diff_%s_to_%s.tar.gz' % (period[0], period[1]))
            wget.download(url, out=dir_diff)
        







