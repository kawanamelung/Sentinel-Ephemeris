from  xml.etree.ElementTree import ElementTree as ET
import psycopg2
import psycopg2.extras
import os
import datetime as dt
import time
import glob

def main():
    files = eof_file()
    numfiles = len(files)
    print('\nNumber of Files to insert: ', numfiles)
    count = 1
    conn=psycopg2.connect(dbname='dev_eodata',
                                 host='greenlaser.int.unavco.org',
                                 port=5432,
                                 user=os.environ.get('DB_USER'),
                                 password=os.environ.get('DB_PASSWORD'))
    c  = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS Kawan2
                (id SERIAL, 
                satellite TEXT NOT NULL, 
                orbit_type TEXT NOT NULL, 
                data_source TEXT NOT NULL,
                observation_time timestamp NOT NULL, 
                position geometry(POINTZ,4978) NOT NULL, 
                velocity_x double precision NOT NULL,
                velocity_y double precision NOT NULL,
                velocity_z double precision NOT NULL
                )''')

    
    for currentfile in files:
        t = time.time()
        xml_root=ET(file=open(currentfile)).getroot() 
        node = xml_root.find('Data_Block/List_of_OSVs')

        args_str = str()
        list_of_rows = list()
        for osv in node.getchildren():
            satellite = currentfile.split("_")[0]
            orbit_type = currentfile.split("_")[3]
            data_source = currentfile
            orbit_time = dt.datetime.strptime(osv.find('UTC').text[4:],"%Y-%m-%dT%H:%M:%S.%f")
            orbit_position = 'POINTZ(%f %f %f)' % (float(osv.find('X').text),float(osv.find('Y').text),float(osv.find('Z').text))
            vx = float(osv.find('VX').text)
            vy = float(osv.find('VY').text)
            vz = float(osv.find('VZ').text)
            list_of_rows.append((satellite, orbit_type, data_source, orbit_time, orbit_position, vx, vy, vz))
        
        for index,tup in enumerate(list_of_rows):
            args_str += ((c.mogrify("(%s, %s, %s, %s, ST_GeomFromText(%s, 4978), %s, %s, %s)", tup)).decode('utf-8')) + ','
    
        newstr = args_str[:-1] + ';'

        c.execute('INSERT INTO Kawan2 (satellite,orbit_type,data_source,observation_time,position,velocity_x,velocity_y,velocity_z) VALUES ' + newstr)
        print('{} sec, {} files remaining'.format(round(time.time()-t,4), numfiles - count))
        count += 1 
    conn.commit()
    conn.close()

def eof_file():
    return glob.glob('*_201412*EOF')

if __name__ == '__main__':
    tstart = time.time()
    main()
    print('total time: ',time.time()-tstart)
