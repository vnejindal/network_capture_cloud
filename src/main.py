import logging
import os
import sys
import gzip 
import paho.mqtt.client as mqtt
import json 

## AWS S3 Support
import boto3
import botocore
####
from time import sleep
from time import time
#from threading import Thread
import threading

g_config = {}
#Threads List 
g_ap_thr_list = []

#### MQTT HANDLING PART ####
mqtt_rc_codes = {
                    0: 'Connection successful',
                    1: 'Connection refused - incorrect protocol version',
                    2: 'Connection refused - invalid client identifier',
                    3: 'Connection refused - server unavailable',
                    4: 'Connection refused - bad username or password',
                    5: 'Connection refused - not authorised'
                }

######## MQTT Handling ###########
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    logging.info('MQTT Client Connected: %s', str(rc))
    
    global mqtt_rc_codes
    
    if rc != 0:
        print 'Client connection failed: ', str(rc), mqtt_rc_codes[rc]
        logging.info('MQTT client connection issue %s %s', str(rc), mqtt_rc_codes[rc])
    

def on_disconnect(client, userdata, rc):
    if rc == 0:
        print 'Client disconnected successfully'
        logging.info('MQTT Client Disconnected')
    else:
        print 'Client disconnection issue: ', str(rc)
        logging.info('MQTT Client Disconnected issue %s', str(rc))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("vne:: "+msg.topic+" "+str(msg.payload))
    logging.info('mqtt msg recived: %s %s', msg.topic, str(msg.payload))
        
def on_publish(client, userdata, mid):
    #print("mid: "+str(mid))
    logging.info('mqtt msg published. ')
    pass

def start_mqtt():
    """
    Connects with MQTT broker
    """
    
    global g_config
    global g_ap_thr_list
    
    if g_config['mqtt']['enabled'] is not 1:
        return
    
    thr_count = 1
    ap_thread = threading.Thread(target=mqtt_thread, args=(thr_count,))
    g_ap_thr_list.append(ap_thread)
    ap_thread.start()
    
def mqtt_thread(count):
    
    global g_config
    
    srv_ip = g_config['mqtt']['broker_url']
    srv_port = g_config['mqtt']['broker_port']
    srv_keepalive = 1
    
    logging.info('starting mqtt thread %d', count)
    
    print 'connecting to broker:', srv_ip,':', srv_port, ' ', srv_keepalive
    logging.info('connecting to mqtt broker: %s %s', srv_ip, srv_port)
    
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_publish = on_publish
    mqtt_client.on_disconnect = on_disconnect
    
    mqtt_client.connect(srv_ip, srv_port, srv_keepalive)

    print 'connection to broker successful'  
    logging.info('connection to mqtt broker successful')
    g_config['mqtt']['mqtt_client'] = mqtt_client
    
    mqtt_client.loop_forever()
    #client.loop_start()
    #dpublish.read_device_data('temperature', '1', client)
    
def publish_msg_mqtt(msg):
    global g_config 
    """
    vne::tbd:: 
    - add topic logic at init time 
    - a timestamp as well
    """
    (rc, mid) = g_config['mqtt']['mqtt_client'].publish(g_config['mqtt']['topic'], msg, g_config['mqtt']['qos'])

#### MQTT HANDLING PART ####

### COMMON FUNCTIONS ###
def get_json_config(json_file):
    """
    returns json object of a json file
    """
    fp = open(json_file)
    json_config = fp.read()
    fp.close()
    return json.loads(json_config)

def load_config(config_file):
    global g_config
    g_config = get_json_config(config_file)


def init_log():
    global g_config
    
    if g_config['logging']['enabled'] is not 1:
        return
    
    log_file = g_config['logging']['file']
    #vne::tbd:: get log level from config
    log_level = logging.DEBUG
    logging.basicConfig(filename = log_file, level=log_level, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.info('\tProgram Started')
    logging.info('========================================')
    

def get_file_list(dpath, file_type):
    """
    Returns the list of files of type 'file_type' present at directory 'dpath'
    """
    files_list = list()
    for (dirpath, dirnames, filenames) in os.walk(dpath):
        for file in filenames: 
            if file_type in file:
                files_list += os.path.join(dirpath, file)
                ## tbd: Check if this single line statement works in python files_list += [os.path.join(dirpath, file) for file in filenames if file_type in file]    
    return files_list

def is_file_open(filename):
    """
    Checks if file is opened by any other process or not
    True: It is open
    False: It is not used by any other
    """
    import psutil

    for proc in psutil.process_iter():
        try:
            # this returns the list of opened files by the current process
            flist = proc.open_files()
            if flist:
                #print(proc.pid,proc.name)
                for filename in flist:
                    #print("\t",nt.path)
                    return True

        # This catches a race condition where a process ends
        # before we can examine its files    
        except psutil.NoSuchProcess as err:
            #print("****",err) 
            pass
        continue
    
    return False

def gzip_file(f_name, gzip_file):
    f_in = open(f_name)
    f_out = gzip.open(gzip_file, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    
### COMMON FUNCTIONS ###

def start_capture_module():
    
    global g_config
    
    if g_config['network_capture']['enabled'] is not 1: 
        return 
    
    thr_count = 2
    ap_thread = threading.Thread(target=capture_thread, args=(thr_count,))
    g_ap_thr_list.append(ap_thread)
    ap_thread.start()

def capture_thread(count):
    
    global g_config 
    logging.info('starting capture thread %d', count)

    if not os.path.exists(g_config['network_capture']['path']):
        os.mkdir(g_config['network_capture']['path'])
        logging.info('Directory created: %s',g_config['network_capture']['path'] )
    else:
        logging.info('Capture Directory exists: %s',g_config['network_capture']['path'] )

    itf = g_config['network_capture']['interface']
    pcap_file = '/'.join([g_config['network_capture']['path'], g_config['network_capture']['pcap_file']])
    
    #mqtt publish tcpdump commandline 
    
    #cmd_name = 'tcpdump -i ' + itf + ' -n -w ' +  pcap_file + ' -U -C 1 -K -n -G 10'
    cmd_name = 'tcpdump -i ' + itf + ' -n -w ' +  pcap_file + ' -U -C 1 -K -n'
    os.system(cmd_name)
    
def start_upload_module():
    thr_count = 3
    ap_thread = threading.Thread(target=upload_thread, args=(thr_count,))
    g_ap_thr_list.append(ap_thread)
    ap_thread.start()

def upload_thread(count):
    """
    check for a new pcap file; 
    zip and upload to S3
    """
    global g_config
    
    logging.info('starting upload thread %d', count)
    
    bucketName = g_config['aws_s3']['bucket_name']
    pcap_path = g_config['network_capture']['path']
    
    s3 = boto3.client('s3')
    
    while True:        
        "Get a list of files that exist in pcaps directory"
        pcap_list = get_file_list(pcap_path, '.pcap')
    
        for fname in pcap_list:
            if is_file_open(fname) is False:
                """
                zip, S3 upload, MQTT channel update, remove it
                """
               
                #Append current timestamp as well 
                time =  time.time()
                ofile = fname + str(time) + '.tar.gz'
                gzip_file(fname, ofile)
                s3.upload_file(ofile,bucketName,ofile)
                
                publish_msg_mqtt("pcap file uploaded " + ofile)
                logging.info('pcap file uploaded %s', ofile)
                os.remove(fname, ofile)
                logging.info('files removed %s %s', fname, ofile)
                
        sleep(5)
    

def start_modules():
    """
    start following modules in each thread: 
    1. tcpdump to capture
    2. mqtt channel 
    3. gzip and upload to aws s3
    """
    global g_config
    
    start_mqtt()
    start_capture_module()
    start_upload_module()

def main():    
    print 'network capture to cloud program started'
    print '========================================'
    global g_config
    if len (sys.argv) != 1 :
        print "Usage: python main.py"
        sys.exit(1)   
    
    config_file = 'config.json'
    load_config(config_file)
    init_log()
    
    start_modules()
       
    
    print 'program end...'

if __name__ == '__main__':
    main()
    
"""
TBDs: 
1. Handing of key 'path' in pcap and log files - done
2. Save last 4 pcap archives on disk as well - may b logrotate will help here
"""


