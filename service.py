import pika
import json
import dataset
import uuid
import traceback
import pymysql
import logging
import logging.handlers
pymysql.install_as_MySQLdb()


logger = logging.getLogger('wsato_qiligeer_dcm_for_api')
logger.setLevel(logging.WARNING)
handler = logging.handlers.TimedRotatingFileHandler(
    filename = '/var/log/wsato_qiligeer/wsato_qiligeer_dcm_for_api.log',
    when = 'D'
    )
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(handler)


credentials = pika.PlainCredentials('server1_dcm', '8nfdsS12gaf')
connection  = pika.BlockingConnection(pika.ConnectionParameters(
    virtual_host = '/server1', credentials = credentials))
channel = connection.channel()

channel1 = connection.channel()
channel1.queue_declare(queue = 'from_api_to_middleware', durable = True)

channel2 = connection.channel()
channel2.queue_declare(queue = 'from_middleware_to_agent', durable = True)


def from_api_to_middleware_callback(ch, method, properties, body):
    decoded_json = json.loads(body.decode('utf-8'))
    user_id = decoded_json['user_id']
    display_name = decoded_json['name']
    operation = decoded_json['op']

    db = dataset.connect('mysql://dcm_user:dcmUser@1115@localhost/wsato_qiligeer')

    if operation == 'create':
        # Select server
        server_id = results['id']
        server_name = results['name']
        os = results['os']
        free_size_gb = int(results['free_size_gb'])
        size = int(decoded_json['size'])
        ram = int(decoded_json['ram'])
        vcpus = int(decoded_json['vcpus'])


        target_server = None
        t_size = 0
        t_core = 0
        t_ram = 0
        vc_servers_table = db['vc_servers']
        for server in vc_servers_table.find:
            t_size = int(vc_server['free_size_gb']) - size
            t_core = int(vc_server['free_cpu_core']) - vcpus
            t_ram = int(vc_server['free_ram_byte']) - ram
            if 0 < t_size and  0 < t_core and 0 < t_ram:
                target_server = server

        # Check free space
        if target_server == None:
            return

        # Check name
        domains_table = db['domains']
        result = domains_table.find_one(
            user_id  = user_id,
            name     = display_name)

        if result != None:
            logger.eror('Can not create vm because of name duplication.')
            return

        # Create unique name
        name = uuid.uuid4().hex

        db.begin()
        try:
            # Create domains record
            domain_id = domains_table.insert(dict(name = name, display_name = display_name, size = size, vcpus = vcpus, ram = ram,  user_id = user_id, server_id = server_id))
            # Update vc_server
            vc_servers_table.update(dict(id = server_id, free_size_gb = t_size, free_cpu_core = t_core, free_ram_byte = t_ram), ['id'])
            db.commit()
        except:
            db.rollback()
            logger.eror('Insert or Update failed due to data error.')
            logger.eror(traceback.format_exc())
            return

        # Select Vhosts
        con = pika.BlockingConnection(pika.ConnectionParameters(
            virtual_host = '/' + server_name , credentials = credentials))
        chn = con.channel()
        chn.queue_declare(queue = 'from_middleware_to_agent', durable=True)

        enqueue_message = {
            'op'    : operation,
            'name'  : name,
            'size'  : size,
            'os'    : decoded_json['os'],
            'ram'   : decoded_json['ram'],
            'vcpus' : decoded_json['vcpus']
        }
        # Enqueue
        chn.basic_publish(exchange = '',
                          routing_key = 'from_middleware_to_agent',
                          body = json.dumps(enqueue_message))
        con.close()
    else:
        # Authentication check
        domains_table = db['domains']
        result = domains_table.find_one(
            user_id  = user_id,
            name     = display_name)
        if result == None:
            logger.eror('Can not operation vm because of it is not exists.')
            return

        domain_id = result['id']
        result    = domain_table.find_one(domain_id = domain_id)

        enqueue_message = {
            'op'  : operation,
            'name' : display_name
        }

        server_table = db['servers']
        result = server_table.find_one(id = server_id)

        # Select Vhosts
        con = pika.BlockingConnection(pika.ConnectionParameters(
            virtual_host = '/' + result['name'], credentials = credentials))
        chn = con.channel()
        chn.queue_declare(queue = 'from_middleware_to_agent', durable = True)

        # Enqueue
        chn.basic_publish(exchange = '',
                          routing_key = 'from_middleware_to_agent',
                          body = json.dumps(enqueue_message))
        con.close()

channel1.basic_consume(from_api_to_middleware_callback,
                      queue = 'from_api_to_middleware',
                      no_ack=True)

channel1.start_consuming()
