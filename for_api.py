import pika
import json
import dataset
import uuid
import pymysql
pymysql.install_as_MySQLdb()

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
    display_name = decoded_json['display_name']
    operation = decoded_json['op']

    db = dataset.connect('mysql://dcm_user:dcmUser@1115@127.0.0.1/wsato_qiligeer')

    if operation == 'create':
        # Select server
        vc_servers_table = db['vc_servers']
        results = vc_servers_table.find_one(order_by = '-free_size_gb')
        server_id = results['id']
        server_name = results['name']
        free_size_gb = int(results['free_size_gb'])
        size = int(decoded_json['size'])
        # Check free space
        if free_size_gb < size:
            pass # TODO logger

        # Check name
        domains_table = db['domains']
        result = domains_table.find_one(
            user_id  = user_id,
            name     = display_name)
        if result != None:
            # TODO logger
            pass

        # Create uniquename and mapping
        domain_name = uuid.uuid4().hex

        db.begin()
        try:
            # Create domains record.
            domain_id = domains_table.insert(dict(name = domain_name, display_name = display_name, user_id = user_id, server_id = server_id))
            # Update vc_server
            vc_servers_table.update(dict(id = server_id, free_size_gb = free_size_gb - size), ['id']
            db.commit()
        except:
            db.rollback()
            pass

        # Select Vhosts
        con = pika.BlockingConnection(pika.ConnectionParameters(
            virtual_host = '/' + server_name , credentials = credentials))
        chn = con.channel()
        chn.queue_declare(queue = 'from_middleware_to_agent', durable=True)

        enqueue_message = {
            'op'  : operation,
            'name' : domain_name,
            'size' : size
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
            name     = vm_name)
        if result == None:
            print('bad permission')
            # TODO Write status
            pass

        domain_id    = result['domain_id']
        domain_table = db['domains']
        result       = domain_table.find_one(domain_id = domain_id)

        # TODO Check operation


        enqueue_message = {
            'ope'  : operation,
            'name' : result['domain_name']
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
