#!/usr/bin/env python3

# Connect a client, check it is restored, clear the client, check it is not there.

from mosq_test_helper import *
persist_help = persist_module()

port = mosq_test.get_port()
conf_file = os.path.basename(__file__).replace('.py', '.conf')
persist_help.write_config(conf_file, port)

rc = 1

persist_help.init(port)

keepalive = 10
client_id = "persist-client-v5-0"
proto_ver = 5

connect_props = mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_SESSION_EXPIRY_INTERVAL, 60)
connect_props += mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_MAXIMUM_PACKET_SIZE, 10000)
connect_packet = mosq_test.gen_connect(client_id, keepalive=keepalive, proto_ver=proto_ver, clean_session=False, properties=connect_props)
connack_packet1 = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)
connack_packet2 = mosq_test.gen_connack(rc=0, flags=1, proto_ver=proto_ver)

connect_packet_clean = mosq_test.gen_connect(client_id, keepalive=keepalive, proto_ver=proto_ver, clean_session=True)

broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

con = None
try:
    # Connect client
    sock = mosq_test.do_client_connect(connect_packet, connack_packet1, timeout=5, port=port, connack_error="connack 1")
    mosq_test.do_ping(sock)
    sock.close()

    # Kill broker
    broker_terminate_rc = mosq_test.terminate_broker(broker)

    persist_help.check_counts(port, clients=1)
    # FIXME - port persist_help.check_client(port, "persist-client-v5-0", None, 0, 1, port, 10000, 2, 1, 60, 0)
    persist_help.check_client(port, "persist-client-v5-0", None, 0, 1, None, 10000, 2, 1, 60, 0)

    # Restart broker
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    # Connect client again, it should have a session
    sock = mosq_test.do_client_connect(connect_packet, connack_packet2, timeout=5, port=port, connack_error="connack 2")
    mosq_test.do_ping(sock)
    sock.close()

    # Clear the client
    sock = mosq_test.do_client_connect(connect_packet_clean, connack_packet1, timeout=5, port=port, connack_error="connack 3")
    mosq_test.do_ping(sock)
    sock.close()

    # Connect client, it should not have a session
    sock = mosq_test.do_client_connect(connect_packet_clean, connack_packet1, timeout=5, port=port, connack_error="connack 4")
    mosq_test.do_ping(sock)
    sock.close()

    # Kill broker
    (broker_terminate_rc, stde) = mosq_test.terminate_broker(broker)
    broker = None

    persist_help.check_counts(port)

    # Restart broker
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    # Connect client, it should not have a session
    sock = mosq_test.do_client_connect(connect_packet_clean, connack_packet1, timeout=5, port=port, connack_error="connack 5")
    mosq_test.do_ping(sock)
    sock.close()

    (broker_terminate_rc, stde) = mosq_test.terminate_broker(broker)
    broker = None
    persist_help.check_counts(port)

    rc = broker_terminate_rc
finally:
    if broker is not None:
        broker.terminate()
        if mosq_test.wait_for_subprocess(broker):
            print("broker not terminated (2)")
            if rc == 0: rc=1
        (_, stde) = broker.communicate()
    os.remove(conf_file)
    rc += persist_help.cleanup(port)

    if rc:
        print(stde.decode('utf-8'))


exit(rc)
