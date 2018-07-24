#!/usr/bin/env python3

import argparse
import sys
from s3_stress.s3_stress_runner import server_logger
from s3_stress import pcap_tools

logger = server_logger.Logger(name=__name__).logger


def get_args():
    parser = argparse.ArgumentParser(
        description='pcap to HTTP requests (c) - 2018')
    parser.add_argument('pcap_file', type=str, help="Path to pcap file")
    parser.add_argument('ip', type=str, help="IP to be filtered")
    parser.add_argument('--port', type=int, help="TCP Port to be filtered", default=80)
    parser.add_argument('--loop', action='store_true', help="Repeat pcap in infinite loop")
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    cap = pcap_tools.open_pcap_file(args.pcap_file,
                                    display_filter='http and tcp.port=={} and ip.addr=={}'.format(args.port, args.ip))
    logger.info("Filter applied: \'http and tcp.port=={} and ip.addr=={}\'".format(args.port, args.ip))
    for http_packet in pcap_tools.get_next_packet(cap):
        logger.debug("Got http packet {}".format(http_packet.request_method))
        pcap_tools.handle_http_methods(http_packet.request_method, packet=http_packet)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
