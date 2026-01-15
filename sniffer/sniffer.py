"""Sniffer minimal: lanza tcpdump para observar tráfico sin interferir.

Este script no actúa como proxy: sólo ejecuta tcpdump con un filtro
configurable y opcionalmente guarda a un archivo pcap.

Nota sobre Docker: para capturar paquetes desde un contenedor necesitas
ejecutarlo con capacidades de captura: --cap-add=NET_RAW --cap-add=NET_ADMIN
o usar --network host según el caso. Si estás en un overlay network de Swarm,
la captura puede necesitar ejecutarse en el nodo host donde circula el tráfico.
"""

import argparse
import shutil
import subprocess
import sys


def find_tcpdump():
    return shutil.which('tcpdump')


def build_cmd(interface, host, port, pcap, ascii_output, extra_filter_push):
    cmd = ['tcpdump']
    if interface:
        cmd += ['-i', interface]
    else:
        cmd += ['-i', 'any']

    cmd += ['-n']  # don't resolve names

    if pcap:
        cmd += ['-w', pcap]
    else:
        # human readable
        if ascii_output:
            cmd += ['-A']
        else:
            cmd += ['-XX']

    # build filter
    flt_parts = []
    if host:
        flt_parts.append(f'host {host}')
    if port:
        flt_parts.append(f'port {port}')

    if extra_filter_push:
        # capture only PUSH packets (data-carrying TCP segments)
        flt = ' and '.join(flt_parts) + ' and tcp[tcpflags] & tcp-push != 0' if flt_parts else 'tcp[tcpflags] & tcp-push != 0'
    else:
        flt = ' and '.join(flt_parts) if flt_parts else ''

    if flt:
        cmd += [flt]

    return cmd


def main():
    parser = argparse.ArgumentParser(description='Sniffer observer (tcpdump runner)')
    parser.add_argument('--interface', '-i', help='Interface to listen on (default: any)')
    parser.add_argument('--host', '-t', help='Target host to filter (IP or name)')
    parser.add_argument('--port', '-p', help='Target port to filter')
    parser.add_argument('--pcap', help='Write raw pcap to this file (if omitted, prints ASCII/hex)')
    parser.add_argument('--no-ascii', action='store_true', help='Do not use ASCII dump (-A) when not writing pcap')
    parser.add_argument('--push-only', action='store_true', help='Only capture TCP PUSH packets (likely application data)')
    args = parser.parse_args()

    tcpdump_path = find_tcpdump()
    if not tcpdump_path:
        print('[SNIFFER] tcpdump no encontrado en la imagen. Instala tcpdump o monta el binario.', file=sys.stderr)
        sys.exit(2)

    ascii_output = not args.no_ascii
    cmd = build_cmd(args.interface, args.host, args.port, args.pcap, ascii_output, args.push_only)

    print('[SNIFFER] Ejecutando:', ' '.join(cmd))
    print('[SNIFFER] Consejo: en Docker usa --cap-add=NET_RAW --cap-add=NET_ADMIN o --network host')

    try:
        proc = subprocess.Popen(cmd)
    except PermissionError:
        print('[SNIFFER] Permiso denegado al ejecutar tcpdump. Necesitas capacidades NET_RAW/NET_ADMIN o ejecutar como root.', file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print('[SNIFFER] Error al lanzar tcpdump:', e, file=sys.stderr)
        sys.exit(4)

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()


if __name__ == '__main__':
    main()
