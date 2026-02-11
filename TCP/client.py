import argparse
from pymodbus.client import ModbusTcpClient
from device import DeviceModel
# entry point CLI

def main():
    ap = argparse.ArgumentParser() # Take the arrguments from the command line
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=15020)
    ap.add_argument("--device-id", type=int, default=1)
    ap.add_argument("--addr", type=int, default=0, help="Register address (0-based)")
    ap.add_argument("--count", type=int, default=2, help="Number of registers to read")
    ap.add_argument("--set-kw", type=float, default=None, help="Write power (kW) to register at --addr")
    args = ap.parse_args()

    client = ModbusTcpClient(args.host, port=args.port)
    if not client.connect():
        raise RuntimeError("Cannot connect to server")

    try:
        if args.set_kw is not None:
            value_u16 = DeviceModel.encode_power_kw(args.set_kw)
            wr = client.write_register(args.addr, value_u16, device_id=args.device_id)
            if wr.isError():
                raise RuntimeError(f"Write failed: {wr}")
            print(f"Wrote HR{args.addr} = {args.set_kw:.1f} kW (device_id={args.device_id})")

        rr = client.read_holding_registers(args.addr, count=args.count, device_id=args.device_id)
        if rr.isError():
            raise RuntimeError(f"Read failed: {rr}")

        print(f"Read (device_id={args.device_id}, addr={args.addr}, count={args.count}):")
        for i, reg_u16 in enumerate(rr.registers):
            addr = args.addr + i
            value_kw = DeviceModel.decode_power_kw(reg_u16)
            print(f"  HR{addr} = {value_kw:.1f} kW")

    finally:
        client.close() # Close connection


if __name__ == "__main__":
    main()
