import argparse
from pymodbus.client import ModbusTcpClient
from device import DeviceModel

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=15020)
    ap.add_argument("--device-id", type=int, default=1)
    ap.add_argument("--addr", type=int, default=0, help="Register address (0-based)")
    ap.add_argument("--count", type=int, default=2, help="Number of registers to read")
    ap.add_argument("--area", choices=["hr", "ir"], default="ir", help="Register area: hr (holding) or ir (input)")
    ap.add_argument("--set-kw", type=float, default=None, help="FC06 write power (kW) to HR at --addr")
    ap.add_argument("--decode", choices=["power", "soc", "capacity", "raw"], default="power",
                    help="Decoder: power (kW signed), soc (%% unsigned), capacity (kWh sc=0.1), raw")
    args = ap.parse_args()

    client = ModbusTcpClient(args.host, port=args.port)
    if not client.connect():
        raise RuntimeError("Cannot connect to server")

    try:
        if args.set_kw is not None:
            value_u16 = DeviceModel.encode_power_kw(args.set_kw)
            wr = client.write_register(args.addr, value_u16, device_id=args.device_id)
            if wr.isError():
                raise RuntimeError(f"Write HR{args.addr} failed: {wr}")
            print(f"Wrote HR{args.addr} = {args.set_kw:.1f} kW (device_id={args.device_id})")

        if args.area == "ir":
            rr = client.read_input_registers(args.addr, count=args.count, device_id=args.device_id)
            area_label = "IR"
        else:
            rr = client.read_holding_registers(args.addr, count=args.count, device_id=args.device_id)
            area_label = "HR"

        if rr.isError():
            raise RuntimeError(f"Read {area_label} failed: {rr}")

        print(f"Read {area_label} (device_id={args.device_id}, addr={args.addr}, count={args.count}):")
        for i, reg_u16 in enumerate(rr.registers):
            addr = args.addr + i
            if args.decode == "power":
                val_str = f"{DeviceModel.decode_power_kw(reg_u16):.1f} kW"
            elif args.decode == "soc":
                val_str = f"{DeviceModel.decode_soc(reg_u16):.0f} %"
            elif args.decode == "capacity":
                val_str = f"{DeviceModel.decode_capacity_kwh(reg_u16):.1f} kWh"
            else:
                val_str = str(reg_u16)
            print(f"  {area_label}{addr} = {val_str} (raw={reg_u16})")

    finally:
        client.close()


if __name__ == "__main__":
    main()
