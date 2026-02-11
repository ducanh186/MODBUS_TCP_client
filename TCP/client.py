import argparse
from pymodbus.client import ModbusTcpClient
from device import DeviceModel
# entry point CLI

def main():
    ap = argparse.ArgumentParser() # Take the arrguments from the command line
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=15020)
    ap.add_argument("--device-id", type=int, default=1)
    ap.add_argument("--set-kw", type=float, required=True, help="Write demand_control_power (kW) to HR0")
    args = ap.parse_args()
    # Connect to Modbus TCP server
    client = ModbusTcpClient(args.host, port=args.port)
    if not client.connect():
        raise RuntimeError("Cannot connect to server")

    try:
        # FC06: write single Holding Register (HR0)
        value_u16 = DeviceModel.encode_power_kw(args.set_kw)
        wr = client.write_register(DeviceModel.HR0_ADDRESS, value_u16, device_id=args.device_id)
        if wr.isError():
            raise RuntimeError(f"Write failed: {wr}")

        # FC03: read Holding Registers (HR0..HR1)
        rr = client.read_holding_registers(DeviceModel.HR0_ADDRESS, count=2, device_id=args.device_id)
        if rr.isError():
            raise RuntimeError(f"Read failed: {rr}")
        # Decode readback values and print
        hr0_u16, hr1_u16 = rr.registers[0], rr.registers[1]
        print("Readback:")
        print(f"  HR0 demand_control_power = {DeviceModel.decode_power_kw(hr0_u16)} kW")
        print(f"  HR1 active_power         = {DeviceModel.decode_power_kw(hr1_u16)} kW (Phase 1 => expect 0)")

    finally:
        client.close() # Close connection


if __name__ == "__main__":
    main()
