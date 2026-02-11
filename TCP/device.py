"""
Define a device model have:
    HR addresses for power measurement
    Convention that 0.1 kW per LSB
    encode: float kW -> int16 register value -> u16 register value
    decode: u16 register value -> int16 register value -> float kW
"""
class DeviceModel:
    HR0_ADDRESS = 0
    HR1_ADDRESS = 1

    POWER_SCALE = 0.1  # 0.1 kW / LSB

    @staticmethod
    def _int16_to_u16(x: int) -> int:
        return x & 0xFFFF

    @staticmethod
    def _u16_to_int16(x: int) -> int:
        return x - 0x10000 if x >= 0x8000 else x

    @classmethod
    def encode_power_kw(cls, kw: float) -> int:
        raw = int(round(kw / cls.POWER_SCALE))  # kW -> 0.1kW units
        if raw < -32768 or raw > 32767:
            raise ValueError("Power out of int16 range after scaling")
        return cls._int16_to_u16(raw)

    @classmethod
    def decode_power_kw(cls, reg_u16: int) -> float:
        raw = cls._u16_to_int16(reg_u16 & 0xFFFF)
        return raw * cls.POWER_SCALE