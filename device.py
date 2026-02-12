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

    # --- Unsigned scaled uint16 helpers (SOC, SOH, capacity) ---

    @staticmethod
    def encode_scaled_uint16(value: float, scale: float,
                             min_val: float = 0.0, max_val: float = 6553.5) -> int:
        """Encode float to unsigned uint16 with scale and clamp."""
        clamped = max(min_val, min(max_val, value))
        raw = int(round(clamped / scale))
        return raw & 0xFFFF

    @staticmethod
    def decode_scaled_uint16(reg_u16: int, scale: float) -> float:
        """Decode unsigned uint16 register to float."""
        return (reg_u16 & 0xFFFF) * scale

    @classmethod
    def encode_soc(cls, percent: float) -> int:
        """SOC (0-100%) -> uint16, scale=1."""
        return cls.encode_scaled_uint16(percent, scale=1.0, min_val=0.0, max_val=100.0)

    @classmethod
    def decode_soc(cls, reg_u16: int) -> float:
        """uint16 -> SOC (%)."""
        return cls.decode_scaled_uint16(reg_u16, scale=1.0)

    @classmethod
    def encode_soh(cls, percent: float) -> int:
        """SOH (0-100%) -> uint16, scale=1."""
        return cls.encode_scaled_uint16(percent, scale=1.0, min_val=0.0, max_val=100.0)

    @classmethod
    def decode_soh(cls, reg_u16: int) -> float:
        """uint16 -> SOH (%)."""
        return cls.decode_scaled_uint16(reg_u16, scale=1.0)

    @classmethod
    def encode_capacity_kwh(cls, kwh: float) -> int:
        """Capacity (kWh) -> uint16, scale=0.1, clamp >= 0."""
        return cls.encode_scaled_uint16(kwh, scale=0.1, min_val=0.0, max_val=6553.5)

    @classmethod
    def decode_capacity_kwh(cls, reg_u16: int) -> float:
        """uint16 -> capacity (kWh)."""
        return cls.decode_scaled_uint16(reg_u16, scale=0.1)