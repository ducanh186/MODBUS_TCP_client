/**
 * modbus_client.js — Modbus TCP client wrapper using modbus-serial.
 *
 * Provides:
 *   - connect / reconnect logic
 *   - readHoldingRegisters  (FC03)
 *   - readInputRegisters    (FC04)
 *   - writeSingleRegister   (FC06)
 *   - int16 decode helper   (two's complement for signed power values)
 *
 * All addresses are 0-based (matches simulator convention).
 */

const ModbusRTU = require("modbus-serial");

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 15020;
const CONNECT_TIMEOUT_MS = 3000;
const READ_TIMEOUT_MS = 2000;

class ModbusClient {
  constructor(host = DEFAULT_HOST, port = DEFAULT_PORT) {
    this.host = host;
    this.port = port;
    this.client = new ModbusRTU();
    this.client.setTimeout(READ_TIMEOUT_MS);
    this._connected = false;
  }

  /** Connect (or reconnect) to the Modbus TCP server. */
  async connect() {
    try {
      if (this._connected) {
        this.client.close(() => {});
      }
      await this.client.connectTCP(this.host, { port: this.port });
      this._connected = true;
      console.log(`[modbus] Connected to ${this.host}:${this.port}`);
    } catch (err) {
      this._connected = false;
      throw err;
    }
  }

  get isConnected() {
    return this._connected && this.client.isOpen;
  }

  /** Ensure connection is alive; reconnect if needed. */
  async ensureConnection() {
    if (!this.isConnected) {
      await this.connect();
    }
  }

  /**
   * Read Holding Registers (FC03).
   * @param {number} unitId  - Modbus slave/unit ID
   * @param {number} addr    - Start address (0-based)
   * @param {number} count   - Number of registers
   * @returns {number[]}     - Array of uint16 values
   */
  async readHR(unitId, addr, count) {
    this.client.setID(unitId);
    const resp = await this.client.readHoldingRegisters(addr, count);
    return resp.data;
  }

  /**
   * Read Input Registers (FC04).
   * @param {number} unitId  - Modbus slave/unit ID
   * @param {number} addr    - Start address (0-based)
   * @param {number} count   - Number of registers
   * @returns {number[]}     - Array of uint16 values
   */
  async readIR(unitId, addr, count) {
    this.client.setID(unitId);
    const resp = await this.client.readInputRegisters(addr, count);
    return resp.data;
  }

  /**
   * Write Single Register (FC06).
   * @param {number} unitId  - Modbus slave/unit ID
   * @param {number} addr    - Register address (0-based)
   * @param {number} value   - uint16 value to write
   */
  async writeHR(unitId, addr, value) {
    this.client.setID(unitId);
    await this.client.writeRegister(addr, value);
  }

  // ----- Encoding / decoding helpers -----

  /**
   * uint16 → int16 (two's complement).
   * Power registers use signed int16 to support negative (charge).
   */
  static u16ToInt16(val) {
    return val >= 0x8000 ? val - 0x10000 : val;
  }

  /** int16 → uint16 */
  static int16ToU16(val) {
    return val & 0xffff;
  }

  /**
   * Decode power register (scale 0.1 kW, signed int16).
   * @param {number} regU16  - Raw uint16 from register
   * @returns {number}       - Power in kW (float)
   */
  static decodePowerKw(regU16) {
    return ModbusClient.u16ToInt16(regU16) * 0.1;
  }

  /**
   * Encode power in kW to uint16 for writing (scale 0.1, int16).
   * @param {number} kw - Power in kW (supports negative)
   * @returns {number}  - uint16 value to write
   */
  static encodePowerKw(kw) {
    const raw = Math.round(kw / 0.1); // kW → 0.1 kW units
    if (raw < -32768 || raw > 32767) {
      throw new RangeError(`Power ${kw} kW out of int16 range after scaling`);
    }
    return ModbusClient.int16ToU16(raw);
  }

  /**
   * Decode unsigned scaled register (SOC/SOH scale=1, capacity scale=0.1).
   */
  static decodeScaled(regU16, scale) {
    return regU16 * scale;
  }
}

module.exports = ModbusClient;
