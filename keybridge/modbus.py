from pymodbus.client import ModbusTcpClient

# --- FD-R (via MP-FEN1) register map ---
REG_FLOW_U32 = 7142
REG_TOTAL_U32 = 7144
REG_TEMP_I16 = 7146
REG_STAB_U16 = 7154
UNIT_ID = 1

# --- MP-FEN1 diagnostic/status registers (if enabled by your mapping) ---
REG_MODULE_STATUS = 6000
REG_DATA_STATUS = 6001
REG_DIAG_INFO = 6019
REG_EVENT0_CODE = 6021


def _read_holding(client: ModbusTcpClient, address: int, count: int):
    """
    pymodbus changed kwarg naming over versions (unit vs slave).
    This helper tries both so your code is robust across installs.
    """
    try:
        return client.read_holding_registers(address, count, slave=UNIT_ID)
    except TypeError:
        return client.read_holding_registers(address, count, unit=UNIT_ID)


def read_u32(client: ModbusTcpClient, address: int) -> int:
    rr = _read_holding(client, address, 2)
    if rr.isError():
        raise RuntimeError(f"Modbus U32 read error at {address}: {rr}")
    return (rr.registers[0] << 16) | rr.registers[1]


def read_i16(client: ModbusTcpClient, address: int) -> int:
    rr = _read_holding(client, address, 1)
    if rr.isError():
        raise RuntimeError(f"Modbus I16 read error at {address}: {rr}")
    val = rr.registers[0]
    return val - 65536 if val >= 32768 else val


def read_u16(client: ModbusTcpClient, address: int) -> int:
    rr = _read_holding(client, address, 1)
    if rr.isError():
        raise RuntimeError(f"Modbus U16 read error at {address}: {rr}")
    return rr.registers[0]


def read_meter(ip: str, port: int = 502, timeout: float = 1.0) -> dict:
    """
    Low-level read for one FD-R meter via MP-FEN1 Modbus TCP.

    Returns dict with:
      ok, flow_raw, total_raw, temp_c, stability,
      module_status, data_status, diag_info, event0_code,
      data_valid (best-effort)
    """
    client = ModbusTcpClient(ip, port=port, timeout=timeout)

    if not client.connect():
        return {"ok": False, "error": f"Modbus connect failed ({ip}:{port})"}

    try:
        flow_raw = read_u32(client, REG_FLOW_U32)
        total_raw = read_u32(client, REG_TOTAL_U32)
        temp_raw = read_i16(client, REG_TEMP_I16)
        temp_c = temp_raw / 10.0
        stability = read_u16(client, REG_STAB_U16)

        # Optional: status/diagnostics (may still be valid even if 0)
        module_status = read_u16(client, REG_MODULE_STATUS)
        data_status = read_u16(client, REG_DATA_STATUS)
        diag_info = read_u16(client, REG_DIAG_INFO)
        event0_code = read_u16(client, REG_EVENT0_CODE)

        # Best-effort data validity:
        # If you know the meaning of data_status bits, update this logic later.
        # For now: treat non-error reads as valid.
        data_valid = True

        return {
            "ok": True,
            "flow_raw": flow_raw,
            "total_raw": total_raw,
            "temp_raw": temp_raw,
            "temp_c": temp_c,
            "stability": stability,
            "module_status": module_status,
            "data_status": data_status,
            "diag_info": diag_info,
            "event0_code": event0_code,
            "data_valid": data_valid,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}

    finally:
        try:
            client.close()
        except Exception:
            pass


def read_fd_r_via_mpfen1(ip: str, port: int = 502, timeout: float = 1.0) -> dict:
    """
    Compatibility wrapper expected by polling.py
    """
    return read_meter(ip=ip, port=port, timeout=timeout)
