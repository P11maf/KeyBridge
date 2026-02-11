from pymodbus.client import ModbusTcpClient

# --- FD-R (via MP-FEN1) register map (Holding registers) ---
REG_FLOW_U32 = 7142
REG_TOTAL_U32 = 7144
REG_TEMP_I16 = 7146
REG_STAB_U16 = 7154

# Unit/config registers
REG_FLOW_UNIT_SETTING_U16 = 7177   # 0=L/min, 1=m3/h, 2=gal/min  :contentReference[oaicite:6]{index=6}
REG_TOTAL_UNIT_SETTING_U16 = 7175  # accumulated flow unit step   :contentReference[oaicite:7]{index=7}

UNIT_ID = 1

# --- MP-FEN1 diagnostic/status registers ---
REG_MODULE_STATUS = 6000
REG_DATA_STATUS = 6001
REG_DIAG_INFO = 6019
REG_EVENT0_CODE = 6021


def _read_holding(client: ModbusTcpClient, address: int, count: int):
    """
    pymodbus kwarg changed over versions (slave vs unit).
    """
    try:
        return client.read_holding_registers(address, count, slave=UNIT_ID)
    except TypeError:
        return client.read_holding_registers(address, count, unit=UNIT_ID)


def read_u16(client: ModbusTcpClient, address: int) -> int:
    rr = _read_holding(client, address, 1)
    if rr.isError():
        raise RuntimeError(f"Modbus U16 read error at {address}: {rr}")
    return rr.registers[0]


def read_i16(client: ModbusTcpClient, address: int) -> int:
    rr = _read_holding(client, address, 1)
    if rr.isError():
        raise RuntimeError(f"Modbus I16 read error at {address}: {rr}")
    val = rr.registers[0]
    return val - 65536 if val >= 32768 else val


def _u32_from_words_hi_lo(hi: int, lo: int) -> int:
    return ((hi & 0xFFFF) << 16) | (lo & 0xFFFF)


def read_u32_auto_word_order(client: ModbusTcpClient, address: int, expected_max: int | None = None) -> tuple[int, str]:
    """
    Reads 2x16-bit holding regs and returns (value, word_order)

    Some devices use HI,LO; some use LO,HI. We auto-detect using expected_max.
    For FD-R instantaneous flow at 7142, manual states 0..99999. :contentReference[oaicite:8]{index=8}
    """
    rr = _read_holding(client, address, 2)
    if rr.isError():
        raise RuntimeError(f"Modbus U32 read error at {address}: {rr}")

    w0 = rr.registers[0]
    w1 = rr.registers[1]

    v_hi_lo = _u32_from_words_hi_lo(w0, w1)
    v_lo_hi = _u32_from_words_hi_lo(w1, w0)

    if expected_max is None:
        # default to HI,LO if we can't check
        return v_hi_lo, "hi_lo"

    # Prefer the value that fits the expected range
    hi_lo_ok = 0 <= v_hi_lo <= expected_max
    lo_hi_ok = 0 <= v_lo_hi <= expected_max

    if hi_lo_ok and not lo_hi_ok:
        return v_hi_lo, "hi_lo"
    if lo_hi_ok and not hi_lo_ok:
        return v_lo_hi, "lo_hi"

    # If both "ok" (rare), prefer HI,LO
    # If neither "ok", still return HI,LO but caller should treat as suspect
    return v_hi_lo, "hi_lo_suspect"


def _flow_unit_str(flow_unit_code: int) -> str:
    # 7177: 0=L/min, 1=m3/h, 2=gal/min :contentReference[oaicite:9]{index=9}
    return {0: "L/min", 1: "m3/h", 2: "gal/min"}.get(flow_unit_code, "unknown")


def _total_unit_step(flow_unit_code: int, total_unit_code: int) -> tuple[float, str]:
    """
    7175 encodes accumulated flow unit step. :contentReference[oaicite:10]{index=10}
    The manual also notes unit depends on flow unit setting. :contentReference[oaicite:11]{index=11}

    Returns (step_per_count, unit_string) so:
      total_value = total_raw * step_per_count
    """
    # When flow unit is m3/h, total is in m3 steps (0.1, 1, 10, 100, 1000)
    if flow_unit_code == 1:
        mapping = {
            0: (0.1, "m3"),
            1: (1.0, "m3"),
            2: (10.0, "m3"),
            3: (100.0, "m3"),
            4: (1000.0, "m3"),
        }
        return mapping.get(total_unit_code, (1.0, "m3"))

    # When flow unit is L/min or gal/min, total is typically in L or gal steps (1,10,100,1000,10000)
    # MP-FEN1 register list shows combined "L / m3" wording; we choose L for L/min and gal for gal/min. :contentReference[oaicite:12]{index=12}
    if flow_unit_code == 2:
        mapping = {
            1: (1.0, "gal"),
            2: (10.0, "gal"),
            3: (100.0, "gal"),
            4: (1000.0, "gal"),
            5: (10000.0, "gal"),
        }
        return mapping.get(total_unit_code, (1.0, "gal"))

    # default L/min
    mapping = {
        1: (1.0, "L"),
        2: (10.0, "L"),
        3: (100.0, "L"),
        4: (1000.0, "L"),
        5: (10000.0, "L"),
    }
    return mapping.get(total_unit_code, (1.0, "L"))


def read_meter(ip: str, port: int = 502, timeout: float = 1.0) -> dict:
    """
    Low-level read for one FD-R meter via MP-FEN1 Modbus TCP.
    Returns:
      ok,
      flow_raw, total_raw (decoded ints),
      flow_value, flow_unit,
      total_value, total_unit,
      temp_c, stability,
      module_status, data_status, diag_info, event0_code,
      data_valid,
      debug_word_order
    """
    client = ModbusTcpClient(ip, port=port, timeout=timeout)

    if not client.connect():
        return {"ok": False, "error": f"Modbus connect failed ({ip}:{port})"}

    try:
        # Read unit settings first (so we can label values)
        flow_unit_code = read_u16(client, REG_FLOW_UNIT_SETTING_U16)
        total_unit_code = read_u16(client, REG_TOTAL_UNIT_SETTING_U16)

        # Instantaneous flow should be 0..99999 per MP-FEN1 register list :contentReference[oaicite:13]{index=13}
        flow_raw, flow_word_order = read_u32_auto_word_order(client, REG_FLOW_U32, expected_max=99999)

        # Total can be large; use the SAME word order as flow if possible.
        # Read total both ways and choose whichever matches the flow word order.
        rr_total = _read_holding(client, REG_TOTAL_U32, 2)
        if rr_total.isError():
            raise RuntimeError(f"Modbus U32 read error at {REG_TOTAL_U32}: {rr_total}")

        tw0, tw1 = rr_total.registers[0], rr_total.registers[1]
        total_hi_lo = _u32_from_words_hi_lo(tw0, tw1)
        total_lo_hi = _u32_from_words_hi_lo(tw1, tw0)
        total_raw = total_lo_hi if flow_word_order.startswith("lo_hi") else total_hi_lo

        temp_raw = read_i16(client, REG_TEMP_I16)
        temp_c = temp_raw / 10.0
        stability = read_u16(client, REG_STAB_U16)

        module_status = read_u16(client, REG_MODULE_STATUS)
        data_status = read_u16(client, REG_DATA_STATUS)
        diag_info = read_u16(client, REG_DIAG_INFO)
        event0_code = read_u16(client, REG_EVENT0_CODE)

        flow_unit = _flow_unit_str(flow_unit_code)
        step, total_unit = _total_unit_step(flow_unit_code, total_unit_code)
        total_value = (total_raw * step) if total_raw is not None else None

        # Note: flow_raw is already "in the selected flow unit" per unit setting; if display resolution is 0.1,
        # we can add that later once we confirm the resolution source for your setup.
        flow_value = float(flow_raw) if flow_raw is not None else None

        data_valid = True

        return {
            "ok": True,
            "flow_raw": flow_raw,
            "total_raw": total_raw,
            "flow_value": flow_value,
            "flow_unit": flow_unit,
            "total_value": total_value,
            "total_unit": total_unit,
            "temp_raw": temp_raw,
            "temp_c": temp_c,
            "stability": stability,
            "module_status": module_status,
            "data_status": data_status,
            "diag_info": diag_info,
            "event0_code": event0_code,
            "data_valid": data_valid,
            "debug_word_order": flow_word_order,
            "debug_flow_unit_code": flow_unit_code,
            "debug_total_unit_code": total_unit_code,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}

    finally:
        try:
            client.close()
        except Exception:
            pass


def read_fd_r_via_mpfen1(ip: str, port: int = 502, timeout: float = 1.0) -> dict:
    return read_meter(ip=ip, port=port, timeout=timeout)
