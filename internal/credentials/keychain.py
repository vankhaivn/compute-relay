"""Fixed private Keychain bridge. Request/secret stdin only; no diagnostic text."""
import base64
import ctypes as c
import hmac
import json
import re
import sys

MAX_BYTES = 65536
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


def main():
    if sys.platform != "darwin":
        return "unsupported", None
    raw = sys.stdin.buffer.read(2 * MAX_BYTES + 1)
    if len(raw) > 2 * MAX_BYTES:
        return "unavailable", None
    request = json.loads(raw)
    if set(request) != {"operation", "installation", "key", "secret"}:
        return "unavailable", None
    operation = request["operation"]
    if operation not in ("create", "read", "delete"):
        return "unavailable", None
    if not all(isinstance(request[key], str) and ID.fullmatch(request[key]) for key in ("installation", "key")):
        return "unavailable", None
    value = base64.b64decode(request["secret"], validate=True) if request["secret"] is not None else b""
    if operation == "create" and not 0 < len(value) <= MAX_BYTES or operation != "create" and value:
        return "unavailable", None
    cf = c.CDLL("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation")
    sec = c.CDLL("/System/Library/Frameworks/Security.framework/Security")
    ptr = c.c_void_p
    cf.CFStringCreateWithCString.argtypes = [ptr, c.c_char_p, c.c_uint32]
    cf.CFStringCreateWithCString.restype = ptr
    cf.CFDataCreate.argtypes = [ptr, ptr, c.c_long]
    cf.CFDataCreate.restype = ptr
    cf.CFDataGetLength.argtypes = [ptr]
    cf.CFDataGetLength.restype = c.c_long
    cf.CFDataGetBytePtr.argtypes = [ptr]
    cf.CFDataGetBytePtr.restype = ptr
    cf.CFGetTypeID.argtypes = [ptr]
    cf.CFGetTypeID.restype = c.c_ulong
    cf.CFDataGetTypeID.restype = c.c_ulong
    cf.CFDictionaryCreateMutable.argtypes = [ptr, c.c_long, ptr, ptr]
    cf.CFDictionaryCreateMutable.restype = ptr
    cf.CFDictionarySetValue.argtypes = [ptr, ptr, ptr]
    cf.CFDictionaryRemoveValue.argtypes = [ptr, ptr]
    cf.CFRelease.argtypes = [ptr]
    sec.SecItemCopyMatching.argtypes = [ptr, c.POINTER(ptr)]
    sec.SecItemCopyMatching.restype = c.c_int32
    sec.SecItemAdd.argtypes = [ptr, c.POINTER(ptr)]
    sec.SecItemAdd.restype = c.c_int32
    sec.SecItemDelete.argtypes = [ptr]
    sec.SecItemDelete.restype = c.c_int32
    sec.SecKeychainSetUserInteractionAllowed.argtypes = [c.c_bool]
    sec.SecKeychainSetUserInteractionAllowed.restype = c.c_int32
    # Fail without a dialog if a locked store or access control requires interaction.
    if sec.SecKeychainSetUserInteractionAllowed(False) != 0:
        return "unavailable", None
    owned = []

    def own(value):
        if not value:
            raise RuntimeError()
        owned.append(value)
        return value

    def constant(name):
        return ptr.in_dll(sec, name).value

    def text(value):
        return own(cf.CFStringCreateWithCString(None, value.encode("utf-8"), 0x08000100))

    def put(query, name, value):
        cf.CFDictionarySetValue(query, constant(name), value)

    # Null callbacks: all created values are explicitly retained until the query ends.
    query = own(cf.CFDictionaryCreateMutable(None, 0, None, None))
    try:
        put(query, "kSecClass", constant("kSecClassGenericPassword"))
        put(query, "kSecAttrService", text("org.compute-relay.vault." + request["installation"]))
        put(query, "kSecAttrAccount", text(request["key"]))
        if operation == "delete":
            status = sec.SecItemDelete(query)
            return ("ok" if status in (0, -25300) else "unavailable"), None
        if operation == "create":
            data = own(cf.CFDataCreate(None, c.c_char_p(value), len(value)))
            put(query, "kSecValueData", data)
            status = sec.SecItemAdd(query, None)
            if status == 0:
                return "ok", None
            if status != -25299:  # Duplicate item: compare only; never overwrite.
                return "unavailable", None
            cf.CFDictionaryRemoveValue(query, constant("kSecValueData"))
        put(query, "kSecReturnData", ptr.in_dll(cf, "kCFBooleanTrue").value)
        put(query, "kSecMatchLimit", constant("kSecMatchLimitOne"))
        result = ptr()
        status = sec.SecItemCopyMatching(query, c.byref(result))
        if status == -25300:
            return "missing", None
        if status != 0 or not result.value:
            return "unavailable", None
        own(result.value)
        if cf.CFGetTypeID(result) != cf.CFDataGetTypeID():
            return "unavailable", None
        size = cf.CFDataGetLength(result)
        if not 0 < size <= MAX_BYTES:
            return "unavailable", None
        stored = c.string_at(cf.CFDataGetBytePtr(result), size)
        if operation == "create":
            return ("ok" if hmac.compare_digest(value, stored) else "conflict"), None
        return "ok", base64.b64encode(stored).decode("ascii")
    finally:
        for value in reversed(owned):
            cf.CFRelease(value)


if __name__ == "__main__":
    try:
        status, secret = main()
    except BaseException:
        status, secret = "unavailable", None
    # Deliberately no exception text, traceback, input echo or framework diagnostics.
    sys.stdout.write(json.dumps({"status": status, "secret": secret}, separators=(",", ":")))
