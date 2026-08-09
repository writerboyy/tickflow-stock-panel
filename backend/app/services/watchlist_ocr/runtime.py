"""截图 OCR 的进程级并发门禁。"""
import anyio

OCR_LIMITER = anyio.CapacityLimiter(2)
