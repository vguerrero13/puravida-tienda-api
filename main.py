"""
API de la tienda online "Pura Vida en Línea" (simulada).

La construyó una agencia externa: por eso los nombres de campos están en inglés.
Los datos se generan hasta fin de 2026, pero la API SOLO devuelve lo que ya "ocurrió"
según la hora actual de Costa Rica: cada día aparecen pedidos nuevos y los estados
cambian (placed -> paid -> shipped -> delivered), así que las cargas incrementales
con `updated_since` tienen sentido real.

Comportamientos intencionales para el curso:
  - Autenticación con header  X-API-Key
  - Paginación (page / page_size, máximo 500) con enlace next_page
  - Límite de 60 solicitudes por minuto por API key  -> 429 + Retry-After
  - Errores aleatorios 500/503 (CHAOS_RATE, por defecto 3 %)
  - En el plan gratuito de Render el servicio "se duerme": la primera llamada tarda ~1 min

Variables de entorno:
  API_KEYS        claves válidas separadas por coma (obligatoria)
  CHAOS_RATE      probabilidad de error aleatorio (0 a 1). Default 0.03
  RATE_LIMIT      solicitudes por minuto por clave. Default 60
  SIMULATED_NOW   fecha-hora fija para pruebas, ej. 2026-10-11T08:00:00 (opcional)
"""
import gzip
import json
import os
import random
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

CR = timezone(timedelta(hours=-6))  # Costa Rica no tiene horario de verano
DATA = Path(__file__).parent / "data"
API_KEYS = {k.strip() for k in os.environ.get("API_KEYS", "demo-key-cambiar").split(",") if k.strip()}
CHAOS_RATE = float(os.environ.get("CHAOS_RATE", "0.03"))
RATE_LIMIT = int(os.environ.get("RATE_LIMIT", "60"))
MAX_PAGE = 500

app = FastAPI(
    title="Pura Vida en Línea — API de la tienda online",
    version="1.0.0",
    description="API REST de la tienda online de Pura Vida Distribución S.A. (datos ficticios para el curso "
                "*Ingeniería de Datos con Microsoft Fabric* — Grow Up Data Analytics). "
                "Todas las rutas /v1 requieren el header **X-API-Key**.",
)


# ---------------------------------------------------------------------------
# datos
# ---------------------------------------------------------------------------
def _cargar(nombre):
    with gzip.open(DATA / f"{nombre}.json.gz", "rt", encoding="utf-8") as f:
        return json.load(f)


def _dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=CR)


CUSTOMERS = _cargar("customers")
for c in CUSTOMERS:
    c["_created"] = _dt(c["created_at"])
ORDERS = _cargar("orders")
for o in ORDERS:
    o["_events"] = [(e["status"], _dt(e["at"])) for e in o["events"]]
    o["_created"] = o["_events"][0][1]
ORDERS.sort(key=lambda o: o["_created"])
REVIEWS = _cargar("reviews")
for r in REVIEWS:
    r["_created"] = _dt(r["created_at"])


def ahora():
    sim = os.environ.get("SIMULATED_NOW")
    if sim:
        return datetime.fromisoformat(sim).replace(tzinfo=CR)
    return datetime.now(CR).replace(microsecond=0)


def iso(dt):
    return dt.isoformat()


def parse_fecha(valor: Optional[str], nombre: str):
    if not valor:
        return None
    try:
        d = datetime.fromisoformat(valor.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(400, detail=f"'{nombre}' debe ser ISO 8601, ej. 2026-10-01T00:00:00-06:00")
    return d if d.tzinfo else d.replace(tzinfo=CR)


# ---------------------------------------------------------------------------
# seguridad, límite de solicitudes y caos
# ---------------------------------------------------------------------------
_ventanas = defaultdict(deque)


def seguridad(request: Request, x_api_key: Optional[str] = Header(default=None)):
    if not x_api_key or x_api_key not in API_KEYS:
        raise HTTPException(401, detail="API key inválida o ausente. Envíe el header X-API-Key.")
    t = time.time()
    v = _ventanas[x_api_key]
    while v and t - v[0] > 60:
        v.popleft()
    if len(v) >= RATE_LIMIT:
        espera = int(60 - (t - v[0])) + 1
        raise HTTPException(429, detail="Demasiadas solicitudes. Respete el límite por minuto.",
                            headers={"Retry-After": str(espera)})
    v.append(t)
    if random.random() < CHAOS_RATE:
        code = random.choice([500, 503])
        raise HTTPException(code, detail="Error temporal del servidor. Intente de nuevo.",
                            headers={"Retry-After": "5"} if code == 503 else None)
    return x_api_key


def paginar(request: Request, items, page, page_size):
    total = len(items)
    paginas = max(1, -(-total // page_size))
    ini = (page - 1) * page_size
    datos = items[ini:ini + page_size]
    sig = None
    if page < paginas:
        sig = str(request.url.include_query_params(page=page + 1, page_size=page_size))
    return {"data": datos,
            "pagination": {"page": page, "page_size": page_size, "total_records": total,
                           "total_pages": paginas, "next_page": sig},
            "as_of": iso(ahora())}


# ---------------------------------------------------------------------------
# vistas "a la fecha"
# ---------------------------------------------------------------------------
def order_as_of(o, now):
    ev = [(s, t) for s, t in o["_events"] if t <= now]
    status, updated = ev[-1]
    out = {k: v for k, v in o.items() if not k.startswith("_") and k != "events"}
    out["status"] = status
    out["created_at"] = iso(o["_created"])
    out["updated_at"] = iso(updated)
    out["status_history"] = [{"status": s, "at": iso(t)} for s, t in ev]
    return out, updated


def customer_out(c):
    out = {k: v for k, v in c.items() if not k.startswith("_")}
    out["created_at"] = iso(c["_created"])
    out["updated_at"] = iso(c["_created"])
    return out


# ---------------------------------------------------------------------------
# rutas
# ---------------------------------------------------------------------------
@app.get("/", tags=["info"])
def raiz():
    return {"servicio": "Pura Vida en Línea API", "version": "1.0.0", "documentacion": "/docs",
            "hora_servidor": iso(ahora()),
            "endpoints": ["/v1/customers", "/v1/orders", "/v1/orders/{order_id}", "/v1/reviews"]}


@app.get("/health", tags=["info"])
def health():
    return {"status": "ok", "time": iso(ahora())}


@app.get("/v1/customers", tags=["customers"])
def customers(request: Request, _=Depends(seguridad),
              updated_since: Optional[str] = Query(None, description="ISO 8601. Devuelve registros con updated_at > este valor"),
              page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=MAX_PAGE)):
    now = ahora()
    desde = parse_fecha(updated_since, "updated_since")
    items = [c for c in CUSTOMERS if c["_created"] <= now and (desde is None or c["_created"] > desde)]
    items.sort(key=lambda c: (c["_created"], c["customer_id"]))
    return _paginar_lazy(request, items, page, page_size, customer_out)


def _paginar_lazy(request, items, page, page_size, conv):
    res = paginar(request, items, page, page_size)
    res["data"] = [conv(x) for x in res["data"]]
    return res


@app.get("/v1/orders", tags=["orders"])
def orders(request: Request, _=Depends(seguridad),
           updated_since: Optional[str] = Query(None, description="ISO 8601. Pedidos creados o con cambio de estado después de esta fecha"),
           created_from: Optional[str] = Query(None, description="ISO 8601 (inclusive)"),
           created_to: Optional[str] = Query(None, description="ISO 8601 (exclusivo)"),
           status: Optional[str] = Query(None, description="placed | paid | shipped | delivered | cancelled | payment_failed"),
           page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=MAX_PAGE)):
    now = ahora()
    desde = parse_fecha(updated_since, "updated_since")
    c_ini = parse_fecha(created_from, "created_from")
    c_fin = parse_fecha(created_to, "created_to")
    sel = []
    for o in ORDERS:
        if o["_created"] > now:
            break  # ORDERS está ordenado por fecha de creación
        if c_ini and o["_created"] < c_ini:
            continue
        if c_fin and o["_created"] >= c_fin:
            continue
        out, upd = order_as_of(o, now)
        if desde and upd <= desde:
            continue
        if status and out["status"] != status:
            continue
        sel.append((upd, out["order_id"], out))
    sel.sort(key=lambda x: (x[0], x[1]))
    return paginar(request, [x[2] for x in sel], page, page_size)


@app.get("/v1/orders/{order_id}", tags=["orders"])
def order(order_id: str, _=Depends(seguridad)):
    now = ahora()
    for o in ORDERS:
        if o["order_id"] == order_id and o["_created"] <= now:
            return order_as_of(o, now)[0]
    raise HTTPException(404, detail=f"Pedido {order_id} no existe")


@app.get("/v1/reviews", tags=["reviews"])
def reviews(request: Request, _=Depends(seguridad),
            created_since: Optional[str] = Query(None, description="ISO 8601"),
            page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=MAX_PAGE)):
    now = ahora()
    desde = parse_fecha(created_since, "created_since")
    items = [r for r in REVIEWS if r["_created"] <= now and (desde is None or r["_created"] > desde)]
    items.sort(key=lambda r: (r["_created"], r["review_id"]))

    def conv(r):
        out = {k: v for k, v in r.items() if not k.startswith("_")}
        out["created_at"] = iso(r["_created"])
        return out
    return _paginar_lazy(request, items, page, page_size, conv)


@app.exception_handler(HTTPException)
async def errores(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code,
                        content={"error": {"code": exc.status_code, "message": exc.detail}},
                        headers=getattr(exc, "headers", None))
