from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

REQUIRED = ("order_id","style_id","supplier_id","order_date","receive_date","delivery_date",
            "quantity","unit_price","total_amount","currency","status")

def normalize_order(row, allowed_currencies=("INR",), allowed_statuses=(), require_order_date_before_receive=True):
    missing = [k for k in REQUIRED if k not in row or row[k] is None or str(row[k]).strip() == ""]
    if missing: return None, [f"Missing required field(s): {', '.join(missing)}"]
    errors = []
    order_id, style_id, supplier_id = (str(row[k]).strip() for k in ("order_id","style_id","supplier_id"))
    try:
        quantity = int(row["quantity"])
        if quantity <= 0: errors.append("quantity must be greater than zero")
    except (ValueError, TypeError):
        quantity = 0; errors.append("quantity must be an integer")
    try:
        price = Decimal(str(row["unit_price"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if price <= 0: errors.append("unit_price must be greater than zero")
    except (InvalidOperation, ValueError):
        price = Decimal("0.00"); errors.append("unit_price must be a valid decimal")
    try:
        total = Decimal(str(row["total_amount"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if total <= 0: errors.append("total_amount must be greater than zero")
    except (InvalidOperation, ValueError):
        total = Decimal("0.00"); errors.append("total_amount must be a valid decimal")
    if quantity > 0 and price > 0 and total != (Decimal(quantity)*price).quantize(Decimal("0.01")):
        errors.append("total_amount must equal quantity * unit_price")
    try:
        od, rd, dd = (date.fromisoformat(str(row[k])[:10]) for k in ("order_date","receive_date","delivery_date"))
        if require_order_date_before_receive and od > rd: errors.append("order_date must be on or before receive_date")
        if rd > dd: errors.append("receive_date must be on or before delivery_date")
    except (ValueError, TypeError):
        od = rd = dd = None; errors.append("dates must be ISO dates (YYYY-MM-DD)")
    currency, status = str(row["currency"]).strip().upper(), str(row["status"]).strip().upper()
    if currency not in allowed_currencies: errors.append(f"currency '{currency}' is not allowed")
    if allowed_statuses and status not in allowed_statuses: errors.append(f"status '{status}' is not allowed")
    if not order_id or not style_id or not supplier_id: errors.append("order_id, style_id and supplier_id cannot be empty")
    if errors: return None, errors
    return {"order_id":order_id,"style_id":style_id,"supplier_id":supplier_id,"order_date":od,
            "receive_date":rd,"delivery_date":dd,"quantity":quantity,"unit_price":price,
            "total_amount":total,"currency":currency,"status":status}, []
