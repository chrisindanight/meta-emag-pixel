# ============================================================
# META API HELPER (with retry logic)
# ============================================================

def send_to_meta_with_retry(events: list[dict], retry_count: int = 3) -> dict:
    """Send events to Meta CAPI with retry logic."""
    for attempt in range(retry_count):
        try:
            response = requests.post(
                META_CAPI_URL,
                json={
                    'data': events,
                    'access_token': META_CAPI_TOKEN
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get('events_received', 0) != len(events):
                log.warning(f"Meta received {result.get('events_received')} of {len(events)} events")
            
            return result
            
        except requests.RequestException as e:
            if attempt == retry_count - 1:
                log.error(f"Meta CAPI failed after {retry_count} attempts: {e}")
                raise
            wait = 2 ** attempt
            log.warning(f"Meta CAPI retry {attempt+1}/{retry_count} after {wait}s: {e}")
            time.sleep(wait)
# ============================================================
# EMAG API CLIENT (with rate limiting)
# ============================================================

class EmagClient:
    def __init__(self):
        self.base_url = EMAG_API_URL
        self.auth = (EMAG_USERNAME, EMAG_PASSWORD)
        self.session = requests.Session()
        self.last_request_time = 0
        self.min_interval = 3  # seconds between requests (eMAG rate limit)

    def _post(self, resource: str, action: str, data: dict, retry_count: int = 3) -> dict:
        # Rate limit enforcement
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            log.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        
        # Retry logic with exponential backoff
        for attempt in range(retry_count):
            try:
                url = f"{self.base_url}/{resource}/{action}"
                response = self.session.post(
                    url,
                    json=data,
                    auth=self.auth,
                    timeout=30
                )
                response.raise_for_status()
                result = response.json()
                
                if result.get('isError'):
                    raise Exception(f"eMAG API error: {result.get('messages', 'Unknown error')}")
                
                return result
                
            except requests.RequestException as e:
                if attempt == retry_count - 1:
                    raise
                wait = 2 ** attempt  # 1s, 2s, 4s
                log.warning(f"eMAG API retry {attempt+1}/{retry_count} after {wait}s: {e}")
                time.sleep(wait)
"""
============================================================
META CAPI SYNC — sync.py
============================================================
Module 1: eMAG Orders → Meta CAPI (cu cross-match Shopify)
Module 2: Shopify Orders → Meta CAPI
Module 3: Audience Suppression
Module 4: Anulări eMAG (rolling 14 zile)
============================================================
"""

import os
import json
import hashlib
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

# ============================================================
# CONFIGURARE
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# Credentials din GitHub Secrets / env vars
EMAG_USERNAME       = os.environ['EMAG_USERNAME']
EMAG_PASSWORD       = os.environ['EMAG_PASSWORD']
SHOPIFY_STORE_URL   = os.environ['SHOPIFY_STORE_URL']   # ex: mystore.myshopify.com
SHOPIFY_API_KEY     = os.environ['SHOPIFY_API_KEY']
SHOPIFY_API_SECRET  = os.environ['SHOPIFY_API_SECRET']
META_CAPI_TOKEN     = os.environ['META_CAPI_TOKEN']
META_DATASET_ID     = os.environ['META_DATASET_ID']

META_CAPI_URL = f"https://graph.facebook.com/v19.0/{META_DATASET_ID}/events"
EMAG_API_URL  = "https://marketplace.emag.ro/api-3"
STATE_FILE    = Path("state.json")

# ============================================================
# STATE MANAGEMENT — persist intre rulari via GitHub Artifacts
# ============================================================

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "last_emag_order_id": 0,
        "last_shopify_order_id": 0,
        "processed_emag_orders": [],   # pentru deduplication
        "processed_shopify_orders": [],
        "suppressed_phones": [],        # telefoane deja trimise la suppression
        "last_run": None
    }

def save_state(state: dict):
    # Pastreaza listele la max 10k entries pentru a nu creste prea mult
    state["processed_emag_orders"] = state["processed_emag_orders"][-5000:]
    state["processed_shopify_orders"] = state["processed_shopify_orders"][-5000:]
    state["suppressed_phones"] = state["suppressed_phones"][-10000:]
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))
    log.info("State saved.")

# ============================================================
# HASHING — SHA-256 conform Meta spec
# ============================================================

def hash_field(value: str) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    return hashlib.sha256(normalized.encode()).hexdigest()

def hash_phone(phone: str) -> str | None:
    if not phone:
        return None
    # Normalizeaza: pastreaza doar cifre, adauga prefix tara daca lipseste
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 10 and digits.startswith('0'):
        digits = '40' + digits[1:]  # 07xx -> 407xx pentru Romania
    elif len(digits) == 9:
        digits = '40' + digits      # 7xx -> 407xx
    return hash_field(digits) if len(digits) >= 9 else None

# ============================================================
# EMAG API CLIENT
# ============================================================

class EmagClient:
    def __init__(self):
        self.base_url = EMAG_API_URL
        self.auth = (EMAG_USERNAME, EMAG_PASSWORD)
        self.session = requests.Session()

    def _post(self, resource: str, action: str, data: dict) -> dict:
        url = f"{self.base_url}/{resource}/{action}"
        response = self.session.post(
            url,
            json=data,
            auth=self.auth,
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        if result.get('isError'):
            raise Exception(f"eMAG API error: {result.get('messages', 'Unknown error')}")
        return result

    def get_new_orders(self, since_id: int = 0) -> list[dict]:
        """Trage comenzile noi din eMAG, paginate."""
        orders = []
        page = 1

        while True:
            result = self._post('order', 'read', {
                'itemsPerPage': 100,
                'currentPage': page,
                'sort': {'id': 'ASC'},
                'filter': {
                    'status': 4,  # 4 = finalizata
                    # Filtrare dupa data — ultimele 24h pentru safety
                    'modifiedAfter': (datetime.now(timezone.utc) - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
                }
            })

            batch = result.get('results', [])
            if not batch:
                break

            # Filtreaza doar comenzile mai noi decat last_id
            new_orders = [o for o in batch if o.get('id', 0) > since_id]
            orders.extend(new_orders)

            # Daca am gasit comenzi mai vechi, ne oprim
            if len(new_orders) < len(batch):
                break

            page += 1
            if page > 10:  # Safeguard: max 1000 comenzi per run
                break

        log.info(f"eMAG: found {len(orders)} new orders since id={since_id}")
        return orders

    def get_orders_for_cancellation_check(self) -> list[dict]:
        """Comenzile din ultimele 14 zile pentru verificare anulari."""
        since = (datetime.now(timezone.utc) - timedelta(days=14)).strftime('%Y-%m-%d %H:%M:%S')

        result = self._post('order', 'read', {
            'itemsPerPage': 500,
            'currentPage': 1,
            'filter': {
                'modifiedAfter': since,
                'status': [1, 2, 5, 6]  # Anulate, returnate, etc.
            }
        })

        orders = result.get('results', [])
        log.info(f"eMAG: found {len(orders)} orders to check for cancellation")
        return orders

# ============================================================
# SHOPIFY API CLIENT
# ============================================================

class ShopifyClient:
    def __init__(self):
        self.base_url = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01"
        self.session = requests.Session()
        self.session.auth = (SHOPIFY_API_KEY, SHOPIFY_API_SECRET)
        self.session.headers['Content-Type'] = 'application/json'

    def get_new_orders(self, since_id: int = 0) -> list[dict]:
        """Trage comenzile Shopify noi."""
        params = {
            'limit': 250,
            'status': 'any',
            'financial_status': 'paid',
        }
        if since_id > 0:
            params['since_id'] = since_id

        response = self.session.get(
            f"{self.base_url}/orders.json",
            params=params,
            timeout=30
        )
        response.raise_for_status()
        orders = response.json().get('orders', [])
        log.info(f"Shopify: found {len(orders)} new orders since id={since_id}")
        return orders

    def find_customer_by_phone(self, phone: str) -> dict | None:
        """Cauta clientul in Shopify dupa telefon pentru cross-match email."""
        if not phone:
            return None

        # Normalizeaza telefonul pentru cautare
        digits = ''.join(filter(str.isdigit, phone))
        search_variants = [phone, digits]
        if digits.startswith('40') and len(digits) > 10:
            search_variants.append('0' + digits[2:])

        for variant in search_variants:
            try:
                response = self.session.get(
                    f"{self.base_url}/customers/search.json",
                    params={'query': f'phone:{variant}', 'limit': 1},
                    timeout=10
                )
                if response.ok:
                    customers = response.json().get('customers', [])
                    if customers:
                        log.info(f"Cross-match found: phone {variant[:6]}*** -> customer {customers[0]['id']}")
                        return customers[0]
            except Exception as e:
                log.warning(f"Shopify customer search error: {e}")

        return None

    def get_customer_order_count(self, customer_id: int) -> int:
        """Numarul de comenzi al clientului — pentru LTV segmentation."""
        try:
            response = self.session.get(
                f"{self.base_url}/customers/{customer_id}.json",
                timeout=10
            )
            if response.ok:
                customer = response.json().get('customer', {})
                return customer.get('orders_count', 0)
        except Exception:
            pass
        return 0

# ============================================================
# META CAPI CLIENT
# ============================================================

class MetaCAPIClient:
    def __init__(self):
        self.url = META_CAPI_URL
        self.token = META_CAPI_TOKEN
        self.session = requests.Session()

    def send_events(self, events: list[dict]) -> dict:
        if not events:
            return {}

        payload = {
            'data': events,
            'access_token': self.token
        }

        response = self.session.post(
            self.url,
            json=payload,
            timeout=30
        )

        result = response.json()

        if not response.ok:
            log.error(f"Meta CAPI error: {result}")
            raise Exception(f"Meta CAPI returned {response.status_code}: {result}")

        log.info(f"Meta CAPI: sent {len(events)} events | Response: {result}")
        return result

    def send_batch(self, events: list[dict], batch_size: int = 50) -> int:
        """Trimite events in batches de max 50 (limita Meta)."""
        total_sent = 0
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            try:
                self.send_events(batch)
                total_sent += len(batch)
                if len(events) > batch_size:
                    time.sleep(0.5)  # Rate limiting
            except Exception as e:
                log.error(f"Batch send failed: {e}")
        return total_sent

# ============================================================
# MODULE 1: eMAG ORDERS → META CAPI
# ============================================================

def sync_emag_orders(emag: EmagClient, shopify: ShopifyClient, meta: MetaCAPIClient, state: dict) -> int:
    log.info("=== MODULE 1: eMAG Orders Sync ===")

    orders = emag.get_new_orders(since_id=state['last_emag_order_id'])
    if not orders:
        return 0

    events = []
    max_id = state['last_emag_order_id']

    for order in orders:
        order_id = order.get('id')

        # Deduplication
        if str(order_id) in state['processed_emag_orders']:
            continue

        order_id_str = str(order_id)
        max_id = max(max_id, order_id)

        # Extrage datele comenzii
        customer = order.get('customer', {})
        phone = customer.get('phone_1') or customer.get('phone_2') or ''
        phone_hash = hash_phone(phone)

        if not phone_hash:
            log.warning(f"eMAG order {order_id}: no valid phone, skipping")
            continue

        # USER DATA — incepe cu telefonul
        user_data = {'ph': phone_hash}

        # === CROSS-MATCH: cauta emailul in Shopify ===
        shopify_customer = shopify.find_customer_by_phone(phone)
        email_hash = None
        is_repeat_customer = False
        lifetime_value = 0.0

        if shopify_customer:
            email = shopify_customer.get('email', '')
            email_hash = hash_field(email)
            if email_hash:
                user_data['em'] = email_hash

            # LTV din Shopify
            is_repeat_customer = shopify_customer.get('orders_count', 0) > 1
            lifetime_value = float(shopify_customer.get('total_spent', 0) or 0)

            # Nume
            fn_hash = hash_field(shopify_customer.get('first_name', ''))
            ln_hash = hash_field(shopify_customer.get('last_name', ''))
            if fn_hash: user_data['fn'] = fn_hash
            if ln_hash: user_data['ln'] = ln_hash

        # Produse din comanda
        products = order.get('products', [])
        content_ids = [str(p.get('product_id', p.get('id', ''))) for p in products]
        contents = [{
            'id': str(p.get('product_id', p.get('id', ''))),
            'quantity': p.get('quantity', 1),
            'item_price': float(p.get('sale_price', p.get('price', 0)) or 0)
        } for p in products]

        total_value = float(order.get('total', order.get('grand_total', 0)) or 0)

        # Determina tipul evenimentului — LTV segmentation
        event_name = 'Purchase'
        supplement_segment = 'repeat_customer' if is_repeat_customer else 'first_purchase'

        event = {
            'event_name': event_name,
            'event_time': int(time.time()),
            'event_id': f"emag_{order_id}",
            # action_source = 'other' pentru comenzi care nu vin direct de pe site
            'action_source': 'other',
            'user_data': user_data,
            'custom_data': {
                'value': total_value,
                'currency': 'RON',
                'order_id': order_id_str,
                'content_type': 'product',
                'content_ids': content_ids,
                'contents': contents,
                'num_items': sum(p.get('quantity', 1) for p in products),
                # Date extra pentru algoritmul Meta
                'channel': 'emag_marketplace',
                'supplement_segment': supplement_segment,
                'customer_lifetime_value': lifetime_value,
                'has_email_match': bool(email_hash),
                'is_repeat_customer': is_repeat_customer
            }
        }

        events.append(event)
        state['processed_emag_orders'].append(order_id_str)

        emq = calculate_emq(user_data)
        log.info(f"eMAG order {order_id}: value={total_value} RON | EMQ={emq} | email_match={bool(email_hash)} | segment={supplement_segment}")

    # Trimite la Meta
    sent = meta.send_batch(events)

    # Actualizeaza state
    if max_id > state['last_emag_order_id']:
        state['last_emag_order_id'] = max_id

    log.info(f"Module 1 complete: {sent}/{len(events)} events sent to Meta")
    return sent

# ============================================================
# MODULE 2: SHOPIFY ORDERS → META CAPI
# ============================================================

def sync_shopify_orders(shopify: ShopifyClient, meta: MetaCAPIClient, state: dict) -> int:
    log.info("=== MODULE 2: Shopify Orders Sync ===")

    orders = shopify.get_new_orders(since_id=state['last_shopify_order_id'])
    if not orders:
        return 0

    events = []
    max_id = state['last_shopify_order_id']

    for order in orders:
        order_id = str(order['id'])
        max_id = max(max_id, order['id'])

        if order_id in state['processed_shopify_orders']:
            continue

        # Extrage date client
        customer = order.get('customer', {})
        email = order.get('email') or customer.get('email', '')
        phone = order.get('phone') or customer.get('phone', '')

        email_hash = hash_field(email)
        phone_hash = hash_phone(phone)

        if not email_hash and not phone_hash:
            log.warning(f"Shopify order {order_id}: no email or phone, skipping")
            continue

        user_data = {}
        if email_hash: user_data['em'] = email_hash
        if phone_hash: user_data['ph'] = phone_hash

        # Nume si adresa
        shipping = order.get('shipping_address', {})
        fn_hash = hash_field(customer.get('first_name') or shipping.get('first_name', ''))
        ln_hash = hash_field(customer.get('last_name') or shipping.get('last_name', ''))
        city_hash = hash_field(shipping.get('city', ''))
        zip_hash = hash_field(shipping.get('zip', ''))

        if fn_hash: user_data['fn'] = fn_hash
        if ln_hash: user_data['ln'] = ln_hash
        if city_hash: user_data['ct'] = city_hash
        if zip_hash: user_data['zp'] = zip_hash

        country = shipping.get('country_code', 'RO').lower()
        user_data['country'] = country

        # FBP/FBC din note attributes (stocate de pixel la checkout)
        note_attrs = {a['name']: a['value'] for a in order.get('note_attributes', [])}
        if note_attrs.get('_fbp'): user_data['fbp'] = note_attrs['_fbp']
        if note_attrs.get('_fbc'): user_data['fbc'] = note_attrs['_fbc']

        # Produse
        line_items = order.get('line_items', [])
        content_ids = [str(item.get('product_id', '')) for item in line_items]
        contents = [{
            'id': str(item.get('product_id', '')),
            'quantity': item['quantity'],
            'item_price': float(item['price'])
        } for item in line_items]

        total_value = float(order.get('total_price', 0))

        # LTV segmentation
        orders_count = customer.get('orders_count', 1)
        total_spent = float(customer.get('total_spent', 0) or 0)
        is_first = orders_count == 1
        segment = 'first_purchase' if is_first else 'repeat_customer'

        event = {
            'event_name': 'Purchase',
            'event_time': int(time.mktime(
                datetime.strptime(order['created_at'][:19], '%Y-%m-%dT%H:%M:%S').timetuple()
            )),
            'event_id': f"shopify_{order_id}",
            'event_source_url': f"https://{SHOPIFY_STORE_URL}/checkout/order-received/{order_id}",
            'action_source': 'website',
            'user_data': user_data,
            'custom_data': {
                'value': total_value,
                'currency': order.get('currency', 'RON'),
                'order_id': order_id,
                'content_type': 'product',
                'content_ids': content_ids,
                'contents': contents,
                'num_items': sum(i['quantity'] for i in line_items),
                'channel': 'shopify',
                'supplement_segment': segment,
                'customer_lifetime_value': total_spent,
                'is_first_purchase': is_first,
                'orders_count': orders_count
            }
        }

        events.append(event)
        state['processed_shopify_orders'].append(order_id)

        emq = calculate_emq(user_data)
        log.info(f"Shopify order {order_id}: value={total_value} RON | EMQ={emq} | segment={segment}")

    sent = meta.send_batch(events)

    if max_id > state['last_shopify_order_id']:
        state['last_shopify_order_id'] = max_id

    log.info(f"Module 2 complete: {sent}/{len(events)} events sent to Meta")
    return sent

# ============================================================
# MODULE 3: AUDIENCE SUPPRESSION
# ============================================================

def sync_suppression(meta: MetaCAPIClient, state: dict, new_phones: list[str], new_emails: list[str]) -> int:
    log.info("=== MODULE 3: Audience Suppression ===")

    events = []

    for phone in new_phones:
        phone_hash = hash_phone(phone)
        if not phone_hash or phone_hash in state['suppressed_phones']:
            continue

        event = {
            'event_name': 'ExistingCustomer',
            'event_time': int(time.time()),
            'event_id': f"suppress_ph_{phone_hash[:8]}_{int(time.time())}",
            'action_source': 'other',
            'user_data': {'ph': phone_hash},
            'custom_data': {
                'suppression_reason': 'existing_customer',
                'channel': 'combined'  # eMAG + Shopify
            }
        }
        events.append(event)
        state['suppressed_phones'].append(phone_hash)

    if not events:
        log.info("Module 3: no new customers to suppress")
        return 0

    sent = meta.send_batch(events)
    log.info(f"Module 3 complete: {sent} suppression events sent")
    return sent

# ============================================================
# MODULE 4: ANULĂRI eMAG
# ============================================================

def sync_cancellations(emag: EmagClient, meta: MetaCAPIClient, state: dict) -> int:
    log.info("=== MODULE 4: eMAG Cancellations Check ===")

    # Statusuri eMAG care reprezinta anulare/retur:
    # 1=Noua, 2=In procesare, 5=Anulata, 6=Returnata, etc.
    CANCELLED_STATUSES = {5, 6, 7}

    cancelled_orders = emag.get_orders_for_cancellation_check()

    events = []
    for order in cancelled_orders:
        order_id = str(order.get('id'))
        status = order.get('status')

        if status not in CANCELLED_STATUSES:
            continue

        # Trimitem doar daca am trimis initial un Purchase pentru aceasta comanda
        if order_id not in state['processed_emag_orders']:
            continue

        customer = order.get('customer', {})
        phone = customer.get('phone_1') or customer.get('phone_2') or ''
        phone_hash = hash_phone(phone)

        if not phone_hash:
            continue

        total_value = float(order.get('total', 0) or 0)

        event = {
            'event_name': 'RefundOrCancellation',  # Custom event
            'event_time': int(time.time()),
            'event_id': f"cancel_emag_{order_id}",
            'action_source': 'other',
            'user_data': {'ph': phone_hash},
            'custom_data': {
                'value': total_value,
                'currency': 'RON',
                'order_id': order_id,
                'cancellation_status': status,
                'channel': 'emag_marketplace'
            }
        }
        events.append(event)
        log.info(f"eMAG cancellation: order {order_id} | status={status} | value={total_value} RON")

    if not events:
        log.info("Module 4: no cancellations found")
        return 0

    sent = meta.send_batch(events)
    log.info(f"Module 4 complete: {sent} cancellation events sent")
    return sent

# ============================================================
# HELPER
# ============================================================

def calculate_emq(user_data: dict) -> str:
    weights = {'em': 2.5, 'ph': 2.0, 'fbc': 1.5, 'fbp': 1.0,
               'fn': 0.5, 'ln': 0.5, 'country': 0.3, 'ct': 0.2, 'zp': 0.2}
    score = sum(w for k, w in weights.items() if user_data.get(k))
    return f"{min(10, score):.1f}"

# ============================================================
# MAIN
# ============================================================

def main():
    log.info("========================================")
    log.info("Meta CAPI Sync started")
    log.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log.info("========================================")

    state = load_state()

    emag    = EmagClient()
    shopify = ShopifyClient()
    meta    = MetaCAPIClient()

    total_events = 0
    new_phones = []
    new_emails = []

    try:
        # Module 1: eMAG
        emag_orders = emag.get_new_orders(since_id=state['last_emag_order_id'])
        for order in emag_orders:
            customer = order.get('customer', {})
            phone = customer.get('phone_1') or customer.get('phone_2') or ''
            if phone:
                new_phones.append(phone)

        sent1 = sync_emag_orders(emag, shopify, meta, state)
        total_events += sent1

    except Exception as e:
        log.error(f"Module 1 failed: {e}", exc_info=True)

    try:
        # Module 2: Shopify
        shopify_orders = shopify.get_new_orders(since_id=state['last_shopify_order_id'])
        for order in shopify_orders:
            phone = order.get('phone') or order.get('customer', {}).get('phone', '')
            email = order.get('email', '')
            if phone: new_phones.append(phone)
            if email: new_emails.append(email)

        sent2 = sync_shopify_orders(shopify, meta, state)
        total_events += sent2

    except Exception as e:
        log.error(f"Module 2 failed: {e}", exc_info=True)

    try:
        # Module 3: Suppression
        sent3 = sync_suppression(meta, state, new_phones, new_emails)
        total_events += sent3

    except Exception as e:
        log.error(f"Module 3 failed: {e}", exc_info=True)

    try:
        # Module 4: Anulari (ruleaza mai rar — la fiecare a 4-a rulare)
        run_count = state.get('run_count', 0) + 1
        state['run_count'] = run_count

        if run_count % 4 == 0:  # La fiecare ora (4 x 15 min)
            sent4 = sync_cancellations(emag, meta, state)
            total_events += sent4

    except Exception as e:
        log.error(f"Module 4 failed: {e}", exc_info=True)

    save_state(state)

    log.info("========================================")
    log.info(f"Sync complete. Total events sent: {total_events}")
    log.info("========================================")

if __name__ == '__main__':
    main()
