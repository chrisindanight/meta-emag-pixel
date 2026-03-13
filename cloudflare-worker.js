/**
 * ============================================================
 * CLOUDFLARE WORKER — Meta CAPI Proxy
 * ============================================================
 * Deploy: Cloudflare Dashboard → Workers → Create Worker
 * 
 * Secrets (Settings → Variables → Encrypt):
 *   META_CAPI_TOKEN    = token din Meta Events Manager
 *   META_DATASET_ID    = ID dataset din Meta Events Manager
 *   ALLOWED_ORIGIN     = https://tudomeniu.ro
 * ============================================================
 */

export default {
  async fetch(request, env) {

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return corsResponse(null, 204, env);
    }

    const url = new URL(request.url);

    // Routing
    if (request.method === 'POST' && url.pathname === '/capi') {
      return handleCAPI(request, env);
    }

    if (request.method === 'POST' && url.pathname === '/capi/session-end') {
      return handleSessionEnd(request, env);
    }

    return new Response('Not found', { status: 404 });
  }
};

// ============================================================
// HANDLER PRINCIPAL — events de la pixel
// ============================================================

async function handleCAPI(request, env) {
  try {
    const body = await request.json();
    const { data: events } = body;

    if (!events || !Array.isArray(events) || events.length === 0) {
      return corsResponse({ error: 'Invalid payload' }, 400, env);
    }

    // Enrichment cu date Cloudflare (IP real + geo)
    const cfData = extractCloudflareData(request);

    const enrichedEvents = events.map(event => enrichEvent(event, cfData, request));

    // Trimite la Meta CAPI
    const metaResult = await sendToMeta(enrichedEvents, env);

    return corsResponse({
      success: true,
      events_received: events.length,
      emq_scores: enrichedEvents.map(e => calculateEMQ(e.user_data)),
      meta_response: metaResult
    }, 200, env);

  } catch (err) {
    console.error('CAPI handler error:', err.message);
    return corsResponse({ error: 'Internal error' }, 500, env);
  }
}

// ============================================================
// HANDLER SESSION END — sendBeacon (text/plain)
// ============================================================

async function handleSessionEnd(request, env) {
  try {
    let body;
    const contentType = request.headers.get('content-type') || '';

    if (contentType.includes('application/json')) {
      body = await request.json();
    } else {
      // sendBeacon trimite text/plain
      const text = await request.text();
      body = JSON.parse(text);
    }

    const cfData = extractCloudflareData(request);

    const event = enrichEvent({
      event_name: 'SessionEnd',
      event_time: Math.floor(Date.now() / 1000),
      event_id: `sess_end_${Date.now()}`,
      event_source_url: request.headers.get('referer') || '',
      action_source: 'website',
      user_data: {},
      custom_data: body.data || {}
    }, cfData, request);

    // Fire and forget — nu asteptam raspunsul Meta
    sendToMeta([event], env).catch(e => console.error('SessionEnd Meta error:', e.message));

    return new Response('OK', { status: 200 });

  } catch (err) {
    // sendBeacon nu citeste raspunsul — returnam mereu 200
    return new Response('OK', { status: 200 });
  }
}

// ============================================================
// ENRICHMENT — adauga date Cloudflare la fiecare event
// ============================================================

function enrichEvent(event, cfData, request) {
  const enriched = {
    ...event,
    user_data: {
      ...event.user_data,
      // IP real — cel mai important pentru EMQ, Meta il hasheza automat
      client_ip_address: cfData.ip,
      // User agent complet
      client_user_agent: request.headers.get('user-agent') || event.user_data?.client_user_agent || '',
      // Geo din Cloudflare — gratuit, fara servicii externe
      ...(cfData.country && !event.user_data?.country && { country: cfData.country.toLowerCase() }),
    }
  };

  // Asigura event_time
  if (!enriched.event_time) {
    enriched.event_time = Math.floor(Date.now() / 1000);
  }

  // Asigura event_id pentru deduplication pixel <-> CAPI
  if (!enriched.event_id) {
    enriched.event_id = `ev_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
  }

  // Asigura action_source
  enriched.action_source = enriched.action_source || 'website';

  return enriched;
}

// ============================================================
// CLOUDFLARE DATA EXTRACTION
// ============================================================

function extractCloudflareData(request) {
  return {
    // IP real al clientului — Cloudflare il pune automat in acest header
    ip: request.headers.get('CF-Connecting-IP') ||
        request.headers.get('X-Forwarded-For')?.split(',')[0]?.trim() ||
        request.headers.get('X-Real-IP') ||
        '0.0.0.0',

    // Geo gratuit din Cloudflare
    country: request.cf?.country || request.headers.get('CF-IPCountry') || null,
    city: request.cf?.city || null,
    region: request.cf?.region || null,
    timezone: request.cf?.timezone || null,
    latitude: request.cf?.latitude || null,
    longitude: request.cf?.longitude || null,

    // Calitatea conexiunii
    asn: request.cf?.asn || null,
    botScore: request.cf?.botManagement?.score || null,
  };
}

// ============================================================
// TRIMITERE LA META CAPI
// ============================================================

async function sendToMeta(events, env) {
  const url = `https://graph.facebook.com/v19.0/${env.META_DATASET_ID}/events`;

  const payload = {
    data: events,
    // Decommenteaza pentru testing:
    // test_event_code: 'TEST12345'
  };

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      ...payload,
      access_token: env.META_CAPI_TOKEN
    })
  });

  const result = await response.json();

  if (!response.ok) {
    console.error('Meta CAPI error:', JSON.stringify(result));
    throw new Error(`Meta CAPI returned ${response.status}`);
  }

  return result;
}

// ============================================================
// EMQ SCORE ESTIMATOR (pentru logging)
// ============================================================

function calculateEMQ(userData = {}) {
  const weights = {
    em: 2.5,
    ph: 2.0,
    fbc: 1.5,
    fbp: 1.0,
    client_ip_address: 1.0,
    fn: 0.5,
    ln: 0.5,
    client_user_agent: 0.5,
    country: 0.3,
    ct: 0.2
  };

  let score = 0;
  for (const [field, weight] of Object.entries(weights)) {
    if (userData[field] && userData[field] !== '0.0.0.0') score += weight;
  }

  return Math.min(10, score).toFixed(1);
}

// ============================================================
// CORS HELPER
// ============================================================

function corsResponse(body, status, env) {
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': env.ALLOWED_ORIGIN || '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
  };

  return new Response(
    body !== null ? JSON.stringify(body) : null,
    { status, headers }
  );
}
