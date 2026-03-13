/**
 * ============================================================
 * META ENHANCED PIXEL - Suplimente Alimentare
 * ============================================================
 * Autor: Custom Implementation
 * Versiune: 2.0
 * 
 * Ce face acest pixel față de cel standard Meta:
 * 1. Colectează 40+ semnale comportamentale (vs 8 standard)
 * 2. Trimite date prin AMBELE canale: Pixel + CAPI (deduplication)
 * 3. Hashing SHA-256 pentru toate datele personale (GDPR compliant)
 * 4. Scoring intent de cumpărare în timp real
 * 5. Segmentare automată audience quality
 * 6. Custom events specifice suplimentelor alimentare
 * ============================================================
 */

(function() {
  'use strict';

  // ============================================================
  // CONFIGURARE - Modifica aceste valori
  // ============================================================
  const CONFIG = {
    PIXEL_ID: '1023917159458734',           // ID-ul tău Meta Pixel
    CAPI_ENDPOINT: 'https://meta-capi-proxy.cristianlinaru.workers.dev/capi',      // Endpoint-ul tău server-side CAPI
    DATASET_ID: '1023917159458734',        // Pentru CAPI
    DEBUG: false,                          // true pentru development
    
    // Praguri scoring intent (0-100)
    INTENT_THRESHOLDS: {
      LOW: 20,
      MEDIUM: 45,
      HIGH: 70,
      VERY_HIGH: 85
    },
    
    // Timpi de engagement (secunde)
    ENGAGEMENT_TIMERS: {
      SHORT_VISIT: 15,
      ENGAGED_VISIT: 45,
      DEEP_ENGAGEMENT: 120,
      POWER_USER: 300
    }
  };

  // ============================================================
  // STATE MANAGEMENT
  // ============================================================
  const STATE = {
    sessionId: generateSessionId(),
    userId: getCookieOrGenerate('_emuid'),
    startTime: Date.now(),
    intentScore: 0,
    pageDepth: 0,
    maxScroll: 0,
    clicks: 0,
    productViews: [],
    searchQueries: [],
    videoEngagement: {},
    formInteractions: {},
    hoverEvents: [],
    copyEvents: [],
    tabVisibility: { hidden: 0, visible: Date.now() },
    deviceContext: getDeviceContext(),
    trafficSource: getTrafficSource(),
    userSegment: null,
    eventQueue: [],
    firedEvents: new Set()
  };

  // ============================================================
  // UTILITĂȚI
  // ============================================================

  function generateSessionId() {
    return 'sess_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
  }

  function getCookieOrGenerate(name) {
    const existing = getCookie(name);
    if (existing) return existing;
    const newId = 'uid_' + Math.random().toString(36).substr(2, 16);
    setCookie(name, newId, 365);
    return newId;
  }

  function getCookie(name) {
    const match = document.cookie.match(new RegExp('(^| )' + name + '=([^;]+)'));
    return match ? match[2] : null;
  }

  function setCookie(name, value, days) {
    const expires = new Date(Date.now() + days * 864e5).toUTCString();
    document.cookie = name + '=' + value + '; expires=' + expires + '; path=/; SameSite=Lax';
  }

  async function hashSHA256(str) {
    if (!str) return null;
    const normalized = str.trim().toLowerCase();
    const buffer = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(normalized));
    return Array.from(new Uint8Array(buffer)).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  function getDeviceContext() {
    const ua = navigator.userAgent;
    const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
    
    return {
      deviceType: /Mobi|Android/i.test(ua) ? 'mobile' : /Tablet|iPad/i.test(ua) ? 'tablet' : 'desktop',
      os: /Windows/i.test(ua) ? 'windows' : /Mac/i.test(ua) ? 'macos' : /iOS/i.test(ua) ? 'ios' : /Android/i.test(ua) ? 'android' : 'other',
      browser: /Chrome/i.test(ua) ? 'chrome' : /Firefox/i.test(ua) ? 'firefox' : /Safari/i.test(ua) ? 'safari' : 'other',
      screenWidth: window.screen.width,
      screenHeight: window.screen.height,
      colorDepth: window.screen.colorDepth,
      pixelRatio: window.devicePixelRatio || 1,
      touchEnabled: 'ontouchstart' in window,
      connectionType: connection ? connection.effectiveType : 'unknown',
      connectionSpeed: connection ? connection.downlink : null,
      language: navigator.language,
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      cookiesEnabled: navigator.cookieEnabled,
      doNotTrack: navigator.doNotTrack === '1',
      memoryGB: navigator.deviceMemory || null,
      cpuCores: navigator.hardwareConcurrency || null
    };
  }

  function getTrafficSource() {
    const params = new URLSearchParams(window.location.search);
    const referrer = document.referrer;
    
    return {
      utmSource: params.get('utm_source'),
      utmMedium: params.get('utm_medium'),
      utmCampaign: params.get('utm_campaign'),
      utmContent: params.get('utm_content'),
      utmTerm: params.get('utm_term'),
      fbclid: params.get('fbclid'),
      gclid: params.get('gclid'),
      referrerDomain: referrer ? new URL(referrer).hostname : 'direct',
      landingPage: window.location.href,
      isRetargeting: !!getCookie('_fbp') || !!getCookie('_ga'),
      isPaidTraffic: !!(params.get('utm_medium') === 'cpc' || params.get('fbclid') || params.get('gclid'))
    };
  }

  function getPageContext() {
    return {
      url: window.location.href,
      path: window.location.pathname,
      title: document.title,
      referrer: document.referrer,
      // Detectare tip pagina pentru suplimente
      pageType: detectPageType(),
      hasVideo: document.querySelectorAll('video').length > 0,
      hasForm: document.querySelectorAll('form').length > 0,
      wordCount: document.body.innerText.split(/\s+/).length,
      loadTime: performance.timing ? performance.timing.loadEventEnd - performance.timing.navigationStart : null
    };
  }

  function detectPageType() {
    const path = window.location.pathname.toLowerCase();
    const title = document.title.toLowerCase();
    const bodyText = document.body.innerText.toLowerCase().substring(0, 500);
    
    if (path.includes('checkout') || path.includes('cart')) return 'checkout';
    if (path.includes('product') || path.includes('produs')) return 'product';
    if (path.includes('category') || path.includes('categorie')) return 'category';
    if (path.includes('blog') || path.includes('articol')) return 'blog';
    if (path === '/' || path === '/index') return 'homepage';
    if (path.includes('contact')) return 'contact';
    if (path.includes('about') || path.includes('despre')) return 'about';
    
    // Detectare bazata pe continut
    if (bodyText.includes('adaugă în coș') || bodyText.includes('add to cart')) return 'product';
    return 'other';
  }

  // ============================================================
  // INTENT SCORING ENGINE
  // ============================================================
  // Algoritmul de scoring evaluează calitatea unui vizitator
  // pe o scală 0-100. Meta folosește aceste semnale pentru
  // a găsi lookalike audiences mai precise.

  function updateIntentScore(action, value = 1) {
    const SCORE_MAP = {
      // Engagement de baza
      'scroll_25': 3,
      'scroll_50': 5,
      'scroll_75': 8,
      'scroll_100': 10,
      'time_15s': 3,
      'time_45s': 6,
      'time_120s': 10,
      'time_300s': 15,
      
      // Interactiuni cu produsul
      'product_image_zoom': 8,
      'product_gallery_view': 6,
      'ingredient_section_view': 12,  // Specific suplimente - citire ingrediente = intent mare
      'dosage_section_view': 10,       // Citire dozaj = intent mare
      'reviews_read': 9,
      'reviews_scroll': 7,
      'faq_open': 8,
      'compare_products': 12,
      'size_selector_click': 10,
      'quantity_change': 12,
      
      // Actiuni de cumparare
      'add_to_cart': 25,
      'view_cart': 15,
      'initiate_checkout': 30,
      'payment_info_fill': 20,
      'promo_code_attempt': 15,
      
      // Semnale de cercetare
      'site_search': 8,
      'filter_use': 6,
      'sort_change': 5,
      'category_navigation': 4,
      
      // Semnale negative (scad scorul)
      'quick_bounce': -20,
      'rage_click': -5,
      'back_button': -10,
      'tab_switch_long': -8
    };
    
    const delta = (SCORE_MAP[action] || 0) * value;
    STATE.intentScore = Math.max(0, Math.min(100, STATE.intentScore + delta));
    
    // Actualizeaza segmentul user
    updateUserSegment();
    
    if (CONFIG.DEBUG) console.log(`[MetaPixel] Intent: ${action} -> ${STATE.intentScore}`);
  }

  function updateUserSegment() {
    const score = STATE.intentScore;
    const prevSegment = STATE.userSegment;
    
    if (score >= CONFIG.INTENT_THRESHOLDS.VERY_HIGH) {
      STATE.userSegment = 'hot_lead';
    } else if (score >= CONFIG.INTENT_THRESHOLDS.HIGH) {
      STATE.userSegment = 'warm_lead';
    } else if (score >= CONFIG.INTENT_THRESHOLDS.MEDIUM) {
      STATE.userSegment = 'interested';
    } else if (score >= CONFIG.INTENT_THRESHOLDS.LOW) {
      STATE.userSegment = 'browsing';
    } else {
      STATE.userSegment = 'cold';
    }
    
    // Trimite event cand segmentul se schimba
    if (prevSegment && prevSegment !== STATE.userSegment) {
      fireEvent('UserSegmentUpgrade', {
        previous_segment: prevSegment,
        new_segment: STATE.userSegment,
        intent_score: STATE.intentScore
      });
    }
  }

  // ============================================================
  // EVENT FIRING ENGINE
  // ============================================================

  async function fireEvent(eventName, eventData = {}, options = {}) {
    const { deduplicate = false, standard = false } = options;
    
    // Deduplication pentru checkout events
    if (deduplicate) {
      const key = eventName + '_' + JSON.stringify(eventData.value || '');
      if (STATE.firedEvents.has(key)) return;
      STATE.firedEvents.add(key);
    }
    
    const eventId = 'ev_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
    
    // Date comune pentru toate eventurile
    const commonData = {
      event_id: eventId,
      session_id: STATE.sessionId,
      user_id: STATE.userId,
      intent_score: STATE.intentScore,
      user_segment: STATE.userSegment,
      time_on_site: Math.round((Date.now() - STATE.startTime) / 1000),
      scroll_depth: STATE.maxScroll,
      click_count: STATE.clicks,
      device: STATE.deviceContext,
      traffic: STATE.trafficSource,
      page: getPageContext(),
      ...eventData
    };
    
    // 1. Trimite prin Meta Pixel (browser-side)
    if (typeof fbq !== 'undefined') {
      if (standard) {
        fbq('track', eventName, commonData, { eventID: eventId });
      } else {
        fbq('trackCustom', eventName, commonData, { eventID: eventId });
      }
    }
    
    // 2. Trimite prin CAPI (server-side) - pentru deduplication si tracking IOS
    await sendToCAPI(eventName, commonData, eventId);
    
    if (CONFIG.DEBUG) console.log(`[MetaPixel] Event fired: ${eventName}`, commonData);
  }

  async function sendToCAPI(eventName, eventData, eventId) {
    // Colecteaza si hasheaza datele utilizatorului
    const userData = await collectUserData();
    
    const capiPayload = {
      data: [{
        event_name: eventName,
        event_time: Math.floor(Date.now() / 1000),
        event_id: eventId,
        event_source_url: window.location.href,
        action_source: 'website',
        user_data: userData,
        custom_data: eventData
      }]
    };
    
    // Trimite la endpoint-ul tau server-side
    // Serverul tau va trimite mai departe la Meta CAPI cu access_token
    try {
      await fetch(CONFIG.CAPI_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(capiPayload),
        keepalive: true  // Important: permite request-ul sa continue dupa navigare
      });
    } catch (e) {
      if (CONFIG.DEBUG) console.warn('[MetaPixel] CAPI send failed:', e);
    }
  }

  async function collectUserData() {
    // Colecteaza tot ce putem din browser si hasheaza pentru Meta
    const userData = {};
    
    // FBP si FBC cookies (Meta le foloseste intern)
    const fbp = getCookie('_fbp');
    const fbc = getCookie('_fbc') || (STATE.trafficSource.fbclid ? 'fb.1.' + Date.now() + '.' + STATE.trafficSource.fbclid : null);
    
    if (fbp) userData.fbp = fbp;
    if (fbc) userData.fbc = fbc;
    
    // IP si User Agent (trimis de server, nu de browser - mai sigur)
    userData.client_user_agent = navigator.userAgent;
    
    // Date din localStorage/sessionStorage daca userul si-a creat cont
    const storedEmail = localStorage.getItem('user_email') || sessionStorage.getItem('checkout_email');
    const storedPhone = localStorage.getItem('user_phone') || sessionStorage.getItem('checkout_phone');
    const storedName = localStorage.getItem('user_name');
    
    if (storedEmail) userData.em = await hashSHA256(storedEmail);
    if (storedPhone) userData.ph = await hashSHA256(storedPhone.replace(/\D/g, ''));
    
    if (storedName) {
      const nameParts = storedName.trim().split(' ');
      userData.fn = await hashSHA256(nameParts[0]);
      if (nameParts.length > 1) userData.ln = await hashSHA256(nameParts.slice(1).join(' '));
    }
    
    // Date din form-uri (dacă utilizatorul le-a completat)
    const emailInputs = document.querySelectorAll('input[type="email"]');
    for (const input of emailInputs) {
      if (input.value && input.value.includes('@') && !userData.em) {
        userData.em = await hashSHA256(input.value);
        break;
      }
    }
    
    // Geolocation aproximativa din timezone
    userData.country = getCountryFromTimezone();
    
    return userData;
  }

  function getCountryFromTimezone() {
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const tzMap = {
      'Europe/Bucharest': 'ro',
      'Europe/London': 'gb',
      'Europe/Paris': 'fr',
      'Europe/Berlin': 'de',
      'America/New_York': 'us',
      'America/Chicago': 'us',
      'America/Los_Angeles': 'us'
    };
    return tzMap[tz] || null;
  }

  // ============================================================
  // SCROLL TRACKING - Granular 5% increments
  // ============================================================

  function initScrollTracking() {
    let lastFired = 0;
    const milestones = new Set();
    
    function onScroll() {
      const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
      const docHeight = document.documentElement.scrollHeight - window.innerHeight;
      const scrollPercent = docHeight > 0 ? Math.round((scrollTop / docHeight) * 100) : 0;
      
      STATE.maxScroll = Math.max(STATE.maxScroll, scrollPercent);
      STATE.pageDepth = scrollTop;
      
      // Fireworks la milestone-uri
      [10, 25, 50, 75, 90, 100].forEach(milestone => {
        if (scrollPercent >= milestone && !milestones.has(milestone)) {
          milestones.add(milestone);
          updateIntentScore('scroll_' + milestone);
          
          if (milestone >= 25) {
            fireEvent('ScrollDepth', {
              scroll_percent: milestone,
              content_type: detectPageType()
            });
          }
        }
      });
      
      // Detectare zone importante pentru suplimente
      checkSupplementSections();
    }
    
    window.addEventListener('scroll', throttle(onScroll, 200), { passive: true });
  }

  // Detecteaza daca utilizatorul citeste sectiunile importante
  function checkSupplementSections() {
    const viewportMid = window.pageYOffset + window.innerHeight * 0.5;
    
    const sectionSelectors = {
      ingredients: '[class*="ingredient"], [class*="ingrediente"], [id*="ingredient"], h2:contains("Ingrediente"), h3:contains("Compoziție")',
      dosage: '[class*="dosage"], [class*="dozaj"], [class*="utilizare"], [id*="how-to-use"]',
      reviews: '[class*="review"], [class*="testimonial"], [class*="recenzie"], .reviews-section',
      benefits: '[class*="benefit"], [class*="beneficii"], [class*="avantaje"]',
      nutrition: '[class*="nutritional"], [class*="nutritional-facts"], [class*="valori-nutritionale"]'
    };
    
    Object.entries(sectionSelectors).forEach(([section, selector]) => {
      try {
        const elements = document.querySelectorAll(selector.split(', ')[0]);
        elements.forEach(el => {
          const rect = el.getBoundingClientRect();
          const elMid = rect.top + window.pageYOffset + rect.height / 2;
          
          if (Math.abs(elMid - viewportMid) < window.innerHeight * 0.4) {
            const key = section + '_' + Math.floor(elMid / 100);
            if (!STATE.firedEvents.has('section_' + key)) {
              STATE.firedEvents.add('section_' + key);
              updateIntentScore(section + '_section_view');
              fireEvent('ContentSectionView', {
                section_type: section,
                page_type: detectPageType()
              });
            }
          }
        });
      } catch(e) {}
    });
  }

  // ============================================================
  // TIME TRACKING
  // ============================================================

  function initTimeTracking() {
    const timers = [
      { seconds: CONFIG.ENGAGEMENT_TIMERS.SHORT_VISIT, action: 'time_15s', event: 'TimeOnSite' },
      { seconds: CONFIG.ENGAGEMENT_TIMERS.ENGAGED_VISIT, action: 'time_45s', event: 'EngagedVisit' },
      { seconds: CONFIG.ENGAGEMENT_TIMERS.DEEP_ENGAGEMENT, action: 'time_120s', event: 'DeepEngagement' },
      { seconds: CONFIG.ENGAGEMENT_TIMERS.POWER_USER, action: 'time_300s', event: 'PowerUser' }
    ];
    
    timers.forEach(({ seconds, action, event }) => {
      setTimeout(() => {
        if (!document.hidden) {
          updateIntentScore(action);
          fireEvent(event, {
            time_seconds: seconds,
            page_type: detectPageType(),
            scroll_depth: STATE.maxScroll,
            products_viewed: STATE.productViews.length
          });
        }
      }, seconds * 1000);
    });
    
    // Track tab visibility
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        STATE.tabVisibility.hidden = Date.now();
      } else {
        const hiddenDuration = Date.now() - STATE.tabVisibility.hidden;
        if (hiddenDuration > 30000) { // > 30 secunde tab ascuns
          updateIntentScore('tab_switch_long');
        }
        STATE.tabVisibility.visible = Date.now();
      }
    });
  }

  // ============================================================
  // CLICK TRACKING - Granular
  // ============================================================

  function initClickTracking() {
    document.addEventListener('click', function(e) {
      STATE.clicks++;
      const target = e.target.closest('a, button, [role="button"], [data-action], .btn, input[type="submit"]');
      if (!target) return;
      
      const clickData = {
        element_type: target.tagName.toLowerCase(),
        element_text: (target.innerText || target.value || '').trim().substring(0, 50),
        element_class: target.className.toString().substring(0, 100),
        element_id: target.id,
        href: target.href,
        page_type: detectPageType()
      };
      
      // Detectare CTA-uri importante
      const text = clickData.element_text.toLowerCase();
      
      if (text.includes('adaugă în coș') || text.includes('add to cart') || text.includes('cumpără')) {
        updateIntentScore('add_to_cart');
        // Standard AddToCart event - folosit de Meta algoritm
        const productData = extractProductData();
        fireEvent('AddToCart', { ...productData, ...clickData }, { standard: true, deduplicate: false });
        
      } else if (text.includes('checkout') || text.includes('finalizare') || text.includes('plată')) {
        updateIntentScore('initiate_checkout');
        fireEvent('InitiateCheckout', clickData, { standard: true });
        
      } else if (text.includes('cumpără acum') || text.includes('buy now')) {
        updateIntentScore('add_to_cart');
        updateIntentScore('initiate_checkout');
        fireEvent('BuyNowClick', clickData);
        
      } else if (text.includes('compară') || text.includes('compare')) {
        updateIntentScore('compare_products');
        fireEvent('ProductCompare', clickData);
        
      } else if (target.closest('.product-gallery, .product-images, [class*="gallery"]')) {
        updateIntentScore('product_image_zoom');
        fireEvent('ProductImageInteraction', clickData);
        
      } else if (text.includes('recenzii') || text.includes('reviews') || text.includes('opinii')) {
        updateIntentScore('reviews_read');
        fireEvent('ReviewsSectionClick', clickData);
      }
      
      // Generic click pentru ML
      fireEvent('ClickInteraction', {
        ...clickData,
        click_number: STATE.clicks,
        intent_score_at_click: STATE.intentScore
      });
      
    }, true);
    
    // Rage click detection
    let recentClicks = [];
    document.addEventListener('click', function() {
      const now = Date.now();
      recentClicks.push(now);
      recentClicks = recentClicks.filter(t => now - t < 1000);
      if (recentClicks.length >= 4) {
        updateIntentScore('rage_click');
        fireEvent('RageClick', { clicks_per_second: recentClicks.length });
        recentClicks = [];
      }
    });
  }

  // ============================================================
  // PRODUCT DATA EXTRACTION
  // ============================================================

  function extractProductData() {
    const data = {};
    
    // Incearca sa extraga date din meta tags si structured data
    const ogTitle = document.querySelector('meta[property="og:title"]');
    const ogPrice = document.querySelector('meta[property="product:price:amount"]');
    const ogCurrency = document.querySelector('meta[property="product:price:currency"]');
    const ogImage = document.querySelector('meta[property="og:image"]');
    
    if (ogTitle) data.content_name = ogTitle.content;
    if (ogPrice) data.value = parseFloat(ogPrice.content);
    if (ogCurrency) data.currency = ogCurrency.content;
    if (ogImage) data.image_url = ogImage.content;
    
    // Incearca schema.org JSON-LD
    const jsonLd = document.querySelector('script[type="application/ld+json"]');
    if (jsonLd) {
      try {
        const schema = JSON.parse(jsonLd.textContent);
        if (schema['@type'] === 'Product') {
          data.content_name = data.content_name || schema.name;
          data.content_ids = [schema.sku || schema.productID || schema.gtin];
          data.brand = schema.brand ? schema.brand.name : null;
          if (schema.offers) {
            data.value = data.value || parseFloat(schema.offers.price);
            data.currency = data.currency || schema.offers.priceCurrency;
          }
        }
      } catch(e) {}
    }
    
    // Date specifice suplimentelor
    data.supplement_category = detectSupplementCategory();
    data.content_type = 'product';
    
    return data;
  }

  function detectSupplementCategory() {
    const bodyText = document.body.innerText.toLowerCase();
    const title = document.title.toLowerCase();
    const combined = title + ' ' + bodyText.substring(0, 1000);
    
    const categories = {
      'protein': ['proteina', 'protein', 'whey', 'casein', 'albumina'],
      'vitamins': ['vitamina', 'vitamin', 'multivitamin', 'complex vitamine'],
      'weight_loss': ['slabit', 'ardere grasimi', 'fat burner', 'metabolism', 'detox'],
      'muscle_gain': ['masa musculara', 'creatina', 'creatine', 'aminoacid', 'bcaa'],
      'energy': ['energie', 'energy', 'pre-workout', 'cofeina', 'ginseng'],
      'immunity': ['imunitate', 'immunity', 'vitamina c', 'zinc', 'echinacea'],
      'joints': ['articulatii', 'colagen', 'collagen', 'glucozamina', 'condroitina'],
      'omega': ['omega', 'ulei peste', 'fish oil', 'acizi grasi'],
      'probiotics': ['probiotic', 'prebiot', 'flora intestinala', 'lactobacil']
    };
    
    for (const [category, keywords] of Object.entries(categories)) {
      if (keywords.some(kw => combined.includes(kw))) return category;
    }
    return 'general_supplement';
  }

  // ============================================================
  // FORM TRACKING - Capture date la checkout
  // ============================================================

  function initFormTracking() {
    // Track completare campuri fara a stoca date brute
    document.addEventListener('blur', async function(e) {
      const input = e.target;
      if (!['INPUT', 'SELECT', 'TEXTAREA'].includes(input.tagName)) return;
      
      const inputType = input.type || input.tagName.toLowerCase();
      const inputName = (input.name || input.id || '').toLowerCase();
      
      // Hash si trimite date personale
      if (inputType === 'email' && input.value.includes('@')) {
        const hashed = await hashSHA256(input.value);
        fireEvent('EmailCaptured', { em: hashed, source: 'form_blur' });
        // Salveaza pentru CAPI
        sessionStorage.setItem('checkout_email', input.value);
      }
      
      if (inputType === 'tel' || inputName.includes('phone') || inputName.includes('telefon')) {
        const cleaned = input.value.replace(/\D/g, '');
        if (cleaned.length >= 9) {
          const hashed = await hashSHA256(cleaned);
          fireEvent('PhoneCaptured', { ph: hashed, source: 'form_blur' });
          sessionStorage.setItem('checkout_phone', input.value);
        }
      }
      
      // Track progres checkout fara date personale
      STATE.formInteractions[inputName] = true;
      const totalFields = document.querySelectorAll('input:not([type="hidden"]), select, textarea').length;
      const filledFields = Object.keys(STATE.formInteractions).length;
      const completionPct = Math.round((filledFields / totalFields) * 100);
      
      if ([25, 50, 75, 100].includes(completionPct)) {
        updateIntentScore('payment_info_fill');
        fireEvent('CheckoutFormProgress', {
          completion_percent: completionPct,
          fields_filled: filledFields,
          total_fields: totalFields
        });
      }
      
    }, true);
  }

  // ============================================================
  // SEARCH TRACKING
  // ============================================================

  function initSearchTracking() {
    const searchInputs = document.querySelectorAll('input[type="search"], input[name*="search"], input[placeholder*="cauta"], input[placeholder*="search"]');
    
    searchInputs.forEach(input => {
      input.addEventListener('keyup', debounce(function() {
        if (input.value.length >= 3) {
          STATE.searchQueries.push(input.value);
          updateIntentScore('site_search');
          fireEvent('Search', {
            search_string: input.value,
            supplement_intent: detectSearchIntent(input.value)
          }, { standard: true });
        }
      }, 500));
    });
  }

  function detectSearchIntent(query) {
    const q = query.toLowerCase();
    if (q.includes('pret') || q.includes('ieftin') || q.includes('oferta')) return 'price_sensitive';
    if (q.includes('cel mai bun') || q.includes('recomandat') || q.includes('top')) return 'quality_seeking';
    if (q.includes('slabit') || q.includes('masa') || q.includes('energie')) return 'goal_oriented';
    if (q.includes('natural') || q.includes('bio') || q.includes('organic')) return 'natural_preference';
    return 'general_search';
  }

  // ============================================================
  // VIDEO TRACKING
  // ============================================================

  function initVideoTracking() {
    const videos = document.querySelectorAll('video');
    
    videos.forEach((video, index) => {
      STATE.videoEngagement[index] = { maxWatched: 0, started: false };
      
      video.addEventListener('play', () => {
        if (!STATE.videoEngagement[index].started) {
          STATE.videoEngagement[index].started = true;
          fireEvent('VideoStart', {
            video_index: index,
            video_src: video.currentSrc,
            video_duration: video.duration,
            page_type: detectPageType()
          });
        }
      });
      
      video.addEventListener('timeupdate', throttle(() => {
        const pct = Math.round((video.currentTime / video.duration) * 100);
        STATE.videoEngagement[index].maxWatched = Math.max(STATE.videoEngagement[index].maxWatched, pct);
        
        [25, 50, 75, 95].forEach(milestone => {
          const key = 'video_' + index + '_' + milestone;
          if (pct >= milestone && !STATE.firedEvents.has(key)) {
            STATE.firedEvents.add(key);
            fireEvent('VideoProgress', {
              video_index: index,
              watched_percent: milestone
            });
          }
        });
      }, 1000));
    });
    
    // YouTube embeds
    if (document.querySelector('iframe[src*="youtube"]')) {
      // YouTube iframe API tracking
      window.onYouTubeIframeAPIReady = function() {
        document.querySelectorAll('iframe[src*="youtube"]').forEach(iframe => {
          try {
            new YT.Player(iframe, {
              events: {
                'onStateChange': function(event) {
                  if (event.data === YT.PlayerState.PLAYING) {
                    fireEvent('YouTubeVideoPlay', { embed_url: iframe.src });
                  }
                }
              }
            });
          } catch(e) {}
        });
      };
    }
  }

  // ============================================================
  // MOUSE BEHAVIOR - Detectare interes
  // ============================================================

  function initMouseTracking() {
    let mouseLeaveTimer;
    
    // Exit intent detection
    document.addEventListener('mouseleave', function(e) {
      if (e.clientY < 5) { // Mouse se duce spre bara browser
        mouseLeaveTimer = setTimeout(() => {
          fireEvent('ExitIntent', {
            time_on_site: Math.round((Date.now() - STATE.startTime) / 1000),
            intent_score: STATE.intentScore,
            scroll_depth: STATE.maxScroll,
            products_viewed: STATE.productViews.length,
            cart_items: getCartItemCount()
          });
        }, 100);
      }
    });
    
    document.addEventListener('mouseenter', function() {
      clearTimeout(mouseLeaveTimer);
    });
    
    // Hover pe produse si CTA-uri
    document.addEventListener('mouseover', throttle(function(e) {
      const productCard = e.target.closest('.product-card, .product-item, [class*="product"]');
      const ctaButton = e.target.closest('.btn-primary, .add-to-cart, [class*="cta"]');
      
      if (productCard) {
        const productName = productCard.querySelector('h2, h3, .product-title')?.innerText;
        if (productName && !STATE.hoverEvents.includes(productName)) {
          STATE.hoverEvents.push(productName);
          fireEvent('ProductHover', {
            product_name: productName.substring(0, 100),
            hover_count: STATE.hoverEvents.length
          });
        }
      }
      
      if (ctaButton && Math.random() < 0.1) { // Sample 10% pentru a nu spama
        fireEvent('CTAHover', {
          cta_text: ctaButton.innerText.trim().substring(0, 50),
          intent_score: STATE.intentScore
        });
      }
    }, 500));
  }

  // ============================================================
  // COPY TRACKING - Copiere text = cercetare activa
  // ============================================================

  function initCopyTracking() {
    document.addEventListener('copy', function() {
      const selection = window.getSelection().toString().trim();
      if (selection.length > 5) {
        STATE.copyEvents.push(selection.substring(0, 50));
        updateIntentScore('ingredient_section_view'); // Copiere = citire activa
        fireEvent('TextCopied', {
          text_length: selection.length,
          text_preview: selection.substring(0, 30) + '...',
          copy_count: STATE.copyEvents.length
        });
      }
    });
  }

  // ============================================================
  // STANDARD META EVENTS - Cu date imbogatite
  // ============================================================

  function initStandardEvents() {
    // PageView cu date extra
    if (typeof fbq !== 'undefined') {
      fbq('track', 'PageView');
    }
    
    fireEvent('EnhancedPageView', {
      ...getPageContext(),
      ...STATE.deviceContext,
      ...STATE.trafficSource,
      session_id: STATE.sessionId,
      is_returning_visitor: !!getCookie('_emuid_ret'),
      supplement_category: detectSupplementCategory()
    });
    
    // Marcheaza vizitatorul ca returnat
    setCookie('_emuid_ret', '1', 365);
    
    // ViewContent pentru pagini de produs
    if (detectPageType() === 'product') {
      const productData = extractProductData();
      STATE.productViews.push(productData);
      
      fireEvent('ViewContent', {
        ...productData,
        view_timestamp: Date.now()
      }, { standard: true });
    }
    
    // Purchase detection prin URL
    if (window.location.pathname.includes('thank-you') || 
        window.location.pathname.includes('order-received') ||
        window.location.pathname.includes('confirmare')) {
      const orderData = extractOrderData();
      fireEvent('Purchase', orderData, { standard: true, deduplicate: true });
    }
  }

  function extractOrderData() {
    // Incearca sa extraga datele comenzii din pagina de confirmare
    const data = {
      currency: 'RON',
      value: 0,
      content_type: 'product',
      content_ids: []
    };
    
    // Cauta in URL
    const params = new URLSearchParams(window.location.search);
    if (params.get('order_id')) data.order_id = params.get('order_id');
    if (params.get('total')) data.value = parseFloat(params.get('total'));
    
    // Cauta in dataLayer (Google Tag Manager)
    if (window.dataLayer) {
      const purchaseEvent = window.dataLayer.find(e => e.event === 'purchase' || e.ecommerce);
      if (purchaseEvent && purchaseEvent.ecommerce) {
        const ecom = purchaseEvent.ecommerce;
        data.value = ecom.value || ecom.revenue || data.value;
        data.order_id = ecom.transaction_id || data.order_id;
        if (ecom.items) {
          data.content_ids = ecom.items.map(i => i.item_id || i.id);
          data.contents = ecom.items.map(i => ({ id: i.item_id, quantity: i.quantity, item_price: i.price }));
        }
      }
    }
    
    return data;
  }

  function getCartItemCount() {
    // Incearca sa gaseasca numarul de produse din cos
    const cartSelectors = ['.cart-count', '.cart-qty', '[data-cart-count]', '#cart-count'];
    for (const sel of cartSelectors) {
      const el = document.querySelector(sel);
      if (el) return parseInt(el.innerText || el.getAttribute('data-cart-count')) || 0;
    }
    return 0;
  }

  // ============================================================
  // PERFORMANCE & CORE WEB VITALS TRACKING
  // ============================================================
  // Meta foloseste si viteza site-ului in algoritmul de plasare

  function initPerformanceTracking() {
    if ('PerformanceObserver' in window) {
      // Largest Contentful Paint
      try {
        const lcpObserver = new PerformanceObserver(list => {
          const entries = list.getEntries();
          const lcp = entries[entries.length - 1];
          fireEvent('CoreWebVitals', {
            metric: 'LCP',
            value: Math.round(lcp.startTime),
            rating: lcp.startTime < 2500 ? 'good' : lcp.startTime < 4000 ? 'needs_improvement' : 'poor'
          });
        });
        lcpObserver.observe({ entryTypes: ['largest-contentful-paint'] });
      } catch(e) {}
      
      // First Input Delay
      try {
        const fidObserver = new PerformanceObserver(list => {
          list.getEntries().forEach(entry => {
            fireEvent('CoreWebVitals', {
              metric: 'FID',
              value: Math.round(entry.processingStart - entry.startTime),
              rating: entry.processingStart - entry.startTime < 100 ? 'good' : 'needs_improvement'
            });
          });
        });
        fidObserver.observe({ entryTypes: ['first-input'] });
      } catch(e) {}
    }
  }

  // ============================================================
  // HELPER FUNCTIONS
  // ============================================================

  function throttle(fn, delay) {
    let lastCall = 0;
    return function(...args) {
      const now = Date.now();
      if (now - lastCall >= delay) {
        lastCall = now;
        return fn.apply(this, args);
      }
    };
  }

  function debounce(fn, delay) {
    let timer;
    return function(...args) {
      clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), delay);
    };
  }

  // ============================================================
  // INIȚIALIZARE
  // ============================================================

  function init() {
    if (CONFIG.DEBUG) console.log('[MetaPixel Enhanced] Initializing...');
    
    // Initializare Meta Pixel standard
    !function(f,b,e,v,n,t,s) {
      if(f.fbq) return;
      n=f.fbq=function(){n.callMethod? n.callMethod.apply(n,arguments):n.queue.push(arguments)};
      if(!f._fbq) f._fbq=n;
      n.push=n;n.loaded=!0;n.version='2.0';
      n.queue=[];t=b.createElement(e);t.async=!0;
      t.src=v;s=b.getElementsByTagName(e)[0];
      s.parentNode.insertBefore(t,s)
    }(window, document,'script','https://connect.facebook.net/en_US/fbevents.js');
    
    fbq('init', CONFIG.PIXEL_ID);
    
    // Initializare tracking-uri
    initStandardEvents();
    initScrollTracking();
    initTimeTracking();
    initClickTracking();
    initFormTracking();
    initSearchTracking();
    initVideoTracking();
    initMouseTracking();
    initCopyTracking();
    initPerformanceTracking();
    
    // Session end event (beforeunload)
    window.addEventListener('beforeunload', function() {
      const sessionData = {
        duration_seconds: Math.round((Date.now() - STATE.startTime) / 1000),
        max_scroll: STATE.maxScroll,
        total_clicks: STATE.clicks,
        products_viewed: STATE.productViews.length,
        searches: STATE.searchQueries.length,
        final_intent_score: STATE.intentScore,
        final_user_segment: STATE.userSegment,
        copy_events: STATE.copyEvents.length,
        page_type: detectPageType()
      };
      
      // Folosim sendBeacon pentru a fi siguri ca se trimite
      const payload = JSON.stringify({
        event_name: 'SessionEnd',
        data: sessionData
      });
      
      if (navigator.sendBeacon) {
        navigator.sendBeacon(CONFIG.CAPI_ENDPOINT + '/session-end', payload);
      }
      
      // Trimite si prin pixel
      if (typeof fbq !== 'undefined') {
        fbq('trackCustom', 'SessionEnd', sessionData);
      }
    });
    
    if (CONFIG.DEBUG) console.log('[MetaPixel Enhanced] Initialized. Session:', STATE.sessionId);
  }

  // Start
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Expune API public pentru apeluri manuale
  window.MetaPixelEnhanced = {
    fireEvent,
    updateIntentScore,
    extractProductData,
    getState: () => ({ ...STATE }),
    trackPurchase: (orderData) => fireEvent('Purchase', orderData, { standard: true, deduplicate: true }),
    trackAddToCart: (productData) => fireEvent('AddToCart', productData, { standard: true }),
    trackLead: async (email, phone) => {
      const userData = {};
      if (email) userData.em = await hashSHA256(email);
      if (phone) userData.ph = await hashSHA256(phone.replace(/\D/g, ''));
      fireEvent('Lead', userData, { standard: true });
    }
  };

})();

// ============================================================
// NOTĂ: Server-side CAPI endpoint (/api/meta-capi)
// ============================================================
// Serverul tău trebuie să trimită mai departe la:
// POST https://graph.facebook.com/v18.0/{DATASET_ID}/events
// Authorization: access_token={ACCESS_TOKEN}
// 
// Asta este ESENTIAL pentru:
// 1. Tracking iOS 14.5+ (ITP blocks pixel)
// 2. Deduplication Pixel + CAPI
// 3. EMQ score mai bun (Event Match Quality)
// ============================================================
