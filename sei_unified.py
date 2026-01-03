"""
===============================================================================
SEI v6.2.3 - UNIFIED SELF-EXPANDING INTELLIGENCE
===============================================================================

CHANGELOG:
- v6.2.3: Content validation, short_description, and title case
  • Added validation before generate (checks equipment_type_id, page_category, etc.)
  • Added validation before publish (checks content, hero_image, short_description)
  • Auto-generates short_description during content generation
  • short_description saved to both generated_content and decision_nodes column
  • Added to_title_case() for proper headline capitalization
  • Applied title case to 'name' and fallback 'seo-title' in Webflow payload
  • "bulldozer financing" → "Bulldozer Financing"
  • Prevents incomplete pages from being published

- v6.2.2: Markdown to HTML conversion
  • Rich text fields now converted from Markdown to HTML for Webflow
  • Fixes hashtag/formatting issues in Webflow CMS
  • Affects: intro, main_content, how_it_works, features

- v6.2.1: Added subheadline field
  • New "subheadline" field (15-25 words) for hero section
  • "intro" remains as longer 2-3 paragraph intro below hero
  • Webflow payload updated to include subheadline

- v6.2.0: Interlinking & Auto-Refresh
  • Body content interlinking - links injected into paragraphs + related section
  • Scheduled refresh system - auto-refresh stale pages (>30 days)
  • Maintenance command - refresh + sitemap + relink in one
  • Relink command - update all internal links across site
  • Hubs now link to child spokes

- v6.1.1: Bug fixes
  • Fixed Claude cost calculation (was 1000x too high)
  • Removed display_name from equipment_types insert
  • Removed lsi_keywords from DB update (column doesn't exist)

- v6.1.0: MAJOR UPDATE - All main.py v5.0 features integrated
  • Added IndexNow instant indexing on publish
  • Added comprehensive interlinking system (hub↔spoke, siblings, contextual)
  • Added Google Search Console integration (keyword discovery, ranking tracking)
  • Added automatic sitemap generation
  • Added performance feedback loop (learns from GSC data)
  • Added LSI keyword extraction from competitors
  • Added Explorer Mode (auto-discover new keyword opportunities)
  • Added SERP change detection (skip re-scraping unchanged SERPs)
  • Added Schema JSON-LD generation (FAQ, Article, Service, Breadcrumb)
  • Added brand compliance checker + auto-sanitizer
  • Added content versioning (prevent quality regression)
  • Added full quality gates validation
  • Added real-time session cost tracking

- v6.0.4: Fixed Supabase relationship ambiguity for equipment_types queries
- v6.0.3: Added node_type field (hub/spoke) to cluster creation
- v6.0.2: Added normalized_decision_key field to cluster creation
- v6.0.1: Initial unified system combining main.py + hub/spoke + safe publishing

THE INTELLIGENCE LOOP:
  1. DISCOVER: GSC impressions + Explorer mode → find keyword opportunities
  2. CREATE: Classify → Cluster → Content → Images → Links → Schema
  3. PUBLISH: Webflow → IndexNow → Sitemap
  4. MONITOR: Track rankings, CTR, SERP changes
  5. LEARN: Analyze what works → improve content patterns
  → Loop back to DISCOVER

===============================================================================
"""

import os
import sys
import json
import csv
import time
import hashlib
import re
import requests
from io import BytesIO
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import unicodedata

# External dependencies
from supabase import create_client, Client
import anthropic

# Optional dependencies
try:
    import markdown
    MARKDOWN_AVAILABLE = True
except ImportError:
    MARKDOWN_AVAILABLE = False

try:
    from firecrawl import FirecrawlApp
    FIRECRAWL_AVAILABLE = True
except ImportError:
    FIRECRAWL_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    from PIL import Image
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False

try:
    from pydantic import BaseModel, validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    GSC_AVAILABLE = True
except ImportError:
    GSC_AVAILABLE = False

from xml.etree import ElementTree as ET


# =============================================================================
# CONFIGURATION
# =============================================================================

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY') or os.environ.get('CLAUDE_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
FIRECRAWL_API_KEY = os.environ.get('FIRECRAWL_API_KEY', '')
WEBFLOW_API_TOKEN = os.environ.get('WEBFLOW_API_TOKEN', '')
WEBFLOW_SITE_ID = os.environ.get('WEBFLOW_SITE_ID', '')
WEBFLOW_COLLECTION_ID = os.environ.get('WEBFLOW_COLLECTION_ID', '')
# IndexNow configuration
INDEXNOW_KEY = os.environ.get('INDEXNOW_KEY', '')
INDEXNOW_KEY_LOCATION = os.environ.get('INDEXNOW_KEY_LOCATION', '')  # URL to key file

# Google Search Console configuration  
GSC_CREDENTIALS_FILE = os.environ.get('GSC_CREDENTIALS_FILE', 'gsc-credentials.json')
GSC_SITE_URL = os.environ.get('GSC_SITE_URL', '')  # e.g., 'sc-domain:equipflow.co'

SITE_DOMAIN = os.environ.get('SITE_DOMAIN', 'equipflow.co')
SITE_URL = f"https://{SITE_DOMAIN}"

# Model settings
CLAUDE_MODEL = os.environ.get('CLAUDE_MODEL', 'claude-sonnet-4-20250514')

# Image settings
STORAGE_BUCKET = "page-hero-images"
IMAGE_SIZE = "1792x1024"
WEBP_QUALITY = 85

# Feature flags
ENABLE_HUNTER_INTELLIGENCE = True  # Search + scrape competitors
ENABLE_HERO_IMAGES = True
ENABLE_INDEXNOW = True
ENABLE_GSC_INTEGRATION = True
ENABLE_INTERLINKING = True
ENABLE_SITEMAP = True
ENABLE_BUDGET_CONTROL = True
ENABLE_SAFE_PUBLISHING = True
ENABLE_FEEDBACK_LOOP = True

# Safety settings
MAX_PAGES_PER_RUN = 50
CANARY_BATCH_SIZE = 5
CIRCUIT_BREAKER_THRESHOLD = 3
RATE_LIMIT_DELAY = 1.5


# =============================================================================
# LOGGING
# =============================================================================

class Status(Enum):
    OK = "✅"
    FAIL = "❌"
    WARN = "⚠️"
    INFO = "ℹ️"
    SKIP = "⏭️"
    BUDGET = "🛑"
    EXPLORE = "🔭"
    PUBLISH = "📤"
    IMAGE = "🖼️"
    SEARCH = "🔍"
    BRAIN = "🧠"
    LOCK = "🔒"
    CANARY = "🐤"
    LINK = "🔗"
    INDEX = "📡"
    GSC = "📊"
    SITEMAP = "🗺️"
    LEARN = "🎓"

def log(status: Status, message: str, indent: int = 0):
    prefix = "   " * indent
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {prefix}{status.value} {message}")


# =============================================================================
# CLIENT INITIALIZATION
# =============================================================================

supabase: Client = None
claude_client = None
openai_client = None
firecrawl = None
gsc_service = None

def init_clients():
    """Initialize all API clients"""
    global supabase, claude_client, openai_client, firecrawl, gsc_service

    if not all([SUPABASE_URL, SUPABASE_KEY, ANTHROPIC_API_KEY]):
        log(Status.FAIL, "Missing required environment variables")
        sys.exit(1)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    log(Status.OK, "Supabase + Claude initialized")

    if OPENAI_AVAILABLE and OPENAI_API_KEY:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        log(Status.OK, "OpenAI initialized - Hero images enabled")
    else:
        log(Status.WARN, "OpenAI not configured - Hero images disabled")

    if FIRECRAWL_AVAILABLE and FIRECRAWL_API_KEY:
        firecrawl = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
        log(Status.OK, "Firecrawl initialized - Hunter intelligence enabled")
    else:
        log(Status.WARN, "Firecrawl not configured - Using brain-only mode")

    # Initialize Google Search Console
    if GSC_AVAILABLE and os.path.exists(GSC_CREDENTIALS_FILE) and GSC_SITE_URL:
        try:
            credentials = service_account.Credentials.from_service_account_file(
                GSC_CREDENTIALS_FILE,
                scopes=['https://www.googleapis.com/auth/webmasters.readonly']
            )
            gsc_service = build('searchconsole', 'v1', credentials=credentials)
            log(Status.OK, "Google Search Console initialized")
        except Exception as e:
            log(Status.WARN, f"GSC init failed: {e}")
    else:
        log(Status.WARN, "GSC not configured - Feedback loop limited")


# =============================================================================
# SAFETY SYSTEMS
# =============================================================================

class CircuitBreaker:
    """Auto-stop on repeated failures"""
    def __init__(self, threshold: int = CIRCUIT_BREAKER_THRESHOLD):
        self.threshold = threshold
        self.failures = 0
        self.is_open = False

    def record_success(self):
        self.failures = 0
        self.is_open = False

    def record_failure(self):
        self.failures += 1
        if self.failures >= self.threshold:
            self.is_open = True
            log(Status.LOCK, f"Circuit breaker OPEN after {self.failures} failures")

    def can_proceed(self) -> bool:
        if self.is_open:
            log(Status.LOCK, "Circuit breaker is OPEN - stopping")
            return False
        return True

    def reset(self):
        self.failures = 0
        self.is_open = False
        log(Status.OK, "Circuit breaker reset")

circuit_breaker = CircuitBreaker()


def check_kill_switch() -> bool:
    """Check if publishing is enabled"""
    try:
        result = supabase.table('publishing_control').select('publishing_enabled').eq('id', 1).single().execute()
        if result.data and not result.data.get('publishing_enabled', True):
            log(Status.LOCK, "KILL SWITCH ACTIVE - Publishing disabled")
            return False
    except:
        pass  # Table might not exist, continue
    return True


def check_budget(service: str, units: int = 1) -> bool:
    """Check if budget allows the operation"""
    if not ENABLE_BUDGET_CONTROL:
        return True
    try:
        result = supabase.rpc('check_budget_available', {
            'service_name': service,
            'estimated_usage': units
        }).execute()
        if result.data is False:
            log(Status.BUDGET, f"Budget exceeded for {service}")
            return False
    except:
        pass  # Budget table might not exist
    return True


def increment_budget(service: str, units: int):
    """Track API usage with session tracking"""
    if not ENABLE_BUDGET_CONTROL:
        return

    # Track in memory for session stats
    if not hasattr(increment_budget, 'session_usage'):
        increment_budget.session_usage = {}

    if service not in increment_budget.session_usage:
        increment_budget.session_usage[service] = 0
    increment_budget.session_usage[service] += units

    # Try to persist to database
    try:
        supabase.rpc('increment_budget', {
            'service_name': service,
            'amount': units
        }).execute()
    except:
        pass


# Cost per unit by service
COST_PER_UNIT = {
    'claude': 0.000004,     # ~$0.004 per 1000 tokens, so $0.000004 per token
    'firecrawl': 0.01,      # $0.01 per credit
    'dalle': 0.08,          # $0.08 per image (1792x1024 standard)
}


def get_session_costs() -> dict:
    """Get costs for current session"""
    if not hasattr(increment_budget, 'session_usage'):
        return {'total': 0, 'breakdown': {}}

    breakdown = {}
    total = 0

    for service, units in increment_budget.session_usage.items():
        cost = units * COST_PER_UNIT.get(service, 0.01)
        breakdown[service] = {'units': units, 'cost': cost}
        total += cost

    return {'total': total, 'breakdown': breakdown}


def print_session_costs():
    """Print session cost summary"""
    costs = get_session_costs()
    if costs['total'] == 0:
        return

    print(f"\n💵 Session API Usage:")
    for service, data in costs['breakdown'].items():
        print(f"   {service}: {data['units']} units = ${data['cost']:.3f}")
    print(f"   ─────────────────────")
    print(f"   Session Total: ${costs['total']:.3f}")


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

# Words that should stay lowercase in titles (unless first word)
TITLE_LOWERCASE_WORDS = {'a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'by', 'of', 'in', 'vs', 'with'}

def to_title_case(text: str) -> str:
    """
    Convert text to proper title case for headlines.
    - Capitalizes first letter of each word
    - Keeps certain words lowercase (a, an, the, and, etc.) unless first word
    - Handles special cases like 'EquipFlow'
    """
    if not text:
        return ""

    # Special brand words to preserve
    brand_words = {'equipflow': 'EquipFlow', 'caterpillar': 'Caterpillar', 'john deere': 'John Deere', 'komatsu': 'Komatsu', 'kubota': 'Kubota'}

    words = text.split()
    result = []

    for i, word in enumerate(words):
        word_lower = word.lower()

        # Check for brand words
        if word_lower in brand_words:
            result.append(brand_words[word_lower])
        # First word always capitalized
        elif i == 0:
            result.append(word.capitalize())
        # Lowercase words stay lowercase (unless first)
        elif word_lower in TITLE_LOWERCASE_WORDS:
            result.append(word_lower)
        # Normal words get capitalized
        else:
            result.append(word.capitalize())

    return ' '.join(result)


def normalize_text(text: str) -> str:
    """Normalize text for matching"""
    if not text:
        return ""
    nfkd = unicodedata.normalize('NFKD', text)
    without_accents = ''.join([c for c in nfkd if not unicodedata.combining(c)])
    return ' '.join(without_accents.lower().split())


def generate_url_slug(text: str) -> str:
    """Generate URL-safe slug"""
    normalized = normalize_text(text)
    slug = normalized.replace(' ', '-')
    slug = re.sub(r'[^a-z0-9\-]', '', slug)
    slug = re.sub(r'\-+', '-', slug)
    return slug.strip('-')[:100]


def parse_json_safely(text: str) -> Optional[Dict]:
    """Parse JSON with cleanup"""
    if not text:
        return None
    text = re.sub(r'^```json\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'^```\s*$', '', text, flags=re.MULTILINE)
    text = text.strip()
    try:
        return json.loads(text)
    except:
        text = re.sub(r',\s*([}\]])', r'\1', text)
        try:
            return json.loads(text)
        except:
            return None


# =============================================================================
# INDEXNOW - INSTANT INDEXING
# =============================================================================

def ping_indexnow(urls: List[str]) -> Dict:
    """
    Ping IndexNow to request instant indexing of URLs.
    Supported by Bing, Yandex, Seznam, and Naver.
    """
    if not ENABLE_INDEXNOW or not INDEXNOW_KEY:
        return {'success': False, 'error': 'IndexNow not configured'}

    if not urls:
        return {'success': True, 'indexed': 0}

    log(Status.INDEX, f"Pinging IndexNow for {len(urls)} URLs")

    endpoint = "https://api.indexnow.org/indexnow"

    payload = {
        "host": SITE_DOMAIN,
        "key": INDEXNOW_KEY,
        "urlList": urls
    }

    if INDEXNOW_KEY_LOCATION:
        payload["keyLocation"] = INDEXNOW_KEY_LOCATION

    try:
        response = requests.post(endpoint, json=payload, headers={"Content-Type": "application/json"})

        if response.status_code in [200, 202]:
            log(Status.OK, f"IndexNow accepted {len(urls)} URLs", indent=1)
            return {'success': True, 'indexed': len(urls)}
        else:
            log(Status.WARN, f"IndexNow returned {response.status_code}", indent=1)
            return {'success': False, 'error': response.text[:200]}

    except Exception as e:
        log(Status.FAIL, f"IndexNow error: {e}", indent=1)
        return {'success': False, 'error': str(e)}


def ping_indexnow_single(url: str) -> bool:
    """Convenience function for single URL"""
    result = ping_indexnow([url])
    return result.get('success', False)


# =============================================================================
# SITEMAP GENERATION
# =============================================================================

def generate_sitemap() -> str:
    """Generate XML sitemap from all published pages"""
    log(Status.SITEMAP, "Generating sitemap...")

    result = supabase.table('decision_nodes').select(
        'url_slug, updated_at, page_category'
    ).eq('status', 'published').execute()

    if not result.data:
        log(Status.WARN, "No published pages found")
        return ""

    urlset = ET.Element('urlset')
    urlset.set('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9')

    for page in result.data:
        url_elem = ET.SubElement(urlset, 'url')

        loc = ET.SubElement(url_elem, 'loc')
        loc.text = f"{SITE_URL}/equipment/{page['url_slug']}/"

        lastmod = ET.SubElement(url_elem, 'lastmod')
        lastmod.text = page.get('updated_at', datetime.now().isoformat())[:10]

        priority = ET.SubElement(url_elem, 'priority')
        priority.text = '0.9' if page.get('page_category') == 'hub' else '0.7'

        changefreq = ET.SubElement(url_elem, 'changefreq')
        changefreq.text = 'weekly'

    xml_string = ET.tostring(urlset, encoding='unicode')
    log(Status.OK, f"Generated sitemap with {len(result.data)} URLs")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_string


def save_sitemap_to_storage() -> Optional[str]:
    """Save sitemap to Supabase storage"""
    sitemap_xml = generate_sitemap()
    if not sitemap_xml:
        return None

    try:
        supabase.storage.from_("sitemaps").upload(
            path="sitemap.xml",
            file=sitemap_xml.encode('utf-8'),
            file_options={"content-type": "application/xml", "upsert": "true"}
        )
        public_url = supabase.storage.from_("sitemaps").get_public_url("sitemap.xml")
        log(Status.OK, f"Sitemap saved: {public_url}")
        return public_url
    except Exception as e:
        log(Status.WARN, f"Sitemap storage error: {e}")
        return None


# =============================================================================
# GOOGLE SEARCH CONSOLE INTEGRATION
# =============================================================================

def fetch_gsc_performance(days: int = 28) -> List[Dict]:
    """Fetch search performance data from GSC"""
    if not gsc_service or not GSC_SITE_URL:
        return []

    log(Status.GSC, f"Fetching GSC data for last {days} days...")

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    try:
        response = gsc_service.searchanalytics().query(
            siteUrl=GSC_SITE_URL,
            body={
                'startDate': start_date.isoformat(),
                'endDate': end_date.isoformat(),
                'dimensions': ['query', 'page'],
                'rowLimit': 1000
            }
        ).execute()

        rows = response.get('rows', [])
        log(Status.OK, f"Retrieved {len(rows)} keyword/page combinations")
        return rows

    except Exception as e:
        log(Status.FAIL, f"GSC fetch error: {e}")
        return []


def discover_keyword_opportunities() -> List[Dict]:
    """Find keywords where we get impressions but don't have dedicated pages"""
    if not ENABLE_GSC_INTEGRATION or not gsc_service:
        return []

    log(Status.GSC, "Discovering keyword opportunities...")

    gsc_data = fetch_gsc_performance(days=28)
    if not gsc_data:
        return []

    existing = supabase.table('decision_nodes').select('primary_keyword').execute()
    existing_keywords = set(normalize_text(p['primary_keyword']) for p in (existing.data or []))

    opportunities = []
    equipment_terms = ['financing', 'for sale', 'rental', 'lease', 'loan', 
                      'excavator', 'crane', 'forklift', 'truck', 'equipment']

    for row in gsc_data:
        keyword = row['keys'][0]
        impressions = row.get('impressions', 0)
        position = row.get('position', 100)

        if normalize_text(keyword) in existing_keywords:
            continue

        if impressions > 50 and position > 10:
            if any(term in keyword.lower() for term in equipment_terms):
                opportunities.append({
                    'keyword': keyword,
                    'impressions': impressions,
                    'position': position,
                    'opportunity_score': impressions * (1 / max(position, 1))
                })

    opportunities.sort(key=lambda x: x['opportunity_score'], reverse=True)
    log(Status.OK, f"Found {len(opportunities)} keyword opportunities")
    return opportunities[:50]


def queue_gsc_opportunities() -> int:
    """Queue discovered opportunities for processing"""
    opportunities = discover_keyword_opportunities()
    if not opportunities:
        return 0

    queued = 0
    for opp in opportunities:
        try:
            supabase.table('market_intelligence_ahrefs').upsert({
                'keyword': opp['keyword'].lower(),
                'volume': int(opp['impressions']),
                'source': 'gsc_discovery',
                'status': 'unprocessed'
            }, on_conflict='keyword').execute()
            queued += 1
        except:
            pass

    log(Status.OK, f"Queued {queued} opportunities from GSC")
    return queued


def track_page_rankings() -> Dict:
    """Track ranking changes for our pages"""
    if not ENABLE_GSC_INTEGRATION or not gsc_service:
        return {}

    log(Status.GSC, "Tracking page rankings...")
    gsc_data = fetch_gsc_performance(days=7)

    if not gsc_data:
        return {}

    pages = supabase.table('decision_nodes').select('id, url_slug').eq('status', 'published').execute()
    page_map = {f"/equipment/{p['url_slug']}/": p['id'] for p in (pages.data or [])}

    tracking = {}
    for row in gsc_data:
        page_path = row['keys'][1].replace(SITE_URL, '')
        if page_path in page_map:
            tracking[page_map[page_path]] = {
                'keyword': row['keys'][0],
                'position': row.get('position', 0),
                'clicks': row.get('clicks', 0),
                'impressions': row.get('impressions', 0)
            }

    log(Status.OK, f"Tracked {len(tracking)} pages")
    return tracking


# =============================================================================
# INTERLINKING SYSTEM
# =============================================================================

def generate_internal_links(node_id: str) -> Dict:
    """Generate comprehensive internal links for a page and inject into body content"""
    if not ENABLE_INTERLINKING:
        return {'links_added': 0}

    log(Status.LINK, "Generating internal links...", indent=1)

    node = supabase.table('decision_nodes').select(
        '*, equipment_types!decision_nodes_equipment_type_id_fkey(name, slug)'
    ).eq('id', node_id).single().execute()

    if not node.data:
        return {'error': 'Node not found'}

    node_data = node.data
    equipment_type_id = node_data.get('equipment_type_id')
    page_category = node_data.get('page_category')
    parent_hub_id = node_data.get('parent_hub_id')

    related_links = []

    # 1. Parent hub link (for spokes)
    if page_category == 'spoke' and parent_hub_id:
        hub = supabase.table('decision_nodes').select('url_slug, primary_keyword').eq('id', parent_hub_id).single().execute()
        if hub.data:
            related_links.append({
                'url': f"/equipment/{hub.data['url_slug']}/",
                'text': hub.data['primary_keyword'],
                'type': 'parent_hub'
            })

    # 2. Sibling spokes (for spokes)
    if page_category == 'spoke':
        siblings = supabase.table('decision_nodes').select(
            'url_slug, primary_keyword, spoke_type'
        ).eq('equipment_type_id', equipment_type_id).eq('page_category', 'spoke').neq('id', node_id).execute()

        for sibling in (siblings.data or []):
            related_links.append({
                'url': f"/equipment/{sibling['url_slug']}/",
                'text': sibling['primary_keyword'],
                'type': 'sibling'
            })

    # 3. Child spokes (for hubs)
    if page_category == 'hub':
        children = supabase.table('decision_nodes').select(
            'url_slug, primary_keyword, spoke_type'
        ).eq('parent_hub_id', node_id).execute()

        for child in (children.data or []):
            related_links.append({
                'url': f"/equipment/{child['url_slug']}/",
                'text': child['primary_keyword'],
                'type': 'child_spoke'
            })

    # 4. Related equipment (different equipment types)
    related = supabase.table('decision_nodes').select(
        'url_slug, primary_keyword'
    ).eq('page_category', 'hub').neq('equipment_type_id', equipment_type_id).limit(4).execute()

    for rel in (related.data or []):
        related_links.append({
            'url': f"/equipment/{rel['url_slug']}/",
            'text': rel['primary_keyword'],
            'type': 'related'
        })

    # 5. Inject links into body content
    content = node_data.get('generated_content', {})
    if isinstance(content, str):
        content = json.loads(content) if content else {}

    if content and related_links:
        content = inject_links_into_content(content, related_links)
        log(Status.OK, f"Injected links into body content", indent=1)

    # Update node with links and updated content
    supabase.table('decision_nodes').update({
        'related_links': related_links,
        'generated_content': content
    }).eq('id', node_id).execute()

    log(Status.OK, f"Added {len(related_links)} internal links", indent=1)
    return {'links_added': len(related_links), 'links': related_links}


def generate_related_links_html(related_links: List[Dict]) -> str:
    """Generate HTML for related links section"""
    if not related_links:
        return ''

    html_parts = ['<div class="related-links"><h3>Related Resources</h3><ul>']

    for link in related_links[:6]:
        url = link.get('url', '#')
        text = link.get('text', 'Related')
        link_type = link.get('type', '')

        icon = '📚 ' if link_type == 'parent_hub' else '→ ' if link_type == 'sibling' else '🔗 '
        html_parts.append(f'<li><a href="{url}">{icon}{text}</a></li>')

    html_parts.append('</ul></div>')
    return ''.join(html_parts)


def inject_links_into_content(content: Dict, related_links: List[Dict]) -> Dict:
    """
    Inject internal links naturally into body content.
    Adds contextual links within paragraphs + related links section.
    """
    if not related_links or not content:
        return content

    # Fields to inject links into
    linkable_fields = ['main_content', 'intro', 'features', 'how_it_works']

    links_injected = 0
    max_links_per_field = 2

    for field in linkable_fields:
        if field not in content or not isinstance(content[field], str):
            continue

        field_content = content[field]
        field_links = 0

        for link in related_links:
            if field_links >= max_links_per_field:
                break
            if links_injected >= 5:  # Max total links in body
                break

            anchor_text = link.get('text', '')
            url = link.get('url', '')

            if not anchor_text or not url:
                continue

            # Try to find natural anchor text variations
            variations = [
                anchor_text,
                anchor_text.lower(),
                anchor_text.replace(' financing', ''),
                anchor_text.split()[0] if ' ' in anchor_text else None  # First word (equipment name)
            ]

            for variation in variations:
                if not variation or len(variation) < 4:
                    continue

                # Case-insensitive search, but only replace if not already a link
                pattern = re.compile(
                    r'(?<!</a>)(?<!href=")(' + re.escape(variation) + r')(?!</a>)(?!">)',
                    re.IGNORECASE
                )

                if pattern.search(field_content):
                    # Replace first occurrence only
                    replacement = f'<a href="{url}">{variation}</a>'
                    field_content = pattern.sub(replacement, field_content, count=1)
                    field_links += 1
                    links_injected += 1
                    break

        content[field] = field_content

    # Add related links section to main_content if exists
    if 'main_content' in content and related_links:
        related_html = generate_related_links_html(related_links)
        content['main_content'] = content['main_content'] + '\n\n' + related_html

    return content


# =============================================================================
# FEEDBACK LOOP - LEARNING SYSTEM
# =============================================================================

def analyze_content_performance() -> Dict:
    """Analyze which content patterns perform best"""
    if not ENABLE_FEEDBACK_LOOP:
        return {}

    log(Status.LEARN, "Analyzing content performance...")

    # Get pages with their word counts and any ranking data we have
    pages = supabase.table('decision_nodes').select(
        'id, primary_keyword, word_count, page_category, spoke_type'
    ).eq('status', 'published').execute()

    if not pages.data:
        return {}

    # Basic analysis
    word_counts = [p['word_count'] for p in pages.data if p.get('word_count')]
    avg_word_count = sum(word_counts) / len(word_counts) if word_counts else 1000

    insights = {
        'total_pages': len(pages.data),
        'avg_word_count': avg_word_count,
        'recommended_word_count': max(avg_word_count, 800)
    }

    log(Status.OK, f"Analyzed {len(pages.data)} pages, avg {avg_word_count:.0f} words")
    return insights


def get_content_recommendations() -> Dict:
    """Get recommendations for content improvement"""
    insights = analyze_content_performance()
    return {
        'target_word_count': insights.get('recommended_word_count', 1000)
    }


# =============================================================================
# LSI KEYWORD EXTRACTION (from main.py v5.0)
# =============================================================================

def extract_lsi_keywords(scraped_content: str, primary_keyword: str) -> List[str]:
    """Extract LSI keywords from competitor content"""
    lsi_candidates = [
        "equipment loan", "equipment lease", "heavy equipment",
        "construction equipment", "financing options", "loan terms",
        "interest rate", "down payment", "credit score", "approval",
        "monthly payment", "lease vs buy", "tax benefits", "depreciation",
        "section 179", "working capital", "cash flow", "collateral",
        "application process", "quick approval", "same day funding",
        "flexible terms", "competitive rates", "equipment lender",
        "commercial loan", "business financing", "capital equipment"
    ]

    content_lower = scraped_content.lower()
    found_lsi = []

    for term in lsi_candidates:
        if term in content_lower and term not in primary_keyword.lower():
            found_lsi.append(term)

    return found_lsi[:10]


# =============================================================================
# EXPLORER MODE - AUTO-DISCOVER KEYWORDS (from main.py v5.0)
# =============================================================================

ENABLE_EXPLORER = True

def extract_expansion_keywords(scraped_content: str, primary_keyword: str) -> List[str]:
    """Extract potential new keywords from competitor content"""
    patterns = [
        r'(\w+\s+financing\s+\w+)',
        r'(\w+\s+loan[s]?\s+\w+)',
        r'(\w+\s+leasing?\s+\w+)',
        r'(bad credit\s+\w+\s+\w+)',
        r'(no money down\s+\w+)',
        r'(\w+\s+equipment\s+financing)',
    ]

    found = set()
    content_lower = scraped_content.lower()

    for pattern in patterns:
        matches = re.findall(pattern, content_lower)
        for match in matches:
            clean = match.strip()
            if 8 < len(clean) < 50:
                found.add(clean)

    found.discard(primary_keyword.lower())
    return list(found)[:15]


def expand_territory(lsi_keywords: List[str], source_keyword: str) -> int:
    """Queue new keywords discovered from competitor content"""
    if not ENABLE_EXPLORER or not lsi_keywords:
        return 0

    log(Status.EXPLORE, f"Found {len(lsi_keywords)} potential opportunities...", indent=1)
    queued_count = 0

    for kw in lsi_keywords:
        clean_kw = kw.strip().lower()

        if len(clean_kw) < 8:
            continue
        if not any(term in clean_kw for term in ['financing', 'loan', 'lease', 'credit', 'equipment']):
            continue
        if clean_kw == source_keyword.lower():
            continue

        try:
            supabase.table('market_intelligence_ahrefs').upsert({
                'keyword': clean_kw,
                'volume': 100,
                'source': f'discovered_from:{source_keyword}',
                'status': 'unprocessed'
            }, on_conflict='keyword').execute()
            queued_count += 1
        except:
            pass

    if queued_count > 0:
        log(Status.OK, f"Queued {queued_count} new opportunities", indent=1)

    return queued_count


# =============================================================================
# SERP CHANGE DETECTION (from main.py v5.0)
# =============================================================================

ENABLE_SERP_CHANGE_DETECTION = True

def get_last_serp_hash(keyword: str) -> Optional[str]:
    """Get the most recent SERP hash for a keyword"""
    try:
        node = supabase.table('decision_nodes').select('serp_signature_hash').eq(
            'primary_keyword', keyword.lower().strip()
        ).execute()

        if node.data and node.data[0].get('serp_signature_hash'):
            return node.data[0]['serp_signature_hash']
        return None
    except:
        return None


def check_serp_changed(keyword: str, new_hash: str) -> Tuple[bool, Optional[str]]:
    """Check if SERP has changed since last scrape"""
    old_hash = get_last_serp_hash(keyword)

    if old_hash is None:
        return True, None  # First time

    if old_hash != new_hash:
        return True, old_hash  # Changed

    return False, old_hash  # Same


def save_serp_snapshot(node_id: str, urls: List[str]) -> Optional[str]:
    """Save SERP snapshot and return hash"""
    if not urls:
        return None

    sig = hashlib.md5(json.dumps(urls).encode()).hexdigest()

    try:
        supabase.table('decision_nodes').update({
            'serp_signature_hash': sig,
            'last_intelligence_run': datetime.now().isoformat()
        }).eq('id', node_id).execute()

        log(Status.OK, f"Saved SERP hash: {sig[:8]}...", indent=1)
        return sig
    except:
        return None


# =============================================================================
# BRAND COMPLIANCE (from main.py v5.0)
# =============================================================================

BANNED_PHRASES = [
    "our lending", "we offer loans", "we fund", "our rates", "we approve",
    "our financing", "we provide financing", "our loan products", "we lend",
    "get approved by us", "our underwriting", "we finance", "our loans"
]

def check_brand_compliance(text: str) -> Tuple[bool, List[str]]:
    """Check for brand rule violations"""
    if not text:
        return True, []
    text_lower = text.lower()
    violations = [phrase for phrase in BANNED_PHRASES if phrase in text_lower]
    return len(violations) == 0, violations


def sanitize_brand_content(text: str) -> str:
    """Auto-fix brand violations"""
    if not text:
        return text

    replacements = [
        ("our lending", "lending partners in our network"),
        ("our financing", "financing options through our partners"),
        ("we offer loans", "our lending partners offer"),
        ("we fund", "our lending partners fund"),
        ("our rates", "rates from our lending partners"),
        ("we approve", "our lending partners approve"),
        ("we lend", "our lending partners provide"),
        ("our loan products", "loan products from our partners"),
        ("we provide financing", "our partners provide financing"),
        ("our underwriting", "underwriting by our lending partners"),
        ("we finance", "our lending partners finance"),
        ("our loans", "loans from our lending partners"),
        ("get approved by us", "get approved by our lending partners"),
    ]

    for old, new in replacements:
        text = text.replace(old, new)
        text = text.replace(old.title(), new.title())

    return text


# =============================================================================
# SCHEMA JSON-LD GENERATOR (from main.py v5.0)
# =============================================================================

ENABLE_SCHEMA = True

def generate_schema_json(
    seo_title: str,
    meta_desc: str,
    url_slug: str,
    equipment_type: str,
    geo: Optional[str] = None,
    faq_list: List[Dict] = None,
    hero_image_url: Optional[str] = None
) -> Dict:
    """Generate comprehensive JSON-LD schema markup"""

    page_url = f"{SITE_URL}/equipment/{url_slug}/"

    # FAQ Schema
    faq_schema = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": []
    }

    for faq in (faq_list or []):
        faq_schema["mainEntity"].append({
            "@type": "Question",
            "name": faq.get('q', ''),
            "acceptedAnswer": {
                "@type": "Answer",
                "text": faq.get('a', '')
            }
        })

    # Article Schema
    article_schema = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": seo_title,
        "description": meta_desc,
        "author": {"@type": "Organization", "name": "EquipFlow", "url": SITE_URL},
        "publisher": {
            "@type": "Organization",
            "name": "EquipFlow",
            "url": SITE_URL,
            "logo": {"@type": "ImageObject", "url": f"{SITE_URL}/logo.png"}
        },
        "datePublished": datetime.now().strftime("%Y-%m-%d"),
        "dateModified": datetime.now().strftime("%Y-%m-%d"),
        "mainEntityOfPage": {"@type": "WebPage", "@id": page_url}
    }

    if hero_image_url:
        article_schema["image"] = {"@type": "ImageObject", "url": hero_image_url}

    # Service Schema
    service_schema = {
        "@context": "https://schema.org",
        "@type": "Service",
        "name": f"{equipment_type.title()} Financing",
        "description": meta_desc,
        "provider": {"@type": "Organization", "name": "EquipFlow", "url": SITE_URL},
        "areaServed": {"@type": "Place", "name": geo if geo else "United States"},
        "serviceType": "Equipment Financing"
    }

    # Breadcrumb Schema
    breadcrumb_items = [
        {"@type": "ListItem", "position": 1, "name": "Home", "item": SITE_URL},
        {"@type": "ListItem", "position": 2, "name": "Equipment Financing", "item": f"{SITE_URL}/equipment-financing"}
    ]

    if geo:
        breadcrumb_items.append({
            "@type": "ListItem",
            "position": 3,
            "name": f"{geo.title()} Financing",
            "item": f"{SITE_URL}/{geo.lower().replace(' ', '-')}-financing"
        })

    breadcrumb_items.append({
        "@type": "ListItem",
        "position": len(breadcrumb_items) + 1,
        "name": seo_title,
        "item": page_url
    })

    breadcrumb_schema = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": breadcrumb_items
    }

    return {
        "@context": "https://schema.org",
        "@graph": [faq_schema, article_schema, service_schema, breadcrumb_schema]
    }


# =============================================================================
# CONTENT VERSIONING (from main.py v5.0)
# =============================================================================

ENABLE_CONTENT_VERSIONING = True

def update_content_safe(node_id: str, content: Dict, word_count: int) -> bool:
    """Update content with version tracking to prevent regression"""
    if not ENABLE_CONTENT_VERSIONING:
        supabase.table('decision_nodes').update({
            'generated_content': content,
            'word_count': word_count
        }).eq('id', node_id).execute()
        return True

    try:
        # Get current version
        current = supabase.table('decision_nodes').select(
            'word_count, content_version'
        ).eq('id', node_id).single().execute()

        current_wc = current.data.get('word_count', 0) if current.data else 0
        current_version = current.data.get('content_version', 0) if current.data else 0

        # Only update if new content is better (more words) or first time
        if word_count >= current_wc * 0.9:  # Allow 10% variance
            supabase.table('decision_nodes').update({
                'generated_content': content,
                'word_count': word_count,
                'content_version': current_version + 1
            }).eq('id', node_id).execute()
            log(Status.OK, f"Content updated (v{current_version + 1})", indent=1)
            return True
        else:
            log(Status.WARN, f"Update skipped - would regress ({word_count} < {current_wc})", indent=1)
            return False
    except:
        # Fallback
        supabase.table('decision_nodes').update({
            'generated_content': content,
            'word_count': word_count
        }).eq('id', node_id).execute()
        return True


# =============================================================================
# QUALITY GATES (from main.py v5.0)
# =============================================================================

@dataclass
class QualityGateResult:
    passed: bool
    failures: List[str] = field(default_factory=list)
    word_count: int = 0
    faq_count: int = 0
    source_count: int = 0


def validate_quality_gates(
    content: Dict,
    sources: List[str],
    min_words: int = 800,
    min_sources: int = 2,
    min_faqs: int = 4
) -> QualityGateResult:
    """Validate content meets quality thresholds"""
    failures = []

    # Calculate word count from all content fields
    word_count = 0
    for key, value in content.items():
        if isinstance(value, str):
            word_count += len(value.split())
        elif isinstance(value, list):  # FAQs
            for item in value:
                if isinstance(item, dict):
                    word_count += len(item.get('q', '').split())
                    word_count += len(item.get('a', '').split())

    faq_count = len(content.get('faq', []))
    source_count = len(sources)

    if word_count < min_words:
        failures.append(f"word_count ({word_count}) < {min_words}")

    if source_count < min_sources:
        failures.append(f"sources ({source_count}) < {min_sources}")

    if faq_count < min_faqs:
        failures.append(f"faqs ({faq_count}) < {min_faqs}")

    # Brand compliance check
    all_text = json.dumps(content)
    is_compliant, violations = check_brand_compliance(all_text)
    if not is_compliant:
        failures.append(f"brand_violations: {violations}")

    return QualityGateResult(
        passed=len(failures) == 0,
        failures=failures,
        word_count=word_count,
        faq_count=faq_count,
        source_count=source_count
    )

# =============================================================================
# SCHEDULED REFRESH SYSTEM
# =============================================================================

REFRESH_AGE_DAYS = 30  # Refresh content older than this
ENABLE_AUTO_REFRESH = True

def get_stale_pages(max_age_days: int = REFRESH_AGE_DAYS, limit: int = 10) -> List[Dict]:
    """Find pages that need refresh based on age"""
    if not ENABLE_AUTO_REFRESH:
        return []

    cutoff_date = (datetime.now() - timedelta(days=max_age_days)).isoformat()

    try:
        result = supabase.table('decision_nodes').select(
            'id, primary_keyword, url_slug, updated_at, word_count'
        ).eq('status', 'published').lt('updated_at', cutoff_date).order(
            'updated_at', desc=False
        ).limit(limit).execute()

        return result.data or []
    except:
        return []


def refresh_stale_content(limit: int = 5) -> Dict:
    """Refresh content for stale pages"""
    log(Status.INFO, f"Checking for stale content (>{REFRESH_AGE_DAYS} days old)...")

    stale_pages = get_stale_pages(limit=limit)

    if not stale_pages:
        log(Status.OK, "No stale pages found")
        return {'refreshed': 0, 'pages': []}

    log(Status.INFO, f"Found {len(stale_pages)} stale pages to refresh")

    refreshed = []

    for page in stale_pages:
        node_id = page['id']
        keyword = page['primary_keyword']

        log(Status.INFO, f"Refreshing: {keyword}", indent=1)

        # Re-generate content (will use SERP change detection)
        content_result = generate_content_for_node(node_id)

        if content_result.get('success'):
            # Re-generate internal links
            if ENABLE_INTERLINKING:
                generate_internal_links(node_id)

            # Re-publish to Webflow
            if ENABLE_SAFE_PUBLISHING:
                publish_to_webflow(node_id)

            refreshed.append(keyword)
            log(Status.OK, f"Refreshed: {keyword}", indent=1)
        else:
            log(Status.WARN, f"Failed to refresh: {keyword}", indent=1)

    return {'refreshed': len(refreshed), 'pages': refreshed}


def run_maintenance() -> Dict:
    """Run full maintenance cycle: refresh stale content, update sitemaps"""
    print("\n" + "="*70)
    print("🔧 SEI MAINTENANCE CYCLE")
    print("="*70)

    stats = {
        'stale_refreshed': 0,
        'sitemap_updated': False,
        'links_updated': 0
    }

    # 1. Refresh stale content
    refresh_result = refresh_stale_content(limit=5)
    stats['stale_refreshed'] = refresh_result['refreshed']

    # 2. Update sitemap
    if ENABLE_SITEMAP:
        log(Status.SITEMAP, "Regenerating sitemap...")
        save_sitemap_to_storage()
        stats['sitemap_updated'] = True

    # 3. Update internal links for all published pages (catch new connections)
    if ENABLE_INTERLINKING:
        log(Status.LINK, "Updating internal links for recent pages...")
        recent = supabase.table('decision_nodes').select('id').eq(
            'status', 'published'
        ).order('updated_at', desc=True).limit(10).execute()

        for page in (recent.data or []):
            result = generate_internal_links(page['id'])
            stats['links_updated'] += result.get('links_added', 0)

    print("\n" + "="*70)
    print("📊 MAINTENANCE COMPLETE")
    print("="*70)
    print(f"  Stale pages refreshed: {stats['stale_refreshed']}")
    print(f"  Sitemap updated:       {'✅' if stats['sitemap_updated'] else '❌'}")
    print(f"  Links updated:         {stats['links_updated']}")
    print_session_costs()
    print("="*70)

    return stats


# =============================================================================
# CLASSIFIED KEYWORD DATA MODEL
# =============================================================================

@dataclass
class ClassifiedKeyword:
    """Result of keyword classification"""
    original_keyword: str
    equipment_type: str
    geo: Optional[str]
    modifier: Optional[str]
    brand: Optional[str]
    spoke_type: str  # 'hub', 'financing', 'for-sale', 'rental', 'brand', 'modifier'
    page_category: str  # 'hub' or 'spoke'
    commercial_score: float
    priority_score: float


def classify_keyword(keyword: str, volume: int = 0, kd: int = 0) -> Optional[ClassifiedKeyword]:
    """
    Use Claude to classify a keyword and determine what type of page to create.
    """
    log(Status.BRAIN, f"Classifying: {keyword}", indent=1)

    prompt = f"""Analyze this equipment financing keyword and classify it:

KEYWORD: "{keyword}"
SEARCH VOLUME: {volume}/month
KEYWORD DIFFICULTY: {kd}

Classify into:
1. Equipment Type: The main equipment (excavator, forklift, crane, semi-truck, etc.) or "general" if none
2. Geography: State name if mentioned (texas, california) or "none"
3. Modifier: Special qualifier (bad-credit, zero-down, startup, used) or "none"
4. Brand: Brand name if mentioned (caterpillar, john-deere, komatsu) or "none"
5. Spoke Type: What kind of page this should be:
   - "hub" = Main equipment page (just equipment name, no modifiers)
   - "financing" = Financing focused (contains financing, loan, lease)
   - "for-sale" = Buying focused (contains for sale, buy, purchase, price)
   - "rental" = Rental focused (contains rental, rent)
   - "brand" = Brand specific (contains brand name)
   - "modifier" = Has credit/payment modifier (bad credit, zero down, etc.)
6. Commercial Score: 0-10 how likely to convert (10 = ready to buy)

OUTPUT AS JSON:
{{
    "equipment_type": "excavator",
    "geo": "texas",
    "modifier": "bad-credit",
    "brand": "none",
    "spoke_type": "modifier",
    "commercial_score": 8.5
}}"""

    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        result = parse_json_safely(response.content[0].text)
        if not result:
            return None

        equipment = result.get('equipment_type', 'general')
        if equipment.lower() in ['none', 'general', '']:
            equipment = 'general-equipment'

        geo = result.get('geo')
        if geo and geo.lower() in ['none', '']:
            geo = None

        modifier = result.get('modifier')
        if modifier and modifier.lower() in ['none', '']:
            modifier = None

        brand = result.get('brand')
        if brand and brand.lower() in ['none', '']:
            brand = None

        spoke_type = result.get('spoke_type', 'financing')
        page_category = 'hub' if spoke_type == 'hub' else 'spoke'

        commercial_score = float(result.get('commercial_score', 7.0))

        # Calculate priority score (Golden Ratio)
        # Higher volume + lower difficulty + higher commercial intent = higher priority
        priority_score = (volume / max(kd, 1)) * (commercial_score / 10)

        return ClassifiedKeyword(
            original_keyword=keyword,
            equipment_type=equipment,
            geo=geo,
            modifier=modifier,
            brand=brand,
            spoke_type=spoke_type,
            page_category=page_category,
            commercial_score=commercial_score,
            priority_score=priority_score
        )

    except Exception as e:
        log(Status.WARN, f"Classification error: {e}", indent=1)
        return None


# =============================================================================
# DECISION ENGINE
# =============================================================================

def get_or_create_equipment_type(equipment_name: str) -> Optional[str]:
    """Get equipment type ID, creating if needed"""
    slug = generate_url_slug(equipment_name)

    # Check if exists
    result = supabase.table('equipment_types').select('id').eq('slug', slug).execute()
    if result.data:
        return result.data[0]['id']

    # Create new
    try:
        result = supabase.table('equipment_types').insert({
            'name': equipment_name.title(),
            'slug': slug
        }).execute()
        if result.data:
            log(Status.OK, f"Created equipment type: {equipment_name}", indent=1)
            return result.data[0]['id']
    except Exception as e:
        log(Status.WARN, f"Error creating equipment type: {e}", indent=1)

    return None


def check_page_exists(equipment_type_id: str, spoke_type: str, geo: Optional[str] = None, 
                      modifier: Optional[str] = None, brand_id: Optional[str] = None) -> Optional[str]:
    """Check if a page already exists, return node_id if so"""
    query = supabase.table('decision_nodes').select('id').eq('equipment_type_id', equipment_type_id)

    if spoke_type == 'hub':
        query = query.eq('page_category', 'hub')
    else:
        query = query.eq('spoke_type', spoke_type)

    if geo:
        query = query.eq('geo', geo)
    else:
        query = query.is_('geo', 'null')

    if modifier:
        query = query.eq('modifier', modifier)
    else:
        query = query.is_('modifier', 'null')

    if brand_id:
        query = query.eq('brand_id', brand_id)

    result = query.execute()
    if result.data:
        return result.data[0]['id']
    return None


def decide_and_queue(classified: ClassifiedKeyword, volume: int = 0, kd: int = 0) -> Dict:
    """
    Decision engine: Determine what pages need to be created for this keyword.
    Returns dict with actions taken.
    """
    result = {
        'keyword': classified.original_keyword,
        'actions': [],
        'pages_created': 0,
        'pages_skipped': 0
    }

    # Get or create equipment type
    equipment_type_id = get_or_create_equipment_type(classified.equipment_type)
    if not equipment_type_id:
        result['error'] = 'Failed to get equipment type'
        return result

    # Check if hub exists for this equipment
    hub_exists = check_page_exists(equipment_type_id, 'hub')

    # If this is a hub keyword and hub doesn't exist, create full cluster
    if classified.spoke_type == 'hub' and not hub_exists:
        log(Status.INFO, f"Creating full cluster for {classified.equipment_type}", indent=1)
        cluster_result = create_equipment_cluster(equipment_type_id, classified.equipment_type)
        result['actions'].append(f"Created cluster: {cluster_result.get('pages_created', 0)} pages")
        result['pages_created'] = cluster_result.get('pages_created', 0)
        return result

    # If hub doesn't exist but this is a spoke keyword, create hub first
    if not hub_exists:
        log(Status.INFO, f"Hub missing for {classified.equipment_type}, creating cluster first", indent=1)
        cluster_result = create_equipment_cluster(equipment_type_id, classified.equipment_type)
        result['actions'].append(f"Created cluster: {cluster_result.get('pages_created', 0)} pages")
        result['pages_created'] += cluster_result.get('pages_created', 0)

    # Now check if this specific spoke exists
    if classified.spoke_type != 'hub':
        spoke_exists = check_page_exists(
            equipment_type_id, 
            classified.spoke_type,
            classified.geo,
            classified.modifier
        )

        if spoke_exists:
            log(Status.SKIP, f"Page already exists for {classified.original_keyword}", indent=1)
            result['pages_skipped'] += 1
            result['actions'].append(f"Skipped: {classified.spoke_type} page exists")
        else:
            # Create the spoke
            spoke_result = create_spoke_page(
                equipment_type_id=equipment_type_id,
                equipment_name=classified.equipment_type,
                spoke_type=classified.spoke_type,
                keyword=classified.original_keyword,
                geo=classified.geo,
                modifier=classified.modifier,
                volume=volume,
                kd=kd
            )
            if spoke_result:
                result['pages_created'] += 1
                result['actions'].append(f"Created {classified.spoke_type} spoke")

    return result


# =============================================================================
# CLUSTER CREATION
# =============================================================================

def create_equipment_cluster(equipment_type_id: str, equipment_name: str) -> Dict:
    """
    Create a full hub + spoke cluster for an equipment type.
    """
    result = {'pages_created': 0, 'pages': []}

    # Get hub node ID if it exists
    hub_check = supabase.table('decision_nodes').select('id').eq(
        'equipment_type_id', equipment_type_id
    ).eq('page_category', 'hub').execute()

    hub_node_id = hub_check.data[0]['id'] if hub_check.data else None

    # Create hub if needed
    if not hub_node_id:
        slug = generate_url_slug(equipment_name)
        hub_data = {
            'primary_keyword': equipment_name.title(),
            'url_slug': slug,
            'normalized_decision_key': slug,  # Required field
            'node_type': 'hub',  # Required field
            'equipment_type_id': equipment_type_id,
            'page_category': 'hub',
            'spoke_type': None,
            'status': 'discovery',
            'commercial_score': 8.0
        }

        try:
            hub_result = supabase.table('decision_nodes').insert(hub_data).execute()
            if hub_result.data:
                hub_node_id = hub_result.data[0]['id']
                result['pages_created'] += 1
                result['pages'].append({'type': 'hub', 'id': hub_node_id})
                log(Status.OK, f"Created hub: {equipment_name}", indent=2)

                # Update equipment_types with hub_node_id
                supabase.table('equipment_types').update({
                    'hub_node_id': hub_node_id
                }).eq('id', equipment_type_id).execute()
        except Exception as e:
            log(Status.WARN, f"Error creating hub: {e}", indent=2)

    # Create default spokes
    default_spokes = [
        {'type': 'financing', 'keyword': f'{equipment_name} financing'},
        {'type': 'for-sale', 'keyword': f'{equipment_name} for sale'},
        {'type': 'rental', 'keyword': f'{equipment_name} rental'},
    ]

    for spoke in default_spokes:
        spoke_exists = check_page_exists(equipment_type_id, spoke['type'])
        if spoke_exists:
            continue

        spoke_result = create_spoke_page(
            equipment_type_id=equipment_type_id,
            equipment_name=equipment_name,
            spoke_type=spoke['type'],
            keyword=spoke['keyword'],
            parent_hub_id=hub_node_id
        )

        if spoke_result:
            result['pages_created'] += 1
            result['pages'].append({'type': spoke['type'], 'id': spoke_result})

    # Update hub with spoke grid
    if hub_node_id:
        update_hub_spoke_grid(hub_node_id, equipment_type_id)

    return result


def create_spoke_page(equipment_type_id: str, equipment_name: str, spoke_type: str,
                      keyword: str, geo: Optional[str] = None, modifier: Optional[str] = None,
                      brand_id: Optional[str] = None, parent_hub_id: Optional[str] = None,
                      volume: int = 0, kd: int = 0) -> Optional[str]:
    """Create a single spoke page"""

    # Get parent hub if not provided
    if not parent_hub_id:
        hub_result = supabase.table('decision_nodes').select('id').eq(
            'equipment_type_id', equipment_type_id
        ).eq('page_category', 'hub').execute()
        if hub_result.data:
            parent_hub_id = hub_result.data[0]['id']

    # Build URL slug
    slug_parts = [equipment_name]
    if spoke_type and spoke_type != 'financing':
        slug_parts.append(spoke_type.replace('-', ' '))
    else:
        slug_parts.append('financing')
    if geo:
        slug_parts.append(geo)
    if modifier:
        slug_parts.append(modifier)

    url_slug = generate_url_slug(' '.join(slug_parts))

    spoke_data = {
        'primary_keyword': keyword,
        'url_slug': url_slug,
        'normalized_decision_key': url_slug,  # Required field
        'node_type': 'spoke',  # Required field
        'equipment_type_id': equipment_type_id,
        'page_category': 'spoke',
        'spoke_type': spoke_type,
        'parent_hub_id': parent_hub_id,
        'geo': geo,
        'modifier': modifier,
        'brand_id': brand_id,
        'status': 'discovery',
        'node_total_volume': volume,
        'difficulty_score': kd
    }

    try:
        result = supabase.table('decision_nodes').insert(spoke_data).execute()
        if result.data:
            node_id = result.data[0]['id']
            log(Status.OK, f"Created spoke: {keyword}", indent=2)
            return node_id
    except Exception as e:
        log(Status.WARN, f"Error creating spoke: {e}", indent=2)

    return None


def update_hub_spoke_grid(hub_node_id: str, equipment_type_id: str):
    """Update hub's spoke_grid with links to all spokes"""
    spokes = supabase.table('decision_nodes').select(
        'url_slug, spoke_type, primary_keyword'
    ).eq('equipment_type_id', equipment_type_id).eq('page_category', 'spoke').execute()

    spoke_grid = []
    type_titles = {
        'financing': 'Financing',
        'for-sale': 'For Sale',
        'rental': 'Rental',
        'brand': 'Brands',
        'modifier': 'Special Options'
    }

    for spoke in (spokes.data or []):
        spoke_grid.append({
            'url': f"/equipment/{spoke['url_slug']}/",
            'title': type_titles.get(spoke['spoke_type'], spoke['spoke_type'].title()),
            'keyword': spoke['primary_keyword']
        })

    supabase.table('decision_nodes').update({
        'spoke_grid': spoke_grid
    }).eq('id', hub_node_id).execute()


# =============================================================================
# HUNTER INTELLIGENCE (Search & Scrape)
# =============================================================================

BAD_DOMAINS = [
    'reddit.com', 'pinterest.com', 'youtube.com', 'facebook.com', 
    'twitter.com', 'instagram.com', 'quora.com', 'tiktok.com'
]

def search_competitors(keyword: str, limit: int = 5) -> List[Dict]:
    """Search Google for competitor content"""
    if not firecrawl or not ENABLE_HUNTER_INTELLIGENCE:
        return []

    if not check_budget('firecrawl', 1):
        return []

    log(Status.SEARCH, f"Searching: {keyword}", indent=1)

    try:
        results = firecrawl.search(query=keyword, limit=10)
        increment_budget('firecrawl', 1)

        clean_results = []
        results_data = []

        if hasattr(results, 'web') and results.web:
            results_data = results.web
        elif isinstance(results, dict) and 'data' in results:
            results_data = results['data']
        elif isinstance(results, list):
            results_data = results

        for item in results_data:
            url = item.url if hasattr(item, 'url') else item.get('url', '')
            title = item.title if hasattr(item, 'title') else item.get('title', '')

            if any(bad in url.lower() for bad in BAD_DOMAINS):
                continue
            if url.lower().endswith('.pdf'):
                continue

            clean_results.append({
                'url': url,
                'title': title[:100],
                'snippet': (item.description if hasattr(item, 'description') 
                           else item.get('description', ''))[:200]
            })

            if len(clean_results) >= limit:
                break

        log(Status.OK, f"Found {len(clean_results)} quality sources", indent=1)
        return clean_results

    except Exception as e:
        log(Status.WARN, f"Search error: {e}", indent=1)
        return []


def scrape_url(url: str) -> Optional[str]:
    """Scrape content from a URL"""
    if not firecrawl:
        return None

    log(Status.INFO, f"Scraping: {url[:50]}...", indent=2)

    try:
        result = firecrawl.scrape(url=url, formats=['markdown'], only_main_content=True)

        content = None
        if hasattr(result, 'markdown'):
            content = result.markdown
        elif isinstance(result, dict):
            content = result.get('markdown') or result.get('content')

        if content:
            log(Status.OK, f"Scraped {len(content)} chars", indent=2)
            return content[:15000]

    except Exception as e:
        log(Status.WARN, f"Scrape error: {e}", indent=2)

    return None


def gather_competitor_intelligence(keyword: str) -> Tuple[str, List[str]]:
    """Full intelligence gathering: search + scrape"""
    search_results = search_competitors(keyword)

    if not search_results:
        return "", []

    combined_content = []
    sources = []

    for result in search_results[:3]:
        content = scrape_url(result['url'])
        if content:
            combined_content.append(f"--- Source: {result['url']} ---\n{content[:5000]}")
            sources.append(result['url'])
            increment_budget('firecrawl', 1)

    return "\n\n".join(combined_content), sources


# =============================================================================
# CONTENT GENERATION
# =============================================================================

BRAND_VOICE = """
EquipFlow is a modern equipment financing marketplace connecting businesses with lenders.

RULES:
- Write in second person (you/your)
- Never say "we offer" or "our loans" - EquipFlow connects, doesn't lend
- Include specific numbers (rates, terms, timelines)
- Be professional but approachable
- End sections with soft CTAs
"""

def generate_content_for_node(node_id: str, use_intelligence: bool = True) -> Dict:
    """Generate content for a decision node with full intelligence pipeline"""
    result = {'success': False, 'node_id': node_id, 'lsi_keywords': [], 'schema_generated': False}

    # Get node data
    node = supabase.table('decision_nodes').select(
        '*, equipment_types!decision_nodes_equipment_type_id_fkey(name, slug)'
    ).eq('id', node_id).single().execute()

    if not node.data:
        result['error'] = 'Node not found'
        return result

    node_data = node.data

    # ==========================================================================
    # VALIDATION - Ensure required fields before generation
    # ==========================================================================
    validation_errors = []

    if not node_data.get('equipment_type_id'):
        validation_errors.append('Missing equipment_type_id')

    if not node_data.get('equipment_types'):
        validation_errors.append('No linked equipment_types record')

    if not node_data.get('page_category'):
        validation_errors.append('Missing page_category (hub/spoke)')

    if node_data.get('page_category') == 'spoke':
        if not node_data.get('spoke_type'):
            validation_errors.append('Missing spoke_type for spoke page')
        if not node_data.get('parent_hub_id'):
            validation_errors.append('Missing parent_hub_id for spoke page')

    if not node_data.get('url_slug'):
        validation_errors.append('Missing url_slug')

    if validation_errors:
        error_msg = f"Validation failed: {', '.join(validation_errors)}"
        log(Status.FAIL, error_msg, indent=1)
        result['error'] = error_msg
        result['validation_errors'] = validation_errors
        return result

    # ==========================================================================

    equipment_name = node_data.get('equipment_types', {}).get('name', 'Equipment')
    keyword = node_data.get('primary_keyword', equipment_name)
    page_category = node_data.get('page_category', 'spoke')
    spoke_type = node_data.get('spoke_type', 'financing')
    geo = node_data.get('geo')
    modifier = node_data.get('modifier')
    url_slug = node_data.get('url_slug', '')

    log(Status.BRAIN, f"Generating content for: {keyword}", indent=1)

    # Gather intelligence if enabled
    competitor_context = ""
    sources = []
    lsi_keywords = []
    serp_urls = []

    if use_intelligence and ENABLE_HUNTER_INTELLIGENCE and firecrawl:
        # Check SERP change detection
        search_results = search_competitors(keyword)

        if search_results:
            serp_urls = [r['url'] for r in search_results]
            new_hash = hashlib.md5(json.dumps(serp_urls).encode()).hexdigest()

            serp_changed, old_hash = check_serp_changed(keyword, new_hash)

            if serp_changed:
                if old_hash:
                    log(Status.INFO, f"SERP changed: {old_hash[:8]}... → {new_hash[:8]}...", indent=1)

                # Scrape competitors
                for search_result in search_results[:3]:
                    content = scrape_url(search_result['url'])
                    if content:
                        competitor_context += f"--- Source: {search_result['url']} ---\n{content[:5000]}\n\n"
                        sources.append(search_result['url'])
                        increment_budget('firecrawl', 1)

                # Extract LSI keywords
                if competitor_context:
                    lsi_keywords = extract_lsi_keywords(competitor_context, keyword)
                    result['lsi_keywords'] = lsi_keywords
                    log(Status.OK, f"Found LSI: {', '.join(lsi_keywords[:5])}...", indent=1)

                    # Explorer mode - find new opportunities
                    if ENABLE_EXPLORER:
                        expansion_kws = extract_expansion_keywords(competitor_context, keyword)
                        if expansion_kws:
                            queued = expand_territory(expansion_kws, keyword)
                            result['opportunities_queued'] = queued

                # Save SERP hash
                save_serp_snapshot(node_id, serp_urls)
            else:
                log(Status.SKIP, f"SERP unchanged ({new_hash[:8]}...) - using cached intel", indent=1)

    # Build prompt based on page type
    lsi_string = ", ".join(lsi_keywords[:8]) if lsi_keywords else ""

    if page_category == 'hub':
        prompt = build_hub_prompt(equipment_name, keyword, competitor_context, lsi_string)
    else:
        prompt = build_spoke_prompt(equipment_name, keyword, spoke_type, geo, modifier, competitor_context, lsi_string)

    # Generate with Claude
    if not check_budget('claude', 4000):
        result['error'] = 'Claude budget exceeded'
        return result

    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )
        increment_budget('claude', 4000)

        content = parse_json_safely(response.content[0].text)
        if not content:
            result['error'] = 'Failed to parse content'
            return result

        # Sanitize brand content
        for key in content:
            if isinstance(content[key], str):
                content[key] = sanitize_brand_content(content[key])
            elif isinstance(content[key], list):  # FAQs
                for i, item in enumerate(content[key]):
                    if isinstance(item, dict):
                        content[key][i] = {
                            'q': sanitize_brand_content(item.get('q', '')),
                            'a': sanitize_brand_content(item.get('a', ''))
                        }

        # Calculate word count
        word_count = sum(len(str(v).split()) for v in content.values() if isinstance(v, str))
        for faq in content.get('faq', []):
            word_count += len(faq.get('q', '').split()) + len(faq.get('a', '').split())

        # Validate quality gates
        gate_result = validate_quality_gates(content, sources)

        if not gate_result.passed:
            log(Status.WARN, f"Quality gates failed: {gate_result.failures}", indent=1)
        else:
            log(Status.OK, f"Quality gates passed: {word_count} words, {gate_result.faq_count} FAQs", indent=1)

        # Generate schema JSON-LD
        schema_json = None
        if ENABLE_SCHEMA:
            schema_json = generate_schema_json(
                seo_title=content.get('seo_title', keyword),
                meta_desc=content.get('meta_desc', ''),
                url_slug=url_slug,
                equipment_type=equipment_name,
                geo=geo,
                faq_list=content.get('faq', [])
            )
            result['schema_generated'] = True
            log(Status.OK, f"Generated schema with {len(schema_json.get('@graph', []))} types", indent=1)

        # Update node with content versioning
        # Note: lsi_keywords stored in result dict, not DB (column may not exist)

        # ==========================================================================
        # AUTO-GENERATE SHORT DESCRIPTION if not in content
        # ==========================================================================
        if not content.get('short_description'):
            if page_category == 'hub':
                short_desc = f"Compare {equipment_name.lower()} financing, rental & buying options"
            elif spoke_type == 'financing':
                short_desc = f"Get flexible {equipment_name.lower()} financing options"
            elif spoke_type == 'rental':
                short_desc = f"Rent quality {equipment_name.lower()} for your project"
            elif spoke_type == 'for-sale':
                short_desc = f"Find {equipment_name.lower()} for sale near you"
            else:
                short_desc = f"Explore {equipment_name.lower()} options"
            content['short_description'] = short_desc
            log(Status.OK, f"Auto-generated short_description: {short_desc}", indent=1)
        # ==========================================================================

        update_data = {
            'generated_content': content,
            'word_count': word_count,
            'faq_count': len(content.get('faq', [])),
            'sources_used': sources,
            'status': 'ready_to_publish' if gate_result.passed else 'blocked_quality',
            'publish_gate_status': 'passed' if gate_result.passed else 'blocked',
            'publish_gate_reason': gate_result.failures if gate_result.failures else None,
            'short_description': content.get('short_description', '')
        }

        if schema_json:
            update_data['schema_json'] = schema_json
            update_data['has_schema_markup'] = True

        # Use content versioning if enabled
        if ENABLE_CONTENT_VERSIONING:
            update_content_safe(node_id, content, word_count)
            # Still update other fields
            supabase.table('decision_nodes').update({
                k: v for k, v in update_data.items() if k != 'generated_content' and k != 'word_count'
            }).eq('id', node_id).execute()
        else:
            supabase.table('decision_nodes').update(update_data).eq('id', node_id).execute()

        result['success'] = True
        result['word_count'] = word_count
        result['gate_passed'] = gate_result.passed
        log(Status.OK, f"Generated {word_count} words", indent=1)

    except Exception as e:
        result['error'] = str(e)
        log(Status.FAIL, f"Generation error: {e}", indent=1)

    return result


def build_hub_prompt(equipment_name: str, keyword: str, competitor_context: str = "", lsi_keywords: str = "") -> str:
    """Build prompt for hub page"""
    context_section = f"\nCOMPETITOR RESEARCH:\n{competitor_context[:8000]}" if competitor_context else ""
    lsi_section = f"\nLSI KEYWORDS TO INCLUDE: {lsi_keywords}" if lsi_keywords else ""

    return f"""{BRAND_VOICE}

Create comprehensive content for the {equipment_name} hub page on EquipFlow.
TARGET WORD COUNT: 1200+ words total

TARGET KEYWORD: {keyword}
{lsi_section}
{context_section}

SEO REQUIREMENTS:
- Use "{keyword}" in the FIRST SENTENCE
- Include "{keyword}" or variations 4-6 times naturally
- Include LSI keywords throughout if provided
- Write for HUMANS first, optimize for Google

OUTPUT JSON:
{{
    "seo_title": "string (max 60 chars, keyword at start)",
    "meta_desc": "string (max 155 chars, keyword in first 10 words)",
    "subheadline": "1-2 punchy sentences (15-25 words max) that hook the reader. Value proposition focused. NO quotes around it.",
    "short_description": "Brief card description (50-75 chars) for when this page appears in navigation/cards. Action-oriented.",
    "intro": "2-3 paragraphs introducing {equipment_name}, what it's used for, who needs it (200+ words). MUST start with a sentence containing '{keyword}'",
    "main_content": "Comprehensive guide with H2/H3 sections covering: types/categories, key features, common use cases, buying considerations (800+ words)",
    "features": "Why get {equipment_name} through EquipFlow - financing options, fast approval, all credit types (200+ words)",
    "faq": [
        {{"q": "question about {equipment_name}?", "a": "detailed answer (60+ words)"}},
        {{"q": "question", "a": "answer (60+ words)"}},
        {{"q": "question", "a": "answer (60+ words)"}},
        {{"q": "question", "a": "answer (60+ words)"}},
        {{"q": "question", "a": "answer (60+ words)"}}
    ]
}}"""


def build_spoke_prompt(equipment_name: str, keyword: str, spoke_type: str, 
                       geo: Optional[str], modifier: Optional[str], 
                       competitor_context: str = "", lsi_keywords: str = "") -> str:
    """Build prompt for spoke page"""
    context_section = f"\nCOMPETITOR RESEARCH:\n{competitor_context[:8000]}" if competitor_context else ""
    lsi_section = f"\nLSI KEYWORDS TO INCLUDE: {lsi_keywords}" if lsi_keywords else ""

    geo_text = f" in {geo}" if geo else ""
    modifier_text = f" ({modifier})" if modifier else ""

    spoke_instructions = {
        'financing': f"Focus on financing options, rates, terms, approval process for {equipment_name}{geo_text}",
        'for-sale': f"Focus on buying guide - new vs used, price ranges, what to look for when buying {equipment_name}",
        'rental': f"Focus on rental rates, when to rent vs buy, rental requirements for {equipment_name}",
        'brand': f"Focus on this specific brand of {equipment_name} - reputation, models, why choose this brand",
        'modifier': f"Focus on {modifier} financing options for {equipment_name} - who qualifies, how it works"
    }

    focus = spoke_instructions.get(spoke_type, spoke_instructions['financing'])

    return f"""{BRAND_VOICE}

Create content for {equipment_name} {spoke_type} page{geo_text}{modifier_text}.
TARGET WORD COUNT: 1000+ words total

TARGET KEYWORD: {keyword}
FOCUS: {focus}
{lsi_section}
{context_section}

SEO REQUIREMENTS:
- Use "{keyword}" in the FIRST SENTENCE
- Include "{keyword}" or variations 4-6 times naturally
- Include LSI keywords throughout if provided
- Write for HUMANS first, optimize for Google

OUTPUT JSON:
{{
    "seo_title": "string (max 60 chars, keyword at start)",
    "meta_desc": "string (max 155 chars, keyword in first 10 words)",
    "subheadline": "1-2 punchy sentences (15-25 words max) that hook the reader. Value proposition focused. NO quotes around it.",
    "short_description": "Brief card description (50-75 chars) for when this page appears in navigation/cards. Action-oriented, e.g. 'Get flexible financing for excavators' or 'Find quality used forklifts'.",
    "intro": "2-3 paragraphs addressing {keyword} (150+ words). MUST start with '{keyword}'",
    "main_content": "Detailed content with H2/H3 sections (600+ words)",
    "how_it_works": "Step by step process through EquipFlow (200+ words)",
    "features": "Key benefits and differentiators (150+ words)",
    "faq": [
        {{"q": "question about {spoke_type}", "a": "detailed answer"}},
        {{"q": "question", "a": "answer"}},
        {{"q": "question", "a": "answer"}},
        {{"q": "question", "a": "answer"}},
        {{"q": "question", "a": "answer"}}
    ]
}}"""


# =============================================================================
# HERO IMAGE GENERATION
# =============================================================================

def build_image_prompt(equipment: str, geo: Optional[str] = None) -> str:
    """Build DALL-E prompt for hero image"""
    equipment_lower = equipment.lower()

    if any(w in equipment_lower for w in ['excavator', 'bulldozer', 'loader', 'backhoe']):
        setting = "active commercial construction site"
        action = "in operation, moving earth"
    elif 'crane' in equipment_lower:
        setting = "major construction project"
        action = "lifting heavy materials"
    elif any(w in equipment_lower for w in ['forklift', 'pallet']):
        setting = "modern warehouse facility"
        action = "lifting pallets"
    elif any(w in equipment_lower for w in ['semi', 'truck', 'trailer']):
        setting = "highway or trucking depot"
        action = "on an open highway"
    elif any(w in equipment_lower for w in ['tractor', 'combine']):
        setting = "expansive agricultural field"
        action = "working in fields"
    else:
        setting = "professional industrial facility"
        action = "in a commercial environment"

    geo_context = f"in {geo}" if geo else ""

    return f"""Ultra-realistic professional photograph of a {equipment} {action} at a {setting} {geo_context}.
Professional commercial photography, natural daylight, clean composition, high detail, modern equipment.
No text, logos, watermarks. No people facing camera. Warm professional color grading."""


def generate_hero_image(node_id: str) -> Dict:
    """Generate and store hero image for a node"""
    result = {'success': False, 'node_id': node_id}

    if not openai_client or not ENABLE_HERO_IMAGES:
        result['error'] = 'Image generation disabled'
        return result

    if not check_budget('dalle', 1):
        result['error'] = 'DALL-E budget exceeded'
        return result

    # Get node data
    node = supabase.table('decision_nodes').select(
        'url_slug, geo, equipment_types!decision_nodes_equipment_type_id_fkey(name)'
    ).eq('id', node_id).single().execute()

    if not node.data:
        result['error'] = 'Node not found'
        return result

    equipment_name = node.data.get('equipment_types', {}).get('name', 'Equipment')
    geo = node.data.get('geo')
    url_slug = node.data.get('url_slug', 'image')

    log(Status.IMAGE, f"Generating image for: {equipment_name}", indent=1)

    prompt = build_image_prompt(equipment_name, geo)

    try:
        response = openai_client.images.generate(
            model="dall-e-3",
            prompt=prompt,
            size=IMAGE_SIZE,
            quality="standard",
            n=1,
            response_format="url"
        )
        increment_budget('dalle', 1)

        # Download image
        image_url = response.data[0].url
        image_response = requests.get(image_url, timeout=60)
        image_bytes = image_response.content

        # Compress to WebP
        if PILLOW_AVAILABLE:
            img = Image.open(BytesIO(image_bytes))
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')
            output = BytesIO()
            img.save(output, format='WebP', quality=WEBP_QUALITY, optimize=True)
            image_bytes = output.getvalue()
            file_ext = 'webp'
            content_type = 'image/webp'
        else:
            file_ext = 'png'
            content_type = 'image/png'

        # Upload to Supabase Storage
        filename = f"{url_slug}-{int(time.time())}.{file_ext}"

        supabase.storage.from_(STORAGE_BUCKET).upload(
            path=filename,
            file=image_bytes,
            file_options={"content-type": content_type}
        )

        public_url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(filename)

        # Update node
        alt_text = f"Professional {equipment_name} available for financing - EquipFlow"
        supabase.table('decision_nodes').update({
            'hero_image_url': public_url,
            'hero_image_alt': alt_text,
            'hero_image_status': 'generated'
        }).eq('id', node_id).execute()

        result['success'] = True
        result['image_url'] = public_url
        log(Status.OK, f"Image uploaded: {filename}", indent=1)

    except Exception as e:
        result['error'] = str(e)
        log(Status.FAIL, f"Image error: {e}", indent=1)

    return result


# =============================================================================
# WEBFLOW PUBLISHING
# =============================================================================

WEBFLOW_API_BASE = "https://api.webflow.com/v2"

def get_webflow_headers() -> Dict:
    return {
        'Authorization': f'Bearer {WEBFLOW_API_TOKEN}',
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }


def markdown_to_html(text: str) -> str:
    """Convert markdown to HTML for Webflow rich text fields"""
    if not text:
        return ""

    if MARKDOWN_AVAILABLE:
        # Convert markdown to HTML with common extensions
        html = markdown.markdown(text, extensions=['extra', 'nl2br'])
        return html
    else:
        # Fallback: basic manual conversion if markdown library not available
        html = text
        # Headers
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        # Bold
        html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
        # Italic
        html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', html)
        # Line breaks to paragraphs
        paragraphs = html.split('\n\n')
        html = ''.join(f'<p>{p.strip()}</p>' for p in paragraphs if p.strip())
        return html


def prepare_webflow_payload(node: Dict) -> Dict:
    """Transform node data to Webflow payload"""
    content = node.get('generated_content', {})
    if isinstance(content, str):
        content = json.loads(content) if content else {}

    # Generate FAQ HTML
    faq_html = ""
    faqs = content.get('faq', [])
    if faqs:
        faq_html = '<div class="faq-list">'
        for qa in faqs:
            q = qa.get('q', '')
            a = qa.get('a', '')
            if q and a:
                faq_html += f'<div class="faq-item"><h3>{q}</h3><p>{a}</p></div>'
        faq_html += '</div>'

    # Convert markdown fields to HTML for Webflow rich text
    intro_html = markdown_to_html(content.get('intro', ''))
    main_content_html = markdown_to_html(content.get('main_content', ''))
    how_it_works_html = markdown_to_html(content.get('how_it_works', ''))
    features_html = markdown_to_html(content.get('features', ''))

    payload = {
        'isArchived': False,
        'isDraft': False,
        'fieldData': {
            'name': to_title_case(node.get('primary_keyword', '')),
            'slug': node.get('url_slug', ''),
            'page-type': node.get('page_category', 'spoke'),
            'spoke-type': node.get('spoke_type', ''),
            'seo-title': content.get('seo_title', to_title_case(node.get('primary_keyword', ''))),
            'meta-description': content.get('meta_desc', ''),
            'subheadline': content.get('subheadline', ''),
            'short-description': node.get('short_description', '') or content.get('short_description', ''),
            'intro': intro_html,
            'main-content': main_content_html,
            'how-it-works': how_it_works_html,
            'financing-options': features_html,
            'faqs': faq_html,
            'word-count': node.get('word_count', 0),
            'supabase-id': str(node.get('id', '')),
        }
    }

    # Remove empty values
    payload['fieldData'] = {k: v for k, v in payload['fieldData'].items() if v}

    # Add hero image
    if node.get('hero_image_url'):
        payload['fieldData']['hero-image'] = {'url': node['hero_image_url']}
        if node.get('hero_image_alt'):
            payload['fieldData']['hero-alt'] = node['hero_image_alt']

    return payload


def publish_to_webflow(node_id: str) -> Dict:
    """Publish a single node to Webflow"""
    result = {'success': False, 'node_id': node_id}

    if not WEBFLOW_API_TOKEN or not WEBFLOW_COLLECTION_ID:
        result['error'] = 'Webflow not configured'
        return result

    if not check_kill_switch():
        result['error'] = 'Kill switch active'
        return result

    if not circuit_breaker.can_proceed():
        result['error'] = 'Circuit breaker open'
        return result

    # Get node
    node = supabase.table('decision_nodes').select('*').eq('id', node_id).single().execute()
    if not node.data:
        result['error'] = 'Node not found'
        return result

    node_data = node.data
    existing_item_id = node_data.get('webflow_item_id')

    # ==========================================================================
    # VALIDATION - Ensure page is complete before publishing
    # ==========================================================================
    validation_errors = []

    if not node_data.get('generated_content'):
        validation_errors.append('No generated content')

    if not node_data.get('hero_image_url'):
        validation_errors.append('No hero image')

    if not node_data.get('short_description'):
        validation_errors.append('No short description')

    if not node_data.get('page_category'):
        validation_errors.append('Missing page_category')

    if node_data.get('page_category') == 'spoke' and not node_data.get('spoke_type'):
        validation_errors.append('Missing spoke_type')

    if validation_errors:
        error_msg = f"Publish validation failed: {', '.join(validation_errors)}"
        log(Status.WARN, error_msg, indent=1)
        log(Status.INFO, "Run 'generate' and 'image' commands first, or fix missing fields", indent=1)
        result['error'] = error_msg
        result['validation_errors'] = validation_errors
        return result
    # ==========================================================================

    payload = prepare_webflow_payload(node_data)

    try:
        if existing_item_id:
            # Update existing
            url = f"{WEBFLOW_API_BASE}/collections/{WEBFLOW_COLLECTION_ID}/items/{existing_item_id}"
            response = requests.patch(url, headers=get_webflow_headers(), json=payload)
        else:
            # Create new
            url = f"{WEBFLOW_API_BASE}/collections/{WEBFLOW_COLLECTION_ID}/items"
            response = requests.post(url, headers=get_webflow_headers(), json=payload)

        if response.status_code in [200, 201, 202]:
            data = response.json()
            item_id = data.get('id') or existing_item_id
            url_slug = node_data.get('url_slug', '')

            # Update Supabase
            supabase.table('decision_nodes').update({
                'webflow_item_id': item_id,
                'webflow_status': 'published',
                'status': 'published'
            }).eq('id', node_id).execute()

            circuit_breaker.record_success()
            result['success'] = True
            result['webflow_item_id'] = item_id
            result['url'] = f"{SITE_URL}/equipment/{url_slug}/"
            log(Status.PUBLISH, f"Published: {url_slug}", indent=1)

            # Ping IndexNow for instant indexing
            if ENABLE_INDEXNOW and INDEXNOW_KEY:
                ping_indexnow_single(result['url'])
        else:
            circuit_breaker.record_failure()
            result['error'] = f"HTTP {response.status_code}: {response.text[:200]}"
            log(Status.FAIL, f"Publish error: {result['error']}", indent=1)

        time.sleep(RATE_LIMIT_DELAY)

    except Exception as e:
        circuit_breaker.record_failure()
        result['error'] = str(e)

    return result


def publish_items_live(item_ids: List[str]) -> bool:
    """Publish staged items to live site"""
    if not item_ids:
        return True

    url = f"{WEBFLOW_API_BASE}/collections/{WEBFLOW_COLLECTION_ID}/items/publish"

    try:
        response = requests.post(
            url, 
            headers=get_webflow_headers(), 
            json={'itemIds': item_ids}
        )
        return response.status_code in [200, 202]
    except:
        return False


# =============================================================================
# AUTOPILOT PIPELINE
# =============================================================================

def process_keyword_full(keyword: str, volume: int = 0, kd: int = 0) -> Dict:
    """
    Full autopilot pipeline for a single keyword:
    Classify → Decide → Create Pages → Generate Content → Generate Images → Links → Publish → IndexNow
    """
    log(Status.INFO, f"Processing: {keyword}")
    result = {
        'keyword': keyword,
        'pages_created': 0,
        'content_generated': 0,
        'images_generated': 0,
        'links_generated': 0,
        'published': 0,
        'indexed': 0,
        'errors': []
    }

    # 1. Classify keyword
    classified = classify_keyword(keyword, volume, kd)
    if not classified:
        result['errors'].append('Classification failed')
        return result

    # 2. Decision engine
    decision = decide_and_queue(classified, volume, kd)
    result['pages_created'] = decision.get('pages_created', 0)

    if decision.get('error'):
        result['errors'].append(decision['error'])
        return result

    # 3. Get pages that need content
    equipment_type_id = get_or_create_equipment_type(classified.equipment_type)
    if not equipment_type_id:
        return result

    pages = supabase.table('decision_nodes').select('id').eq(
        'equipment_type_id', equipment_type_id
    ).eq('status', 'discovery').execute()

    published_urls = []

    # 4. Generate content + images + links for each page
    for page in (pages.data or []):
        node_id = page['id']

        # Generate content
        content_result = generate_content_for_node(node_id)
        if content_result.get('success'):
            result['content_generated'] += 1

            # Generate image
            image_result = generate_hero_image(node_id)
            if image_result.get('success'):
                result['images_generated'] += 1

            # Generate internal links
            if ENABLE_INTERLINKING:
                links_result = generate_internal_links(node_id)
                result['links_generated'] += links_result.get('links_added', 0)

            # Publish to Webflow
            if ENABLE_SAFE_PUBLISHING:
                publish_result = publish_to_webflow(node_id)
                if publish_result.get('success'):
                    result['published'] += 1
                    if publish_result.get('url'):
                        published_urls.append(publish_result['url'])
                        result['indexed'] += 1  # IndexNow called in publish_to_webflow

    return result


def run_autopilot(csv_path: Optional[str] = None, limit: int = MAX_PAGES_PER_RUN) -> Dict:
    """
    Main autopilot function - process keywords from CSV, GSC, or database queue.
    """
    print("\n" + "="*70)
    print("🤖 SEI v6.2.0 - UNIFIED AUTOPILOT")
    print("="*70)

    stats = {
        'keywords_processed': 0,
        'pages_created': 0,
        'content_generated': 0,
        'images_generated': 0,
        'links_generated': 0,
        'published': 0,
        'indexed': 0,
        'errors': []
    }

    # Check kill switch
    if not check_kill_switch():
        print("🛑 Kill switch active - aborting")
        return stats

    keywords_to_process = []

    # Load from CSV if provided
    if csv_path and os.path.exists(csv_path):
        log(Status.INFO, f"Loading keywords from {csv_path}")
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    kw = row.get('Keyword', row.get('keyword', '')).strip()
                    vol = int(row.get('Volume', row.get('volume', 0)) or 0)
                    diff = int(row.get('KD', row.get('Difficulty', 0)) or 0)
                    if kw:
                        keywords_to_process.append({'keyword': kw, 'volume': vol, 'kd': diff})
        except Exception as e:
            log(Status.FAIL, f"CSV error: {e}")

    # Or load from Ahrefs queue in database
    if not keywords_to_process:
        log(Status.INFO, "Loading from market_intelligence_ahrefs queue")
        try:
            result = supabase.table('market_intelligence_ahrefs').select(
                'keyword, volume, kd'
            ).eq('status', 'unprocessed').order('volume', desc=True).limit(limit).execute()

            for row in (result.data or []):
                keywords_to_process.append({
                    'keyword': row['keyword'],
                    'volume': row.get('volume', 0),
                    'kd': row.get('kd', 0)
                })
        except:
            pass

    if not keywords_to_process:
        log(Status.INFO, "No keywords to process")
        return stats

    log(Status.OK, f"Found {len(keywords_to_process)} keywords to process")

    # Process keywords (with canary approach)
    processed = 0
    webflow_ids = []

    for kw_data in keywords_to_process[:limit]:
        if not circuit_breaker.can_proceed():
            log(Status.LOCK, "Circuit breaker open - stopping")
            break

        result = process_keyword_full(
            kw_data['keyword'],
            kw_data['volume'],
            kw_data['kd']
        )

        stats['keywords_processed'] += 1
        stats['pages_created'] += result.get('pages_created', 0)
        stats['content_generated'] += result.get('content_generated', 0)
        stats['images_generated'] += result.get('images_generated', 0)
        stats['links_generated'] += result.get('links_generated', 0)
        stats['published'] += result.get('published', 0)
        stats['indexed'] += result.get('indexed', 0)
        stats['errors'].extend(result.get('errors', []))

        processed += 1

        # Canary check after first batch
        if processed == CANARY_BATCH_SIZE:
            error_rate = len(stats['errors']) / max(processed, 1)
            if error_rate > 0.2:
                log(Status.WARN, f"High error rate ({error_rate:.0%}) - stopping")
                break
            log(Status.CANARY, f"Canary batch OK - continuing")

        # Mark as processed in Ahrefs table
        try:
            supabase.table('market_intelligence_ahrefs').update({
                'status': 'processed',
                'processed_at': datetime.now().isoformat()
            }).eq('keyword', kw_data['keyword']).execute()
        except:
            pass

    # Update sitemap after batch
    if ENABLE_SITEMAP and stats['published'] > 0:
        log(Status.SITEMAP, "Updating sitemap...")
        save_sitemap_to_storage()

    # Final summary
    print("\n" + "="*70)
    print("📊 AUTOPILOT COMPLETE")
    print("="*70)
    print(f"  Keywords processed: {stats['keywords_processed']}")
    print(f"  Pages created:      {stats['pages_created']}")
    print(f"  Content generated:  {stats['content_generated']}")
    print(f"  Images generated:   {stats['images_generated']}")
    print(f"  Links generated:    {stats['links_generated']}")
    print(f"  Published:          {stats['published']}")
    print(f"  Indexed (IndexNow): {stats['indexed']}")
    print(f"  Errors:             {len(stats['errors'])}")

    # Session costs
    print_session_costs()
    print("="*70)

    return stats


# =============================================================================
# CLI INTERFACE
# =============================================================================

def print_help():
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║          SEI v6.2.0 - UNIFIED SELF-EXPANDING INTELLIGENCE            ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  AUTOPILOT COMMANDS:                                                 ║
║    python sei_unified.py autopilot                                   ║
║    python sei_unified.py autopilot --csv keywords.csv                ║
║    python sei_unified.py autopilot --limit 20                        ║
║                                                                      ║
║  MAINTENANCE COMMANDS:                                               ║
║    python sei_unified.py maintenance      (refresh stale + sitemap)  ║
║    python sei_unified.py refresh          (refresh stale pages)      ║
║    python sei_unified.py relink           (update all internal links)║
║                                                                      ║
║  MANUAL COMMANDS:                                                    ║
║    python sei_unified.py process <keyword>                           ║
║    python sei_unified.py cluster <equipment>                         ║
║    python sei_unified.py generate <node_id>                          ║
║    python sei_unified.py image <node_id>                             ║
║    python sei_unified.py links <node_id>                             ║
║    python sei_unified.py publish <node_id>                           ║
║                                                                      ║
║  INDEXING & SITEMAP:                                                 ║
║    python sei_unified.py indexnow <url>                              ║
║    python sei_unified.py sitemap                                     ║
║                                                                      ║
║  GSC COMMANDS:                                                       ║
║    python sei_unified.py gsc-discover                                ║
║    python sei_unified.py gsc-rankings                                ║
║                                                                      ║
║  SAFETY COMMANDS:                                                    ║
║    python sei_unified.py status                                      ║
║    python sei_unified.py kill-on                                     ║
║    python sei_unified.py kill-off                                    ║
║    python sei_unified.py reset-breaker                               ║
║                                                                      ║
║  IMPORT COMMANDS:                                                    ║
║    python sei_unified.py import-ahrefs <csv_path>                    ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """)


def show_status():
    """Show system status"""
    print("\n" + "="*60)
    print("📊 SEI v6.2.0 STATUS")
    print("="*60)

    # Kill switch
    kill_active = not check_kill_switch()
    print(f"\n🔧 Safety Controls:")
    print(f"   Kill Switch:     {'🛑 ACTIVE' if kill_active else '✅ Off'}")
    print(f"   Circuit Breaker: {'🛑 OPEN' if circuit_breaker.is_open else '✅ Closed'}")

    # API status
    print(f"\n🔌 API Status:")
    print(f"   Claude:          {'✅ Ready' if claude_client else '❌ Not configured'}")
    print(f"   OpenAI:          {'✅ Ready' if openai_client else '⚠️ Disabled'}")
    print(f"   Firecrawl:       {'✅ Ready' if firecrawl else '⚠️ Disabled'}")
    print(f"   Webflow:         {'✅ Ready' if WEBFLOW_API_TOKEN else '❌ Not configured'}")
    print(f"   GSC:             {'✅ Ready' if gsc_service else '⚠️ Not configured'}")
    print(f"   IndexNow:        {'✅ Ready' if INDEXNOW_KEY else '⚠️ Not configured'}")

    # Core Features
    print(f"\n⚙️  Core Features:")
    print(f"   Hunter Intel:    {'✅ On' if ENABLE_HUNTER_INTELLIGENCE else '❌ Off'}")
    print(f"   Hero Images:     {'✅ On' if ENABLE_HERO_IMAGES else '❌ Off'}")
    print(f"   Interlinking:    {'✅ On' if ENABLE_INTERLINKING else '❌ Off'}")
    print(f"   IndexNow:        {'✅ On' if ENABLE_INDEXNOW else '❌ Off'}")
    print(f"   GSC Integration: {'✅ On' if ENABLE_GSC_INTEGRATION else '❌ Off'}")
    print(f"   Feedback Loop:   {'✅ On' if ENABLE_FEEDBACK_LOOP else '❌ Off'}")

    # Intelligence Features (from main.py v5.0)
    print(f"\n🧠 Intelligence Layer:")
    print(f"   Schema JSON-LD:  {'✅ On' if ENABLE_SCHEMA else '❌ Off'}")
    print(f"   Explorer Mode:   {'✅ On' if ENABLE_EXPLORER else '❌ Off'}")
    print(f"   SERP Detection:  {'✅ On' if ENABLE_SERP_CHANGE_DETECTION else '❌ Off'}")
    print(f"   Content Version: {'✅ On' if ENABLE_CONTENT_VERSIONING else '❌ Off'}")
    print(f"   Budget Control:  {'✅ On' if ENABLE_BUDGET_CONTROL else '❌ Off'}")

    # Database stats
    try:
        discovery = supabase.table('decision_nodes').select('id', count='exact').eq('status', 'discovery').execute()
        ready = supabase.table('decision_nodes').select('id', count='exact').eq('status', 'ready_to_publish').execute()
        published = supabase.table('decision_nodes').select('id', count='exact').eq('status', 'published').execute()
        blocked = supabase.table('decision_nodes').select('id', count='exact').eq('status', 'blocked_quality').execute()

        print(f"\n📋 Page Queue:")
        print(f"   Discovery:       {discovery.count or 0}")
        print(f"   Ready to Publish:{ready.count or 0}")
        print(f"   Published:       {published.count or 0}")
        print(f"   Blocked Quality: {blocked.count or 0}")

        # Cost estimate based on pages with content
        total_pages = (ready.count or 0) + (published.count or 0)
        est_cost = total_pages * 0.14  # $0.14 per page estimate
        print(f"\n💰 Estimated Spend:")
        print(f"   Pages processed: {total_pages}")
        print(f"   Est. total cost: ${est_cost:.2f}")
    except:
        pass

    # Ahrefs queue
    try:
        ahrefs = supabase.table('market_intelligence_ahrefs').select('id', count='exact').eq('status', 'unprocessed').execute()
        discovered = supabase.table('market_intelligence_ahrefs').select('id', count='exact').like('source', 'discovered_%').execute()
        print(f"\n🔭 Keyword Queue:")
        print(f"   Unprocessed:     {ahrefs.count or 0}")
        print(f"   Auto-discovered: {discovered.count or 0}")
    except:
        pass

    print("="*60)


def main():
    """Main CLI entry point"""
    init_clients()

    if len(sys.argv) < 2:
        print_help()
        return

    command = sys.argv[1]

    if command == 'autopilot':
        csv_path = None
        limit = MAX_PAGES_PER_RUN

        for i, arg in enumerate(sys.argv[2:]):
            if arg == '--csv' and i + 3 < len(sys.argv):
                csv_path = sys.argv[i + 3]
            elif arg == '--limit' and i + 3 < len(sys.argv):
                limit = int(sys.argv[i + 3])

        run_autopilot(csv_path, limit)

    elif command == 'process' and len(sys.argv) > 2:
        keyword = ' '.join(sys.argv[2:])
        result = process_keyword_full(keyword)
        print(json.dumps(result, indent=2))
        print_session_costs()

    elif command == 'cluster' and len(sys.argv) > 2:
        equipment = sys.argv[2]
        equipment_id = get_or_create_equipment_type(equipment)
        if equipment_id:
            result = create_equipment_cluster(equipment_id, equipment)
            print(json.dumps(result, indent=2, default=str))

    elif command == 'generate' and len(sys.argv) > 2:
        node_id = sys.argv[2]
        result = generate_content_for_node(node_id)
        print(json.dumps(result, indent=2))

    elif command == 'image' and len(sys.argv) > 2:
        node_id = sys.argv[2]
        result = generate_hero_image(node_id)
        print(json.dumps(result, indent=2))

    elif command == 'publish' and len(sys.argv) > 2:
        node_id = sys.argv[2]
        result = publish_to_webflow(node_id)
        print(json.dumps(result, indent=2))

    elif command == 'status':
        show_status()

    elif command == 'kill-on':
        try:
            supabase.table('publishing_control').update({
                'publishing_enabled': False
            }).eq('id', 1).execute()
            log(Status.LOCK, "Kill switch ACTIVATED")
        except Exception as e:
            log(Status.FAIL, f"Error: {e}")

    elif command == 'kill-off':
        try:
            supabase.table('publishing_control').update({
                'publishing_enabled': True
            }).eq('id', 1).execute()
            log(Status.OK, "Kill switch deactivated")
        except Exception as e:
            log(Status.FAIL, f"Error: {e}")

    elif command == 'reset-breaker':
        circuit_breaker.reset()

    # Maintenance commands (v6.2.0)
    elif command == 'maintenance':
        run_maintenance()

    elif command == 'refresh':
        limit = 5
        if len(sys.argv) > 2:
            try:
                limit = int(sys.argv[2])
            except:
                pass
        result = refresh_stale_content(limit=limit)
        print(json.dumps(result, indent=2))
        print_session_costs()

    elif command == 'relink':
        log(Status.LINK, "Updating internal links for all published pages...")
        pages = supabase.table('decision_nodes').select('id, primary_keyword').eq(
            'status', 'published'
        ).execute()

        total_links = 0
        for page in (pages.data or []):
            result = generate_internal_links(page['id'])
            total_links += result.get('links_added', 0)
            log(Status.OK, f"Updated: {page['primary_keyword']}", indent=1)

        print(f"\n✅ Updated {len(pages.data or [])} pages with {total_links} total links")

    # Links and indexing commands
    elif command == 'links' and len(sys.argv) > 2:
        node_id = sys.argv[2]
        result = generate_internal_links(node_id)
        print(json.dumps(result, indent=2, default=str))

    elif command == 'indexnow' and len(sys.argv) > 2:
        url = sys.argv[2]
        result = ping_indexnow([url])
        print(json.dumps(result, indent=2))

    elif command == 'sitemap':
        sitemap = generate_sitemap()
        if sitemap:
            print(sitemap)
            url = save_sitemap_to_storage()
            if url:
                print(f"\nSaved to: {url}")

    elif command == 'gsc-discover':
        if not gsc_service:
            log(Status.WARN, "GSC not configured")
        else:
            opportunities = discover_keyword_opportunities()
            print(f"\nTop {len(opportunities)} keyword opportunities:\n")
            for opp in opportunities[:20]:
                print(f"  {opp['keyword']}")
                print(f"    Impressions: {opp['impressions']}, Position: {opp['position']:.1f}")

    elif command == 'gsc-rankings':
        if not gsc_service:
            log(Status.WARN, "GSC not configured")
        else:
            tracking = track_page_rankings()
            print(f"\nTracked {len(tracking)} pages")

    elif command == 'gsc-queue':
        if not gsc_service:
            log(Status.WARN, "GSC not configured")
        else:
            queued = queue_gsc_opportunities()
            print(f"\nQueued {queued} keyword opportunities from GSC")

    elif command == 'import-ahrefs' and len(sys.argv) > 2:
        csv_path = sys.argv[2]
        log(Status.INFO, f"Importing Ahrefs data from {csv_path}")

        try:
            imported = 0
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                batch = []

                for row in reader:
                    keyword = row.get('Keyword', row.get('keyword', '')).strip().lower()
                    if not keyword:
                        continue

                    batch.append({
                        'keyword': keyword,
                        'volume': int(row.get('Volume', 0) or 0),
                        'kd': int(row.get('KD', row.get('Difficulty', 0)) or 0),
                        'traffic_potential': int(row.get('Traffic potential', 0) or 0),
                        'cpc': float(row.get('CPC', 0) or 0),
                        'status': 'unprocessed'
                    })

                    if len(batch) >= 100:
                        supabase.table('market_intelligence_ahrefs').upsert(
                            batch, on_conflict='keyword'
                        ).execute()
                        imported += len(batch)
                        batch = []

                if batch:
                    supabase.table('market_intelligence_ahrefs').upsert(
                        batch, on_conflict='keyword'
                    ).execute()
                    imported += len(batch)

            log(Status.OK, f"Imported {imported} keywords")
        except Exception as e:
            log(Status.FAIL, f"Import error: {e}")

    elif command == 'help':
        print_help()

    else:
        print(f"Unknown command: {command}")
        print_help()


if __name__ == '__main__':
    main()