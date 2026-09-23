import argparse
import os
import math
import logging
import time
import gc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import firebase_admin
from firebase_admin import credentials, firestore
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from google_auth_httplib2 import AuthorizedHttp
import httplib2
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys, codecs

# Ensure UTF-8 output (fixes UnicodeEncodeError on Windows)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables from .env file
load_dotenv()

# DB toggle: --db new (default) | --db old
_db_parser = argparse.ArgumentParser(add_help=False)
_db_parser.add_argument('--db', choices=['new', 'old'], default='new')
_db_args, _ = _db_parser.parse_known_args()
_DB_PREFIX = "NEW_" if _db_args.db == 'new' else ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------
# Firebase Configuration
# ---------------------------
FIREBASE_PROJECT_ID       = os.getenv(f"{_DB_PREFIX}FIREBASE_PROJECT_ID")
FIREBASE_PRIVATE_KEY_ID   = os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY_ID")
FIREBASE_PRIVATE_KEY      = os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n')
FIREBASE_CLIENT_EMAIL     = os.getenv(f"{_DB_PREFIX}FIREBASE_CLIENT_EMAIL")

# ---------------------------
# Google Sheets Configuration
# ---------------------------
GSPREAD_PROJECT_ID        = os.getenv("GSPREAD_PROJECT_ID")
GSPREAD_PRIVATE_KEY       = os.getenv("GSPREAD_PRIVATE_KEY", "").replace('\\n', '\n')
GSPREAD_CLIENT_EMAIL      = os.getenv("GSPREAD_CLIENT_EMAIL")
GOOGLE_SHEET_ID           = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"

FETCH_BATCH_SIZE = 5000
MAX_WORKERS      = min(32, (os.cpu_count() or 1) * 4)
CHUNK_SIZE       = 500
WRITE_BATCH_SIZE = 2000
WRITE_WORKERS    = 12

# ---------------------------
# Firestore Collection Name & Sheet Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = "Properties" if _db_args.db == 'new' else "acnTestProperties"
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Inventories new")

# ---------------------------
# Initialize Firebase (same pattern as inventories-from-firebase.py)
# ---------------------------
def initialize_firebase():
    creds_dict = {
        "type": "service_account",
        "project_id": FIREBASE_PROJECT_ID,
        "private_key_id": FIREBASE_PRIVATE_KEY_ID,
        "private_key": FIREBASE_PRIVATE_KEY,
        "client_email": FIREBASE_CLIENT_EMAIL,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(creds_dict))
    logger.info("Firebase initialized.")


# ---------------------------
# Helpers
# ---------------------------
def convert_unix_to_date(unix_timestamp):
    """Return YYYY-MM-DD for numeric timestamps (seconds or ms)."""
    try:
        if unix_timestamp in (None, "", 0, "0"):
            return ""
        if isinstance(unix_timestamp, str):
            try:
                unix_timestamp = float(unix_timestamp)
            except ValueError:
                return unix_timestamp
        ts = float(unix_timestamp)
        if ts > 9999999999:  # milliseconds
            ts = ts / 1000
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return str(unix_timestamp) if unix_timestamp else ""


def convert_unix_to_datetime(unix_timestamp):
    """Return YYYY-MM-DD HH:MM:SS for numeric timestamps (seconds or ms)."""
    try:
        if unix_timestamp in (None, "", 0, "0"):
            return ""
        if isinstance(unix_timestamp, str):
            try:
                unix_timestamp = float(unix_timestamp)
            except ValueError:
                return unix_timestamp
        ts = float(unix_timestamp)
        if ts > 9999999999:  # milliseconds
            ts = ts / 1000
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return str(unix_timestamp) if unix_timestamp else ""


def format_price(price_value):
    """Normalize price-like values to a clean string."""
    try:
        if price_value in (None, "", "null"):
            return ""
        if isinstance(price_value, dict):
            # If nested price objects arrive, pick common price fields
            for key in ("soldPrice", "totalAskPrice", "pricePerSqft", "value"):
                if key in price_value and price_value.get(key) not in (None, ""):
                    return str(price_value.get(key))
            return ""
        if isinstance(price_value, list):
            if len(price_value) > 0:
                return format_price(price_value[0])
            return ""
        if isinstance(price_value, str):
            cleaned_price = price_value.replace('₹', '').replace(',', '').replace('Rs', '').replace('INR', '').strip()
            try:
                return str(float(cleaned_price))
            except ValueError:
                return price_value
        return str(float(price_value))
    except Exception as e:
        print(f"⚠️ Error formatting price {price_value}: {e}")
        return str(price_value) if price_value else ""


def format_list(value):
    """Join arrays to comma-separated string."""
    if not value:
        return ""
    if isinstance(value, list):
        return ", ".join([str(v) for v in value if v is not None and v != ""])
    return str(value)


def safe_dict(value):
    """Use when Firestore may store null or non-dict for nested objects."""
    return value if isinstance(value, dict) else {}


def column_index_to_letter(col_num):
    """1-based column index to Excel column letters (1=A, 27=AA)."""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(65 + (col_num % 26)) + result
        col_num //= 26
    return result


def sheet_a1_range(sheet_name, a1_suffix):
    """Sheets API range with proper quoting for names with spaces or quotes."""
    safe = sheet_name.replace("'", "''")
    return f"'{safe}'!{a1_suffix}"


UNIFIED_HEADERS = [
    # Core identifiers
    "propertyId",
    "listingType",
    "propertyType",
    "assetType",
    "propertyCategory",
    "commercialPropertyType",
    "commercialSubType",
    # Stakeholder info
    "cpId",
    "agentName",
    "agentPhoneNumber",
    "kamId",
    "kamName",
    # Location
    "propertyName",
    "micromarket",
    "area",
    "zone",
    "communityType",
    "lat",
    "lng",
    "mapLocation",
    "driveLink",
    # Physical characteristics
    "sbua",
    "carpetArea",
    "plotArea",
    "facing",
    # Residential specifics
    "apartmentType",
    "structure",
    "noOfBedrooms",
    "extraRooms",
    "noOfBathrooms",
    "noOfBalconies",
    "balconyFacing",
    # Floor data
    "floorNumber",
    "referredFloorNumber",
    "totalFloors",
    # Commercial specifics
    "noOfSeats",
    "totalRooms",
    "waterSupply",
    "typeOfWaterSupply",
    # Plot specifics
    "plotNo",
    "plotLength",
    "plotBreadth",
    "oddSized",
    # Furnishing & building
    "furnishing",
    "ageOfTheBuilding",
    "possession",
    "availableFrom",
    "readyToMove",
    "handOverDate",
    # Financials
    "totalAskPrice",
    "pricePerSqft",
    # Rental info
    "rent",
    "deposit",
    "maintenance",
    "maintenanceAmount",
    "commissionType",
    "rentalIncome",
    "currentDeposit",
    "startDate",
    "endDate",
    # Tenant preferences
    "preferredTenants",
    "petsAllowed",
    "nonVegAllowed",
    # Features
    "cornerUnit",
    "exclusive",
    "ocReceived",
    # Legal
    "landKhata",
    "buildingKhata",
    "eKhata",
    "biappaApproved",
    "bdaApproved",
    # Amenities / extras
    "amenities",
    "parking",
    "uds",
    "suitableFor",
    # Media
    "photos",
    "videos",
    "documents",
    # Misc
    "extraDetails",
    "unitNumber",
    "status",
    "kamStatus",
    "dataStatus",
    "stage",
    # System metadata
    "added",
    "dateOfLastChecked",
    "lastModified",
    "ageOfInventory",
    "ageOfStatus",
    "source",
]


def build_row(item: dict):
    """Flatten a property dict into the unified header order."""
    geoloc = safe_dict(item.get("_geoloc"))
    pricing = safe_dict(item.get("pricing"))
    rental = safe_dict(item.get("rentalInfo"))
    tenant = safe_dict(item.get("tenantPreferences"))
    features = safe_dict(item.get("features"))
    legal = safe_dict(item.get("legalInfo"))
    media = safe_dict(item.get("media"))

    row_map = {
        "propertyId": item.get("propertyId", ""),
        "listingType": item.get("listingType", ""),
        "propertyType": item.get("propertyType", ""),
        "assetType": item.get("assetType", ""),
        "propertyCategory": item.get("propertyCategory", ""),
        "commercialPropertyType": item.get("commercialPropertyType", ""),
        "commercialSubType": item.get("commercialSubType", ""),
        "cpId": item.get("cpId", ""),
        "agentName": item.get("agentName", ""),
        "agentPhoneNumber": item.get("agentPhoneNumber", ""),
        "kamId": item.get("kamId", ""),
        "kamName": item.get("kamName", ""),
        "propertyName": item.get("propertyName", ""),
        "micromarket": item.get("micromarket", ""),
        "area": item.get("area", ""),
        "zone": item.get("zone", ""),
        "communityType": item.get("communityType", ""),
        "lat": geoloc.get("lat", ""),
        "lng": geoloc.get("lng", ""),
        "mapLocation": item.get("mapLocation", ""),
        "driveLink": item.get("driveLink", ""),
        "sbua": item.get("sbua", ""),
        "carpetArea": item.get("carpetArea", ""),
        "plotArea": item.get("plotArea", ""),
        "facing": item.get("facing", ""),
        "apartmentType": item.get("apartmentType", ""),
        "structure": item.get("structure", ""),
        "noOfBedrooms": item.get("noOfBedrooms", ""),
        "extraRooms": item.get("extraRooms", ""),
        "noOfBathrooms": item.get("noOfBathrooms", ""),
        "noOfBalconies": item.get("noOfBalconies", ""),
        "balconyFacing": item.get("balconyFacing", ""),
        "floorNumber": item.get("floorNumber", item.get("floorNo", "")),
        "referredFloorNumber": item.get("referredFloorNumber", ""),
        "totalFloors": item.get("totalFloors", ""),
        "noOfSeats": item.get("noOfSeats", ""),
        "totalRooms": item.get("totalRooms", ""),
        "waterSupply": item.get("waterSupply", ""),
        "typeOfWaterSupply": item.get("typeOfWaterSupply", ""),
        "plotNo": item.get("plotNo", ""),
        "plotLength": item.get("plotLength", ""),
        "plotBreadth": item.get("plotBreadth", ""),
        "oddSized": item.get("oddSized", ""),
        "furnishing": item.get("furnishing", ""),
        "ageOfTheBuilding": item.get("ageOfTheBuilding", ""),
        "possession": item.get("possession", ""),
        "availableFrom": convert_unix_to_date(item.get("availableFrom")),
        "readyToMove": item.get("readyToMove", ""),
        "handOverDate": convert_unix_to_date(item.get("handOverDate")),
        "totalAskPrice": format_price(pricing.get("totalAskPrice", "")),
        "pricePerSqft": format_price(pricing.get("pricePerSqft", "")),
        "rent": format_price(rental.get("rent", "")),
        "deposit": format_price(rental.get("deposit", "")),
        "maintenance": rental.get("maintenance", ""),
        "maintenanceAmount": format_price(rental.get("maintenanceAmount", "")),
        "commissionType": rental.get("commissionType", ""),
        "rentalIncome": format_price(rental.get("rentalIncome", "")),
        "currentDeposit": format_price(rental.get("currentDeposit", "")),
        "startDate": convert_unix_to_date(rental.get("startDate")),
        "endDate": convert_unix_to_date(rental.get("endDate")),
        "preferredTenants": format_list(tenant.get("preferredTenants")),
        "petsAllowed": tenant.get("petsAllowed", ""),
        "nonVegAllowed": tenant.get("nonVegAllowed", ""),
        "cornerUnit": features.get("cornerUnit", ""),
        "exclusive": features.get("exclusive", ""),
        "ocReceived": features.get("ocReceived", ""),
        "landKhata": legal.get("landKhata", ""),
        "buildingKhata": legal.get("buildingKhata", ""),
        "eKhata": legal.get("eKhata", ""),
        "biappaApproved": legal.get("biappaApproved", ""),
        "bdaApproved": legal.get("bdaApproved", ""),
        "amenities": format_list(item.get("amenities")),
        "parking": item.get("parking", ""),
        "uds": item.get("uds", ""),
        "suitableFor": item.get("suitableFor", ""),
        "photos": format_list(media.get("photos")),
        "videos": format_list(media.get("videos")),
        "documents": format_list(media.get("documents")),
        "extraDetails": item.get("extraDetails", ""),
        "unitNumber": item.get("unitNumber", ""),
        "status": item.get("status", ""),
        "kamStatus": item.get("kamStatus", ""),
        "dataStatus": item.get("dataStatus", ""),
        "stage": item.get("stage", ""),
        "added": convert_unix_to_datetime(item.get("added")),
        "dateOfLastChecked": convert_unix_to_datetime(item.get("dateOfLastChecked")),
        "lastModified": convert_unix_to_datetime(item.get("lastModified")),
        "ageOfInventory": item.get("ageOfInventory", ""),
        "ageOfStatus": item.get("ageOfStatus", ""),
        "source": item.get("source", ""),
    }
    return [row_map.get(key, "") for key in UNIFIED_HEADERS]


def build_row_new(item: dict):
    """Flatten a new-schema Properties doc into the unified header order."""
    geoloc = safe_dict(item.get("_geoloc"))
    # media is now an array of {type, url, ...} maps
    media_list = item.get("media", []) or []
    if not isinstance(media_list, list):
        media_list = []
    photos = ", ".join([m.get("url") or "" for m in media_list if isinstance(m, dict) and m.get("type") == "image"])
    videos = ", ".join([m.get("url") or "" for m in media_list if isinstance(m, dict) and m.get("type") == "video"])
    docs_list = item.get("documents", []) or []
    documents = ", ".join([str(d) for d in docs_list if d]) if isinstance(docs_list, list) else ""

    # pricing is now top-level
    total_ask = format_price(item.get("totalAskPrice", ""))
    price_sqft = format_price(item.get("pricePerSqft", ""))

    # legalInfo, features, rentalInfo may not exist — fall back to top-level fields
    legal = safe_dict(item.get("legalInfo"))
    rental = safe_dict(item.get("rentalInfo"))
    tenant = safe_dict(item.get("tenantPreferences"))

    row_map = {
        "propertyId":               item.get("propertyId", item.get("id", item.get("objectId", ""))),
        "listingType":              item.get("listingType", ""),
        "propertyType":             item.get("propertyType", ""),
        "assetType":                item.get("assetType", ""),
        "propertyCategory":         item.get("propertyCategory", ""),
        "commercialPropertyType":   item.get("commercialPropertyType", ""),
        "commercialSubType":        item.get("commercialSubType", item.get("apartmentSubType", "")),
        "cpId":                     item.get("cpId", ""),
        "agentName":                item.get("agentName", ""),
        "agentPhoneNumber":         item.get("agentPhoneNumber", ""),
        "kamId":                    item.get("kamId", ""),
        "kamName":                  item.get("kamName", ""),
        "propertyName":             item.get("propertyName", ""),
        "micromarket":              item.get("micromarket", ""),
        "area":                     item.get("area", ""),
        "zone":                     item.get("zone", ""),
        "communityType":            item.get("societyType", item.get("communityType", "")),
        "lat":                      geoloc.get("lat", ""),
        "lng":                      geoloc.get("lng", ""),
        "mapLocation":              item.get("location", item.get("mapLocation", "")),
        "driveLink":                item.get("driveLink", ""),
        "sbua":                     item.get("sbua", ""),
        "carpetArea":               item.get("carpet", item.get("carpetArea", "")),
        "plotArea":                 item.get("plotArea", ""),
        "facing":                   item.get("facing", ""),
        "apartmentType":            item.get("apartmentSubType", item.get("apartmentType", "")),
        "structure":                item.get("structure", ""),
        "noOfBedrooms":             item.get("bedroom", item.get("noOfBedrooms", "")),
        "extraRooms":               item.get("extraRooms", ""),
        "noOfBathrooms":            item.get("bathroom", item.get("noOfBathrooms", "")),
        "noOfBalconies":            item.get("noOfBalconies", ""),
        "balconyFacing":            item.get("balconyFacing", ""),
        "floorNumber":              item.get("floorNumber", item.get("floorNo", "")),
        "referredFloorNumber":      item.get("referredFloorNumber", ""),
        "totalFloors":              item.get("totalFloors", ""),
        "noOfSeats":                item.get("noOfSeats", ""),
        "totalRooms":               item.get("totalRooms", ""),
        "waterSupply":              item.get("waterSupply", ""),
        "typeOfWaterSupply":        item.get("typeOfWaterSupply", ""),
        "plotNo":                   item.get("plotNo", ""),
        "plotLength":               item.get("plotLength", ""),
        "plotBreadth":              item.get("plotBreadth", ""),
        "oddSized":                 item.get("oddSized", ""),
        "furnishing":               item.get("furnishing", ""),
        "ageOfTheBuilding":         item.get("ageOfBuilding", item.get("ageOfTheBuilding", "")),
        "possession":               item.get("possession", ""),
        "availableFrom":            convert_unix_to_date(item.get("availableFrom")),
        "readyToMove":              item.get("readyToMove", ""),
        "handOverDate":             convert_unix_to_date(item.get("handOverDate", item.get("handoverDate"))),
        "totalAskPrice":            total_ask,
        "pricePerSqft":             price_sqft,
        "rent":                     format_price(rental.get("rent", "")),
        "deposit":                  format_price(rental.get("deposit", "")),
        "maintenance":              rental.get("maintenance", ""),
        "maintenanceAmount":        format_price(rental.get("maintenanceAmount", "")),
        "commissionType":           rental.get("commissionType", ""),
        "rentalIncome":             format_price(rental.get("rentalIncome", "")),
        "currentDeposit":           format_price(rental.get("currentDeposit", "")),
        "startDate":                convert_unix_to_date(rental.get("startDate")),
        "endDate":                  convert_unix_to_date(rental.get("endDate")),
        "preferredTenants":         format_list(tenant.get("preferredTenants")),
        "petsAllowed":              tenant.get("petsAllowed", ""),
        "nonVegAllowed":            tenant.get("nonVegAllowed", ""),
        "cornerUnit":               str(item.get("isCornerUnit", item.get("cornerUnit", ""))),
        "exclusive":                str(item.get("isExclusive", item.get("exclusive", ""))),
        "ocReceived":               str(item.get("ocReceived", "")),
        "landKhata":                item.get("landKhata", legal.get("landKhata", "")),
        "buildingKhata":            item.get("buildingKhata", legal.get("buildingKhata", "")),
        "eKhata":                   str(item.get("hasEKhata", item.get("eKhata", legal.get("eKhata", "")))),
        "biappaApproved":           str(item.get("isBiapaApproved", item.get("biappaApproved", legal.get("biappaApproved", "")))),
        "bdaApproved":              str(item.get("isBdaApproved", item.get("bdaApproved", legal.get("bdaApproved", "")))),
        "amenities":                format_list(item.get("amenities")),
        "parking":                  item.get("parking", ""),
        "uds":                      item.get("uds", ""),
        "suitableFor":              item.get("suitableFor", ""),
        "photos":                   photos,
        "videos":                   videos,
        "documents":                documents,
        "extraDetails":             item.get("extraDetails", ""),
        "unitNumber":               item.get("unitNumber", ""),
        "status":                   item.get("status", ""),
        "kamStatus":                item.get("kamStatus", ""),
        "dataStatus":               item.get("dataStatus", item.get("currentStatus", "")),
        "stage":                    item.get("stage", ""),
        "added":                    convert_unix_to_datetime(item.get("added")),
        "dateOfLastChecked":        convert_unix_to_datetime(item.get("dateOfLastChecked")),
        "lastModified":             convert_unix_to_datetime(item.get("lastModified")),
        "ageOfInventory":           item.get("ageOfInventory", ""),
        "ageOfStatus":              item.get("ageOfStatus", ""),
        "source":                   item.get("source", ""),
    }
    return [row_map.get(key, "") for key in UNIFIED_HEADERS]


# ---------------------------
# Field projection — only the fields build_row / build_row_new actually read.
# Firestore ships ~5.4 KB/doc without a mask; unused blobs (media_bkp,
# media_backup, projectImages, ...) are ~40% of that.
# ---------------------------
SELECT_FIELDS = [
    "_geoloc", "added", "ageOfBuilding", "ageOfInventory", "ageOfStatus",
    "ageOfTheBuilding", "agentName", "agentPhoneNumber", "amenities",
    "apartmentSubType", "apartmentType", "area", "assetType", "availableFrom",
    "balconyFacing", "bathroom", "bdaApproved", "bedroom", "biappaApproved",
    "buildingKhata", "carpet", "carpetArea", "commercialPropertyType",
    "commercialSubType", "communityType", "cornerUnit", "cpId", "currentStatus",
    "dataStatus", "dateOfLastChecked", "documents", "driveLink", "eKhata",
    "exclusive", "extraDetails", "extraRooms", "facing", "features", "floorNo",
    "floorNumber", "furnishing", "handOverDate", "handoverDate", "hasEKhata",
    "id", "isBdaApproved", "isBiapaApproved", "isCornerUnit", "isExclusive",
    "kamId", "kamName", "kamStatus", "landKhata", "lastModified", "legalInfo",
    "listingType", "location", "mapLocation", "media", "micromarket",
    "noOfBalconies", "noOfBathrooms", "noOfBedrooms", "noOfSeats", "objectId",
    "ocReceived", "oddSized", "parking", "plotArea", "plotBreadth", "plotLength",
    "plotNo", "possession", "pricePerSqft", "pricing", "propertyCategory",
    "propertyId", "propertyName", "propertyType", "readyToMove",
    "referredFloorNumber", "rentalInfo", "sbua", "societyType", "source",
    "stage", "status", "structure", "suitableFor", "tenantPreferences",
    "totalAskPrice", "totalFloors", "totalRooms", "typeOfWaterSupply", "uds",
    "unitNumber", "waterSupply", "zone",
]


# ---------------------------
# Fetch data from Firestore (batched pagination, same as inventories-from-firebase.py)
# ---------------------------
def fetch_and_process(collection_name):
    """Fetch + convert in one pass — never stores raw docs, minimises peak RAM."""
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    PAGE_SIZE = 1000
    row_builder = build_row_new if _db_args.db == 'new' else build_row

    def fetch_page(last_doc):
        q = collection_ref.select(SELECT_FIELDS).order_by("__name__").limit(PAGE_SIZE)
        if last_doc is not None:
            q = q.start_after(last_doc)
        return list(q.stream())

    def to_row(doc):
        item = doc.to_dict()
        if not item:
            return None
        try:
            row = row_builder(item)
            return [
                "" if (isinstance(cell, float) and math.isnan(cell)) or cell is None
                else str(cell) for cell in row
            ]
        except Exception as e:
            logger.error(f"Row build error: {e}")
            return None

    logger.info(f"🚀 Fetching+processing '{collection_name}' | page={PAGE_SIZE}")
    t0 = time.time()
    rows = []
    count = 0

    with ThreadPoolExecutor(max_workers=2) as ex:
        future = ex.submit(fetch_page, None)
        while True:
            page = future.result()
            if not page:
                break
            future = ex.submit(fetch_page, page[-1]) if len(page) == PAGE_SIZE else None
            for doc in page:
                row = to_row(doc)
                if row:
                    rows.append(row)
                count += 1
            if count % 5000 == 0:
                logger.info(f"  📦 {count} docs ({count / (time.time() - t0):.0f}/s)")
            if future is None:
                break

    logger.info(f"✅ Fetched+processed {count} docs → {len(rows)} rows in {time.time()-t0:.2f}s")
    return rows


# ---------------------------
# Write data to Google Sheets (Sheets API v4: clear + update, same as inventories-from-firebase.py)
# ---------------------------
def _build_sheets_service():
    """One service per thread — googleapiclient http objects are not thread-safe."""
    sheets_creds_dict = {
        "type": "service_account",
        "project_id": GSPREAD_PROJECT_ID,
        "private_key": GSPREAD_PRIVATE_KEY,
        "client_email": GSPREAD_CLIENT_EMAIL,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    creds = Credentials.from_service_account_info(
        sheets_creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    authed_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=300))
    return build("sheets", "v4", http=authed_http, cache_discovery=False)


def write_to_google_sheet(data):
    if not data:
        logger.info("No data to write.")
        return
    try:
        service = _build_sheets_service()
        thread_local = threading.local()

        def service_for_thread():
            if not hasattr(thread_local, "service"):
                thread_local.service = _build_sheets_service()
            return thread_local.service

        num_cols = len(UNIFIED_HEADERS)
        end_col = column_index_to_letter(num_cols)
        quoted = f"'{GOOGLE_SHEET_NAME}'"
        t0 = time.time()

        all_rows = [UNIFIED_HEADERS] + data
        total = len(all_rows)

        logger.info("🧹 Clearing sheet...")
        service.spreadsheets().values().clear(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=sheet_a1_range(GOOGLE_SHEET_NAME, f"A:{end_col}"),
            body={}
        ).execute(num_retries=3)

        batches = []
        for i in range(0, total, WRITE_BATCH_SIZE):
            chunk = all_rows[i:i + WRITE_BATCH_SIZE]
            start_row = i + 1
            batches.append({
                "range": f"{quoted}!A{start_row}:{end_col}{start_row + len(chunk) - 1}",
                "values": chunk,
                "majorDimension": "ROWS"
            })

        logger.info(f"📝 Writing {total} rows in {len(batches)} parallel batches...")

        def write_batch(batch):
            service_for_thread().spreadsheets().values().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body={"valueInputOption": "USER_ENTERED", "data": [batch],
                      "includeValuesInResponse": False}
            ).execute(num_retries=3)
            return len(batch["values"])

        written = 0
        with ThreadPoolExecutor(max_workers=min(WRITE_WORKERS, len(batches))) as ex:
            futures = [ex.submit(write_batch, b) for b in batches]
            for f in as_completed(futures):
                written += f.result()

        logger.info(f"✅ Written {written} rows in {time.time()-t0:.2f}s")
    except Exception as e:
        logger.error(f"Error writing to Google Sheets: {e}", exc_info=True)


def main():
    start = time.time()
    logger.info("="*60)
    logger.info("⚡ ULTRA-FAST FIRESTORE TO SHEETS SYNC (New Inventory)")
    logger.info(f"🔧 {MAX_WORKERS} workers | fetch batch: {FETCH_BATCH_SIZE} | chunk: {CHUNK_SIZE}")
    logger.info(f"🔑 DB: {'NEW' if _DB_PREFIX else 'OLD'} | Collection: {FIRESTORE_COLLECTION_NAME}")
    logger.info("="*60)

    initialize_firebase()

    logger.info(f"\n📥 Fetching + processing '{FIRESTORE_COLLECTION_NAME}'")
    rows = fetch_and_process(FIRESTORE_COLLECTION_NAME)
    if not rows:
        logger.warning("No documents found.")
        return

    logger.info(f"\n📤 PHASE 3: Writing to Sheets '{GOOGLE_SHEET_NAME}'")
    write_to_google_sheet(rows)

    logger.info(f"\n🎉 Total time: {time.time()-start:.2f}s | {len(rows)} records")


if __name__ == "__main__":
    main()