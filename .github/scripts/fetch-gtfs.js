/**
 * fetch-gtfs.js — server-side GTFS-RT decoder for VectorWatch
 * Fetches vehicle positions from open transit APIs, decodes protobuf,
 * writes compact JSON to data/land/{CC}.json for same-origin serving.
 */
'use strict';
const fetch = require('node-fetch');
const protobuf = require('protobufjs');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '../../data/land');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

// Full GTFS-RT proto2 schema — includes extension ranges so protobufjs
// can gracefully skip any extension fields (1000-1999) instead of throwing.
const PROTO_SCHEMA = `
syntax = "proto2";
message FeedMessage {
  required FeedHeader header = 1;
  repeated FeedEntity entity = 2;
  extensions 1000 to 1999;
}
message FeedHeader {
  required string gtfs_realtime_version = 1;
  optional uint32 incrementality = 2;
  optional uint64 timestamp = 3;
  extensions 1000 to 1999;
}
message FeedEntity {
  required string id = 1;
  optional bool is_deleted = 2;
  optional TripUpdate trip_update = 3;
  optional VehiclePosition vehicle = 4;
  optional Alert alert = 5;
  extensions 1000 to 1999;
}
message TripUpdate {
  optional TripDescriptor trip = 1;
  optional VehicleDescriptor vehicle = 3;
  extensions 1000 to 1999;
}
message VehiclePosition {
  optional TripDescriptor trip = 1;
  optional VehicleDescriptor vehicle = 8;
  optional Position position = 2;
  optional uint32 current_stop_sequence = 3;
  optional string stop_id = 7;
  optional int32 current_status = 4;
  optional uint64 timestamp = 5;
  optional int32 congestion_level = 6;
  optional int32 occupancy_status = 9;
  extensions 1000 to 1999;
}
message TripDescriptor {
  optional string trip_id = 1;
  optional string route_id = 5;
  optional string direction_id = 6;
  optional string start_time = 2;
  optional string start_date = 3;
  optional int32 schedule_relationship = 4;
  extensions 1000 to 1999;
}
message VehicleDescriptor {
  optional string id = 1;
  optional string label = 2;
  optional string license_plate = 3;
  extensions 1000 to 1999;
}
message Position {
  required float latitude = 1;
  required float longitude = 2;
  optional float bearing = 3;
  optional double odometer = 4;
  optional float speed = 5;
}
message Alert {
  extensions 1000 to 1999;
}
`;

// Patch protobufjs Reader to skip unknown wire types gracefully.
// Wire type 4 (end group): tag already consumed, nothing more to skip.
// Wire types 6/7: undefined by spec; treat as 8/4 bytes respectively (best-effort).
(function patchReader() {
  const Reader = protobuf.Reader;
  const orig = Reader.prototype.skipType;
  Reader.prototype.skipType = function(wireType) {
    if (wireType === 4) { return this; }
    if (wireType === 6) { this.pos += 8; return this; }
    if (wireType === 7) { this.pos += 4; return this; }
    return orig.call(this, wireType);
  };
}());

// Decompress gzip if magic bytes detected (some servers omit Content-Encoding header).
async function maybeDecompress(buf) {
  if (buf[0] === 0x1f && buf[1] === 0x8b) {
    return new Promise((resolve, reject) =>
      zlib.gunzip(buf, (err, r) => err ? reject(err) : resolve(r))
    );
  }
  return buf;
}

// Robust FeedMessage decode — if full decode fails, fall back to per-entity parsing
// so that one malformed entity (or non-standard extension) doesn't lose all data.
function robustDecode(root, buf) {
  const FeedMessage = root.lookupType('FeedMessage');
  const FeedEntity  = root.lookupType('FeedEntity');

  try {
    return FeedMessage.decode(new Uint8Array(buf));
  } catch (firstErr) {
    console.warn(`  Full decode failed (${firstErr.message}), attempting per-entity fallback…`);
    const entities = [];
    const r = protobuf.Reader.create(new Uint8Array(buf));
    try {
      while (r.pos < r.len) {
        const tag = r.uint32();
        const fn = tag >>> 3, wt = tag & 7;
        if (fn === 2 && wt === 2) {
          // FeedEntity (field 2, length-delimited)
          const len = r.uint32();
          if (r.pos + len > r.len) break;
          const entitySlice = buf.slice(r.pos, r.pos + len);
          try { entities.push(FeedEntity.decode(new Uint8Array(entitySlice))); } catch (_) {}
          r.pos += len;
        } else if (wt <= 5) {
          r.skipType(wt);  // skip header / unknown fields
        } else {
          break;  // truly unknown wire type in outer message — stop
        }
      }
    } catch (_) { /* stop on any outer read error */ }
    console.warn(`  Per-entity fallback recovered ${entities.length} entities`);
    return { entity: entities };
  }
}

// SE: per-operator URLs (GTFS Sweden 3 Realtime)
// No national endpoint exists — must query each operator separately.
const SE_OPERATORS = ['ul', 'otraf', 'klt', 'skane', 'varm', 'dt', 'xt', 'vastmanland'];

// Feed definitions — free/open feeds first.
// FR and CH removed: PRIM does not offer GTFS-RT vehicle positions;
// opentransportdata.swiss vehicle positions endpoint does not exist.
const FEEDS = [
  {
    code: 'NO',
    url: 'https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions',
    headers: { 'ET-Client-Name': 'ThorstenGru-VectorWatch' },
  },
  {
    code: 'NL',
    url: 'https://gtfs.ovapi.nl/nl/vehiclePositions.pb',
    headers: {},
    robustDecode: true,  // OV-API uses non-standard extension wire types
  },
  {
    code: 'DE',
    // VBB Berlin/Brandenburg — publicly accessible combined GTFS-RT feed
    url: 'https://production.gtfsrt.vbb.de/data',
    headers: {},
  },
  {
    code: 'SE',
    // opendata.samtrafiken.se (not openapi) — per-operator, requires Accept-Encoding
    operators: SE_OPERATORS,
    urlTemplate: `https://opendata.samtrafiken.se/gtfs-rt-sweden/{op}/VehiclePositionsSweden.pb`,
    headers: { 'Accept-Encoding': 'gzip, deflate' },
    requiresKey: 'SE_API_KEY',
  },
];

async function fetchAndDecode(url, headers, root, useRobust) {
  const res = await fetch(url, { headers, timeout: 20000 });
  if (!res.ok) {
    console.warn(`  HTTP ${res.status} from ${url.split('?')[0]}`);
    return null;
  }
  const rawBuf = await res.buffer();
  const buf = await maybeDecompress(rawBuf);
  console.log(`  ${rawBuf.length} bytes raw → ${buf.length} bytes decoded`);
  return useRobust ? robustDecode(root, buf)
                   : root.lookupType('FeedMessage').decode(new Uint8Array(buf));
}

function extractVehicles(msg, countryCode) {
  const vehicles = [];
  for (const entity of (msg.entity || [])) {
    try {
      const vp = entity.vehicle;
      if (!vp || !vp.position) continue;
      const p = vp.position;
      if (p.latitude == null || p.longitude == null) continue;
      if (Math.abs(p.latitude) > 90 || Math.abs(p.longitude) > 180) continue;
      if (p.latitude === 0 && p.longitude === 0) continue;
      vehicles.push({
        id: `${countryCode}-${vp.vehicle?.id || vp.vehicleId || entity.id}`,
        lat: p.latitude,
        lng: p.longitude,
        bearing: Math.round(p.bearing || 0),
        speed: Math.round((p.speed || 0) * 3.6),  // m/s → km/h
        line: vp.trip?.route_id || vp.trip?.routeId || '—',
        tripId: vp.trip?.trip_id || vp.trip?.tripId || '—',
        dest: '—',
        routeType: 3,
        label: vp.vehicle?.label || vp.vehicle?.id || entity.id,
        operator: countryCode,
      });
    } catch (_) { /* skip malformed entity */ }
  }
  return vehicles;
}

async function decodeFeed(feed) {
  if (feed.requiresKey && !process.env[feed.requiresKey]) {
    console.log(`[${feed.code}] Skipping — ${feed.requiresKey} secret not set`);
    return null;
  }

  const root = protobuf.parse(PROTO_SCHEMA, { keepCase: true }).root;

  try {
    if (feed.operators) {
      // SE: fetch all operators in parallel, merge
      const key = process.env[feed.requiresKey] || '';
      console.log(`[${feed.code}] Fetching ${feed.operators.length} operator feeds in parallel…`);
      const results = await Promise.all(
        feed.operators.map(async op => {
          const url = feed.urlTemplate.replace('{op}', op) + `?key=${key}`;
          try {
            const msg = await fetchAndDecode(url, feed.headers, root, false);
            if (!msg) return [];
            const veh = extractVehicles(msg, feed.code);
            console.log(`  ${op}: ${veh.length} vehicles`);
            return veh;
          } catch (e) {
            console.warn(`  ${op}: ${e.message}`);
            return [];
          }
        })
      );
      const vehicles = results.flat();
      console.log(`[${feed.code}] Total: ${vehicles.length} vehicles`);
      return { updated: Math.floor(Date.now() / 1000), count: vehicles.length, vehicles };
    } else {
      // Single URL feed
      console.log(`[${feed.code}] Fetching ${feed.url.split('?')[0]}…`);
      let msg;
      try {
        msg = await fetchAndDecode(feed.url, feed.headers, root, feed.robustDecode);
      } catch (e) {
        const hexDump = typeof e._buf !== 'undefined'
          ? '' : '';
        console.warn(`[${feed.code}] Decode error: ${e.message}`);
        return null;
      }
      if (!msg) return null;
      const vehicles = extractVehicles(msg, feed.code);
      console.log(`[${feed.code}] ${vehicles.length} vehicles`);
      return { updated: Math.floor(Date.now() / 1000), count: vehicles.length, vehicles };
    }
  } catch (e) {
    console.error(`[${feed.code}] Fatal: ${e.message}`);
    return null;
  }
}

async function main() {
  for (const feed of FEEDS) {
    const result = await decodeFeed(feed);
    if (result && result.vehicles.length > 0) {
      const outPath = path.join(OUT_DIR, `${feed.code}.json`);
      fs.writeFileSync(outPath, JSON.stringify(result));
      console.log(`[${feed.code}] ✓ Wrote ${result.count} vehicles → data/land/${feed.code}.json`);
    }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
