#!/usr/bin/env python3
"""Eclipse Zephyr MCP — v0.1.0, phase S2 (public-read adapter).

Powered by Cyan Eclipse Technologies (cetech.ai).

Eclipse Zephyr — the Bluesky adapter: the family's open-protocol
reference implementation, using the AT Protocol's sanctioned records
and services while keeping CETech's review-first and engagement
limits fully in force. (Spec: eclipse-social-family-spec-20261025.md
SS17.)

V0.1 SCOPE — PUBLIC READS ONLY. Tools read the AT Protocol public API
(public.api.bsky.app, no auth) for named accounts: profiles, actor
search, author feeds, post threads. There is deliberately NO posting
surface: Zephyr posting requires an owner account and an approval
pack (Wave 2), and protocol possibility is not product permission —
review-first still binds (SS17). House engagement caps (core
policy_check) apply to any future pilot unchanged.

RATE DISCIPLINE: a minimum interval between live calls, a per-process
call budget, bounded retries on transient failures only, and a
hash-chained receipt for EVERY read (endpoint, actor, result count).
Analytics never invent a native view metric: output names the source
endpoint for every figure (SS6/SS17).

Transport: MCP over stdio (newline-delimited JSON-RPC 2.0), stdlib only.
State file: $ECLIPSE_ZEPHYR_STATE or ./data/state.json beside here.
Live network: public.api.bsky.app only. If the VM's egress blocks a
call, the tool returns a named error and the read is still receipted;
fixtures under fixtures/ drive the offline tests.
"""
import gzip
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

STATE_PATH = Path(os.environ.get(
    "ECLIPSE_ZEPHYR_STATE",
    Path(__file__).parent / "data" / "state.json"))

API_BASE = "https://public.api.bsky.app/xrpc/"
USER_AGENT = "eclipse-zephyr/0.1.0 (Cyan Eclipse Technologies; public reads)"
MIN_INTERVAL_S = 0.4        # rate discipline: min seconds between live calls
PROCESS_CALL_BUDGET = 60    # per-process live-call ceiling
TIMEOUT_S = 10

_last_call = [0.0]
_call_count = [0]


class ToolError(Exception):
    pass


def now() -> int:
    return int(time.time())


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


# --------------------------------------------------------------------- state
def empty_state() -> dict:
    return {"receipts": []}


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return empty_state()


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=1))


def emit_receipt(state: dict, rtype: str, detail: dict) -> dict:
    prev = state["receipts"][-1]["hash"] if state["receipts"] else "GENESIS"
    entry = {"seq": len(state["receipts"]) + 1, "ts": now(), "type": rtype,
             "prev_hash": prev, "detail": detail}
    entry["hash"] = sha(prev + canon({k: v for k, v in entry.items()
                                      if k != "hash"}))
    state["receipts"].append(entry)
    return entry


def verify_receipt_chain(receipts) -> bool:
    prev = "GENESIS"
    for e in receipts:
        if e.get("prev_hash") != prev:
            return False
        expect = sha(prev + canon({k: v for k, v in e.items() if k != "hash"}))
        if e.get("hash") != expect:
            return False
        prev = e["hash"]
    return True


# ------------------------------------------------------------------ transport
def fetch_json(url: str) -> dict:
    """Live GET with rate discipline + bounded retries. Transient
    failures (timeout, connection reset, 5xx) retry twice; 4xx does
    not. Raises ToolError with a named reason on final failure."""
    wait = MIN_INTERVAL_S - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait)
    last_err = None
    for attempt in range(3):
        if _call_count[0] >= PROCESS_CALL_BUDGET:
            raise ToolError(
                f"per-process live-call budget exhausted "
                f"({PROCESS_CALL_BUDGET}) — rate discipline (SS7)")
        _call_count[0] += 1
        _last_call[0] = time.time()
        req = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT,
                          "Accept-Encoding": "gzip",
                          # This VM's egress proxy intermittently
                          # truncates large chunked keep-alive bodies;
                          # Connection: close + gzip keeps bodies whole.
                          "Connection": "close"})
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code < 500:
                break
        except Exception as e:  # timeout / reset / DNS — transient class
            last_err = f"{type(e).__name__}: {e}"
        if attempt < 2:
            time.sleep(1.5 * (attempt + 1))
    raise ToolError(f"live read failed after retries: {last_err}")


def xrpc(method: str, params: dict, fetcher=None) -> dict:
    url = API_BASE + method + "?" + urllib.parse.urlencode(params)
    return (fetcher or fetch_json)(url)


# --------------------------------------------------------------- normalizers
def norm_profile(p: dict) -> dict:
    return {"did": p.get("did"), "handle": p.get("handle"),
            "display_name": p.get("displayName"),
            "description": p.get("description"),
            "avatar": p.get("avatar"),
            "followers_count": p.get("followersCount"),
            "follows_count": p.get("followsCount"),
            "posts_count": p.get("postsCount"),
            "created_at": p.get("createdAt"),
            "source_endpoint": "app.bsky.actor.getProfile"}


def norm_post(post: dict) -> dict:
    rec = post.get("record") or {}
    embed = post.get("embed") or {}
    etype = embed.get("$type", "")
    return {"uri": post.get("uri"), "cid": post.get("cid"),
            "author_handle": (post.get("author") or {}).get("handle"),
            "author_did": (post.get("author") or {}).get("did"),
            "text": rec.get("text"),
            "created_at": rec.get("createdAt"),
            "langs": rec.get("langs"),
            "like_count": post.get("likeCount"),
            "repost_count": post.get("repostCount"),
            "reply_count": post.get("replyCount"),
            "has_media": "images" in etype or "video" in etype,
            "embed_type": etype or None,
            "source_endpoint": "app.bsky.feed.getAuthorFeed"}


def read_and_receipt(state, rtype, endpoint, subject, fn):
    """Run one read; receipt it on success AND on failure."""
    try:
        out, count = fn()
    except ToolError as e:
        emit_receipt(state, rtype, {"endpoint": endpoint,
                                    "subject": subject, "status": "error",
                                    "error": str(e)})
        raise
    r = emit_receipt(state, rtype, {"endpoint": endpoint,
                                    "subject": subject, "status": "ok",
                                    "results": count})
    out["receipt_seq"] = r["seq"]
    return out


# --------------------------------------------------------------------- tools
def t_profile_read(state, args, fetcher=None):
    actor = str(args["actor"])

    def fn():
        raw = xrpc("app.bsky.actor.getProfile", {"actor": actor}, fetcher)
        return {"profile": norm_profile(raw),
                "retrieved_at": now(),
                "mode": "READ_API"}, 1

    return read_and_receipt(state, "profile_read",
                            "app.bsky.actor.getProfile", actor, fn)


def t_actor_search(state, args, fetcher=None):
    query = str(args["query"])
    limit = int(args.get("limit", 10))

    def fn():
        raw = xrpc("app.bsky.actor.searchActors",
                   {"q": query, "limit": limit}, fetcher)
        actors = [{"did": a.get("did"), "handle": a.get("handle"),
                   "display_name": a.get("displayName"),
                   "description": a.get("description"),
                   "followers_count": a.get("followersCount"),
                   "follows_count": a.get("followsCount"),
                   "posts_count": a.get("postsCount"),
                   "created_at": a.get("createdAt"),
                   "source_endpoint": "app.bsky.actor.searchActors"}
                  for a in raw.get("actors", [])]
        return {"actors": actors, "count": len(actors),
                "cursor": raw.get("cursor"), "retrieved_at": now(),
                "mode": "READ_API"}, len(actors)

    return read_and_receipt(state, "actor_search",
                            "app.bsky.actor.searchActors", query, fn)


def t_author_feed_read(state, args, fetcher=None):
    actor = str(args["actor"])
    limit = int(args.get("limit", 20))

    def fn():
        raw = xrpc("app.bsky.feed.getAuthorFeed",
                   {"actor": actor, "limit": limit,
                    "filter": "posts_no_replies"}, fetcher)
        posts = [norm_post(item.get("post") or {})
                 for item in raw.get("feed", [])]
        return {"actor": actor, "posts": posts, "count": len(posts),
                "cursor": raw.get("cursor"), "retrieved_at": now(),
                "mode": "READ_API",
                "note": "Feed contents depend on the source service; "
                        "figures are the protocol's own counts, named "
                        "per post — there is no invented view metric "
                        "(SS6/SS17)."}, len(posts)

    return read_and_receipt(state, "author_feed_read",
                            "app.bsky.feed.getAuthorFeed", actor, fn)


def t_post_thread_read(state, args, fetcher=None):
    uri = str(args["uri"])

    def fn():
        raw = xrpc("app.bsky.feed.getPostThread", {"uri": uri}, fetcher)
        thread = (raw.get("thread") or {})
        root = norm_post(thread.get("post") or {})
        replies = [norm_post((r or {}).get("post") or {})
                   for r in thread.get("replies", [])]
        for r in replies:
            r["source_endpoint"] = "app.bsky.feed.getPostThread"
        return {"root": root, "replies": replies,
                "reply_count": len(replies), "retrieved_at": now(),
                "mode": "READ_API"}, 1 + len(replies)

    return read_and_receipt(state, "post_thread_read",
                            "app.bsky.feed.getPostThread", uri, fn)


def t_receipt_list(state, args):
    return {"receipts": state["receipts"],
            "count": len(state["receipts"]),
            "chain_valid": verify_receipt_chain(state["receipts"])}


TOOLS = {
    "profile_read": (t_profile_read,
                     "Read one public Bluesky account profile through the AT Protocol public API: DID, handle, display name, description, avatar, follower/following/post counts, and account creation date, normalized into snake_case fields. Use when you need one known account's identity or counts; use actor_search instead to find accounts by name or keyword when you do not know the handle, author_feed_read for the account's posts, and post_thread_read for a single post and its replies. Live external read: calls the public endpoint app.bsky.actor.getProfile at public.api.bsky.app with no authentication and no account of your own. Rate discipline is built in: calls are spaced at least 0.4 seconds apart, transient failures (timeout, connection reset, HTTP 5xx) retry up to twice while HTTP 4xx does not, and the whole server process stops making live calls after 60. Every call, success or failure, appends a hash-chained receipt to the local state file, so the tool is read-only toward Bluesky but not side-effect free locally; on failure it returns a named error and the failed read is still receipted. Returns an object with profile (including source_endpoint naming the endpoint the figures came from), retrieved_at as epoch seconds, mode READ_API, and receipt_seq.",
                     {"actor": {"type": "string", "description": "Bluesky handle (for example 'name.bsky.social') or DID of the account to read. Required."}},
                     ["actor"],
                     {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
                     {"type": "object", "properties": {"profile": {"type": "object"}, "retrieved_at": {"type": "integer"}, "mode": {"type": "string"}, "receipt_seq": {"type": "integer"}}, "required": ["profile", "retrieved_at", "mode", "receipt_seq"]}),
    "actor_search": (t_actor_search,
                     "Search public Bluesky accounts by a free-text query through the AT Protocol public API and return normalized profile summaries (DID, handle, display name, description, follower/following/post counts, creation date) for each match. Use for discovery when you have a name or keyword but not an exact handle; use profile_read instead when you already know the exact account and want its single full profile, and author_feed_read to read a found account's posts. This is read-only discovery only: no following, liking, or any other engagement action exists in this server. Live external read against app.bsky.actor.searchActors at public.api.bsky.app with no authentication. The same rate discipline applies as for every live read here: at least 0.4 seconds between calls, up to two retries on transient failures only, and a 60 live-call ceiling per server process. Every call, success or failure, appends a hash-chained receipt to the local state file; failures return a named error and are still receipted. Returns an object with actors, count, cursor (pass back through the underlying API pagination concept; null when no further page is indicated), retrieved_at as epoch seconds, mode READ_API, and receipt_seq.",
                     {"query": {"type": "string", "description": "Free-text search string matched against public accounts by the search endpoint. Required."},
                      "limit": {"type": "integer", "description": "Maximum number of account summaries to return. Defaults to 10."}},
                     ["query"],
                     {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
                     {"type": "object", "properties": {"actors": {"type": "array", "items": {"type": "object"}}, "count": {"type": "integer"}, "cursor": {"type": ["string", "null"]}, "retrieved_at": {"type": "integer"}, "mode": {"type": "string"}, "receipt_seq": {"type": "integer"}}, "required": ["actors", "count", "retrieved_at", "mode", "receipt_seq"]}),
    "author_feed_read": (t_author_feed_read,
                         "Read a named public Bluesky account's recent original posts through the AT Protocol public API, excluding replies: each post is normalized to its URI, CID, author handle and DID, text, creation time, languages, the protocol's own like/repost/reply counts, and whether it carries image or video media. Use to review what one known account has posted; use profile_read for the account's profile and counts instead, actor_search to find the account first, and post_thread_read to expand one post into its reply thread. Live external read against app.bsky.feed.getAuthorFeed at public.api.bsky.app with no authentication. Counts are the protocol's own figures, named per post with source_endpoint, and no view metric is invented or estimated. Rate discipline: at least 0.4 seconds between live calls, up to two retries on transient failures only, and a 60 live-call ceiling per server process. Every call, success or failure, appends a hash-chained receipt to the local state file; failures return a named error and are still receipted. Returns an object with actor, posts, count, cursor, retrieved_at as epoch seconds, mode READ_API, a note restating the no-invented-metric rule, and receipt_seq.",
                         {"actor": {"type": "string", "description": "Bluesky handle (for example 'name.bsky.social') or DID whose original posts should be read. Required."},
                          "limit": {"type": "integer", "description": "Maximum number of feed items to return. Defaults to 20."}},
                         ["actor"],
                         {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
                         {"type": "object", "properties": {"actor": {"type": "string"}, "posts": {"type": "array", "items": {"type": "object"}}, "count": {"type": "integer"}, "cursor": {"type": ["string", "null"]}, "retrieved_at": {"type": "integer"}, "mode": {"type": "string"}, "note": {"type": "string"}, "receipt_seq": {"type": "integer"}}, "required": ["actor", "posts", "count", "retrieved_at", "mode", "receipt_seq"]}),
    "post_thread_read": (t_post_thread_read,
                         "Read one public Bluesky post thread through the AT Protocol public API: the root post plus its direct replies, each normalized with URI, CID, author handle and DID, text, creation time, languages, the protocol's own like/repost/reply counts, media presence, and source_endpoint naming this endpoint. Use when you have a specific post URI and need its conversational context; use author_feed_read instead to browse an account's recent posts, and profile_read for account-level information. Live external read against app.bsky.feed.getPostThread at public.api.bsky.app with no authentication. Only the root and its direct replies as returned by the endpoint are included; this tool does not page deeper into nested branches. Rate discipline: at least 0.4 seconds between live calls, up to two retries on transient failures only, and a 60 live-call ceiling per server process. Every call, success or failure, appends a hash-chained receipt to the local state file; failures return a named error and are still receipted. Returns an object with root, replies, reply_count, retrieved_at as epoch seconds, mode READ_API, and receipt_seq.",
                         {"uri": {"type": "string", "description": "Full AT URI of the post to expand, in the form 'at://<did>/app.bsky.feed.post/<record-key>', as returned in feed or search results. Required."}},
                         ["uri"],
                         {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
                         {"type": "object", "properties": {"root": {"type": "object"}, "replies": {"type": "array", "items": {"type": "object"}}, "reply_count": {"type": "integer"}, "retrieved_at": {"type": "integer"}, "mode": {"type": "string"}, "receipt_seq": {"type": "integer"}}, "required": ["root", "replies", "reply_count", "retrieved_at", "mode", "receipt_seq"]}),
    "receipt_list": (t_receipt_list,
                     "List this server's local append-only receipt ledger: one hash-chained entry for every live read performed by profile_read, actor_search, author_feed_read, and post_thread_read, whether it succeeded or failed, each recording the endpoint, subject, status, and result count or error. Use to audit what was actually read and to verify the chain; use the four read tools themselves to fetch new Bluesky data instead. Fully local and read-only: no network call, no authentication, and it appends nothing. chain_valid is recomputed on every call by re-hashing each entry against its predecessor back to GENESIS, so a tampered or reordered ledger reports false. Returns an object with receipts (in sequence order), count, and chain_valid.",
                     {},
                     [],
                     {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
                     {"type": "object", "properties": {"receipts": {"type": "array", "items": {"type": "object"}}, "count": {"type": "integer"}, "chain_valid": {"type": "boolean"}}, "required": ["receipts", "count", "chain_valid"]}),
}


def tool_list():
    return [{"name": name, "description": desc,
             "inputSchema": {"type": "object", "properties": props, "required": req},
             "annotations": ann, "outputSchema": out}
            for name, (_, desc, props, req, ann, out) in TOOLS.items()]
def handle(req: dict):
    method, rid = req.get("method", ""), req.get("id")
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": req.get("params", {}).get("protocolVersion", "2025-03-26"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "eclipse-zephyr", "version": "0.1.0"}}}
    if method == "ping":
        return {"jsonrpc": "2.0", "id": rid, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": tool_list()}}
    if method == "tools/call":
        name = req["params"]["name"]
        args = req["params"].get("arguments", {})
        if name not in TOOLS:
            return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32602, "message": f"unknown tool {name}"}}
        state = load_state()
        try:
            out = TOOLS[name][0](state, args)
            save_state(state)
            return {"jsonrpc": "2.0", "id": rid, "result": {
                "content": [{"type": "text", "text": json.dumps(out, indent=1)}]}}
        except ToolError as e:
            save_state(state)  # the failure receipt must persist
            return {"jsonrpc": "2.0", "id": rid, "result": {
                "content": [{"type": "text", "text": json.dumps({"error": str(e)})}], "isError": True}}
    if rid is not None:
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"method not found: {method}"}}
    return None  # notification


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            resp = handle(json.loads(line))
        except Exception as e:  # never crash the transport
            resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": str(e)}}
        if resp is not None:
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
