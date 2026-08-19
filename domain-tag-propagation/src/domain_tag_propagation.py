# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Domain Tag Propagation
# MAGIC
# MAGIC End-to-end domain lifecycle automation: reads the approved domain roster from a master
# MAGIC governed tag, ensures full domain infrastructure exists (governed tag keys **and** Discover
# MAGIC page domain cards), then propagates per-domain tags from schemas down to every table, view, and volume so
# MAGIC assets surface under the correct Domain on the Discover page.
# MAGIC
# MAGIC ### The two-tier tag model (read this first)
# MAGIC
# MAGIC This notebook deliberately uses **two kinds of governed tag key**:
# MAGIC
# MAGIC | Layer | Tag key | Where applied | Purpose |
# MAGIC |-------|---------|---------------|---------|
# MAGIC | **Master** | e.g. `domain` (with an *allowed-values* list `[Clinical, Finance, ...]`) | applied to **schemas** by data owners | single source of truth for the approved domain roster |
# MAGIC | **Per-domain** | one key **per approved value** (e.g. `Clinical`, `Finance`) | applied to **tables/views** by this notebook | binds each asset to its Discover domain card |
# MAGIC
# MAGIC A Discover **domain card** is bound to a per-domain governed tag key (`domain.tag_key`). An asset
# MAGIC surfaces under that domain when it carries that key. The per-domain tags are applied **key-only**
# MAGIC (presence markers, no value).
# MAGIC
# MAGIC ### What this notebook does on each run
# MAGIC
# MAGIC 1. Reads the allowed values from the master governed tag (default key `domain`).
# MAGIC 2. For each approved value, ensures **both** infrastructure layers exist:
# MAGIC    * **Governed tag key** via `POST /api/2.1/tag-policies`.
# MAGIC    * **Discover page domain card** via `POST /api/2.0/domains`.
# MAGIC 3. For each schema carrying the master tag, propagates the per-domain tag key to every
# MAGIC    in-scope securable in that schema. The executor chooses which object kinds to include via
# MAGIC    the `object_kinds` parameter: table-family types (MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW,
# MAGIC    STREAMING_TABLE, FOREIGN) and/or `VOLUME`.
# MAGIC 4. All operations are idempotent, dry-run capable, parameterized, and logged per-object.
# MAGIC
# MAGIC ### Behavior, requirements, and known caveats
# MAGIC
# MAGIC * **Additive only.** This notebook creates and applies; it never deletes. If a value is removed
# MAGIC   from the master tag, stale domain cards and per-table tags are **not** pruned. Reconciliation is
# MAGIC   out of scope by design.
# MAGIC * **Requires Databricks Runtime 16.1+** (or current serverless) for the `SET TAG ON ...` syntax;
# MAGIC   tagging. On older runtimes the tag-apply step fails per-object and is reported as an error (the run does not abort).
# MAGIC * **Volumes** are pre-checked for idempotency via `information_schema.volume_tags`.
# MAGIC * **Discover / data domains is an evolving feature.** A freshly created domain may land as a
# MAGIC   **draft** (`effective_draft = true`) that must be published in the Discover UI before it appears.
# MAGIC   This notebook surfaces the draft state per domain but does not auto-publish. Confirm the
# MAGIC   behavior in your workspace.
# MAGIC * **Required privileges:** authority to create tag policies (metastore admin / governed-tag
# MAGIC   manage), create domains, and `APPLY TAG` on the target securables (tables/views/volumes).

# COMMAND ----------

# DBTITLE 1,Parameters
# Parameters (widgets) -- as a Databricks Job task, these map directly to task parameters.
dbutils.widgets.text("catalog_name", "", "Catalog to scan")
dbutils.widgets.text("master_tag_key", "domain", "Master governed tag key (allowed-values source of truth)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no writes)")
# Which schema-level object kinds to tag. Comma-separated; pick any of:
#   table_type values -> MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE, METRIC_VIEW, FOREIGN
#   other securables  -> VOLUME (SET TAG ON VOLUME)
dbutils.widgets.text(
    "object_kinds",
    "MANAGED,EXTERNAL,VIEW,MATERIALIZED_VIEW,STREAMING_TABLE,METRIC_VIEW",
    "Object kinds to tag (comma-separated; incl. VOLUME)",
)

CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() if dbutils.widgets.get("catalog_name") else None
MASTER_TAG_KEY = dbutils.widgets.get("master_tag_key").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
OBJECT_KINDS = [t.strip().upper() for t in dbutils.widgets.get("object_kinds").split(",") if t.strip()]

if not CATALOG_NAME or CATALOG_NAME == '':
    raise ValueError("Widget 'catalog_name' is required.")
if not MASTER_TAG_KEY:
    raise ValueError("Widget 'master_tag_key' is required.")

print(f"catalog_name={CATALOG_NAME} master_tag_key={MASTER_TAG_KEY} dry_run={DRY_RUN} object_kinds={OBJECT_KINDS}")


# COMMAND ----------

# DBTITLE 1,Imports and logging setup
import logging
import re
from typing import Optional
from urllib.parse import quote

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, NotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("domain_tag_propagation")

w = WorkspaceClient()

# Governed tag keys can be namespaced (e.g. 'class.credit_card') and may contain '/', '-', '.'.
# They are URL-encoded before path interpolation and backtick-escaped before DDL interpolation.
_TAG_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_./\-]+$")


def assert_no_control_chars(value: str, label: str) -> None:
    """Reject only control characters. Catalog/schema/table names reach SQL exclusively as bound parameters (:name) or via q() backtick-escaping, so a stricter allowlist would needlessly
    reject legitimate Unity Catalog names (hyphens, dots, etc.)."""
    if any(ord(c) < 32 for c in value):
        raise ValueError(f"Invalid {label}: {value!r} contains control characters.")

# SET TAG requires an object-type keyword that matches the object.
#
# Table-family kinds come from information_schema.tables.table_type; map each to its SET TAG
# keyword. table_type values absent from this map are skipped.
_TABLE_TYPE_KEYWORD = {
    "MANAGED": "TABLE",
    "EXTERNAL": "TABLE",
    "FOREIGN": "TABLE",
    "VIEW": "VIEW",
    "MATERIALIZED_VIEW": "MATERIALIZED VIEW",
    "STREAMING_TABLE": "STREAMING TABLE",
    "METRIC_VIEW": "TABLE",  # Metric views use TABLE keyword for SET TAG
}

# Non-table securables that also live under a schema. Each is listed from its own
# information_schema view and tagged with its own SET TAG keyword.
#   - VOLUME:   pre-check via information_schema.volume_tags (queryable).
_SPECIAL_KIND_KEYWORD = {
    "VOLUME": "VOLUME",
}

# All kinds the executor may request.
_ALL_OBJECT_KINDS = list(_TABLE_TYPE_KEYWORD.keys()) + list(_SPECIAL_KIND_KEYWORD.keys())


def assert_valid_tag_key(value: str) -> None:
    """Guard a tag key. Kept fairly strict because tag keys are interpolated into a REST path
    (URL-encoded) and into DDL (backtick-escaped); the pattern excludes whitespace and quotes."""
    if not _TAG_KEY_PATTERN.match(value):
        raise ValueError(f"Invalid tag key: '{value}'.")


def q(identifier: str) -> str:
    """Escape a SQL identifier for safe interpolation inside backticks."""
    if "\n" in identifier or "\r" in identifier:
        raise ValueError(f"Illegal control character in identifier: {identifier!r}")
    return identifier.replace("`", "``")


# Validate widget inputs once, up front.
assert_no_control_chars(CATALOG_NAME, "catalog_name")
assert_valid_tag_key(MASTER_TAG_KEY)


# COMMAND ----------

# DBTITLE 1,Helper: tag policy (governed tag key) reads/writes
# The tag policies API is versioned at 2.1.
# The installed databricks-sdk may predate a typed tag_policies service, so calls go through the
# low-level REST dispatcher (api_client.do) against the same endpoints the CLI and typed API use.
_TAG_POLICIES_PATH = "/api/2.1/tag-policies"


def get_tag_policy(client: WorkspaceClient, tag_key: str) -> dict:
    """Fetch a tag policy by key. Raises databricks.sdk.errors.NotFound if it does not exist."""
    assert_valid_tag_key(tag_key)
    # URL-encode the key (with safe="") so a namespaced key containing '/' does not alter the path.
    return client.api_client.do("GET", f"{_TAG_POLICIES_PATH}/{quote(tag_key, safe='')}")


def create_tag_policy(client: WorkspaceClient, tag_key: str, description: Optional[str] = None) -> dict:
    """Create a governed tag key (presence-only: no allowed-values restriction)."""
    payload: dict = {"tag_key": tag_key}
    if description:
        payload["description"] = description
    return client.api_client.do("POST", _TAG_POLICIES_PATH, body=payload)


def get_approved_domain_values(client: WorkspaceClient, master_tag_key: str) -> list[str]:
    """Return the allowed-values list defined on the master governed tag (e.g. 'domain').

    Raises ValueError if the master governed tag policy does not exist -- it must be created once,
    up front, via the governance process before this notebook can run.
    """
    try:
        policy = get_tag_policy(client, master_tag_key)
    except NotFound as exc:
        raise ValueError(
            f"Master governed tag '{master_tag_key}' does not exist. Create it first."
        ) from exc
    return [v["name"] for v in (policy.get("values") or [])]


# COMMAND ----------

# DBTITLE 1,Helper: Discover domain cards
_DOMAINS_PATH = "/api/2.0/domains"


def list_existing_domains(client: WorkspaceClient) -> dict[str, bool]:
    """Return a map of {tag_key: effective_draft} for every existing Discover domain card.

    Pages through the domains list so idempotency does not depend on catching a create-time
    duplicate error (whose exact type/shape is not guaranteed). Carrying the draft flag lets the
    tool re-surface drafts created in a prior run, not just freshly created ones.
    """
    domains: dict[str, bool] = {}
    page_token: Optional[str] = None
    while True:
        query = {"page_token": page_token} if page_token else None
        resp = client.api_client.do("GET", _DOMAINS_PATH, query=query)
        for d in resp.get("domains") or []:
            if d.get("tag_key"):
                domains[d["tag_key"]] = bool(d.get("effective_draft"))
        page_token = resp.get("next_page_token")
        if not page_token:
            break
    return domains


def register_domain_on_discover(
    client: WorkspaceClient,
    tag_key: str,
    subtitle: str = "",
    description: str = "",
) -> dict:
    """Create a Discover domain card bound to a governed tag key. Returns the created payload."""
    payload = {"tag_key": tag_key, "subtitle": subtitle, "description": description}
    return client.api_client.do("POST", _DOMAINS_PATH, body=payload)


# COMMAND ----------

# DBTITLE 1,Helper: ensure domain infrastructure
def ensure_domain(
    client: WorkspaceClient,
    domain_name: str,
    existing_domains: dict[str, bool],
    subtitle: str = "",
    description: Optional[str] = None,
    dry_run: bool = True,
) -> dict:
    """Ensure both layers of infrastructure exist for a single domain:
      1. Governed tag key (tag policy) -- required to apply the tag to objects.
      2. Discover page domain card -- required for the domain to appear on Discover.

    Idempotent, and honors dry_run: when dry_run is True, existence is checked (reads only) but
    nothing is created -- missing infrastructure is reported with a 'would_create' status.

    existing_domains maps already-registered tag_key -> effective_draft (from list_existing_domains).

    Returns a dict with 'domain', 'tag_policy_status', 'discover_status', and 'discover_draft'
    keys. Never raises -- validation and infrastructure errors are captured per-domain so one bad
    domain does not abort the whole roster.
    """
    desc = description or f"{domain_name} data domain"
    result: dict = {"domain": domain_name, "discover_draft": None}

    # Validation is per-domain: a malformed roster value must not abort the whole run.
    try:
        assert_valid_tag_key(domain_name)
    except ValueError as exc:
        logger.error("Skipping invalid domain value %r: %s", domain_name, exc)
        result["tag_policy_status"] = f"error: {exc}"
        result["discover_status"] = "skipped_invalid_value"
        return result

    # --- Layer 1: Governed tag key ---
    try:
        try:
            get_tag_policy(client, domain_name)
            result["tag_policy_status"] = "already_exists"
        except NotFound:
            if dry_run:
                logger.info("[dry_run] would create governed tag key '%s'.", domain_name)
                result["tag_policy_status"] = "would_create"
            else:
                create_tag_policy(client, domain_name, description=desc)
                result["tag_policy_status"] = "created"
                logger.info("Created governed tag key '%s'.", domain_name)
    except Exception as exc:  # noqa: BLE001 -- capture per-domain, keep going
        logger.error("Failed ensuring tag policy for '%s': %s", domain_name, exc)
        result["tag_policy_status"] = f"error: {exc}"

    # --- Layer 2: Discover page domain card ---
    try:
        if domain_name in existing_domains:
            result["discover_status"] = "already_exists"
            # Re-surface a draft created in a prior run, not just freshly created ones.
            result["discover_draft"] = existing_domains[domain_name]
            if result["discover_draft"]:
                logger.warning(
                    "Domain '%s' already exists but is still a DRAFT -- publish it in the Discover UI.",
                    domain_name,
                )
        elif dry_run:
            logger.info("[dry_run] would register Discover domain card for '%s'.", domain_name)
            result["discover_status"] = "would_create"
        else:
            created = register_domain_on_discover(client, tag_key=domain_name, subtitle=subtitle, description=desc)
            result["discover_status"] = "created"
            # Surface draft state: a draft card is not visible on Discover until published.
            result["discover_draft"] = bool(created.get("effective_draft"))
            if result["discover_draft"]:
                logger.warning(
                    "Domain '%s' was created as a DRAFT -- publish it in the Discover UI to make it visible.",
                    domain_name,
                )
            else:
                logger.info("Registered domain '%s' on Discover page.", domain_name)
    except AlreadyExists:
        result["discover_status"] = "already_exists"
    except Exception as exc:  # noqa: BLE001 -- capture per-domain, keep going
        logger.error("Failed registering Discover card for '%s': %s", domain_name, exc)
        result["discover_status"] = f"error: {exc}"

    return result


def ensure_all_domains(
    client: WorkspaceClient,
    approved_values: list[str],
    dry_run: bool = True,
    subtitle_template: str = "{domain} data assets",
    description_template: str = "{domain} data domain",
) -> list[dict]:
    """Ensure domain infrastructure exists for every approved value. Idempotent and safe to re-run.

    Honors dry_run: when True, nothing is created (missing infra reported as 'would_create').
    """
    if not approved_values:
        logger.warning("No approved values on the master tag -- nothing to create.")
        return []

    existing_domains = list_existing_domains(client)
    logger.info("Discover already has %d domain card(s).", len(existing_domains))

    results = []
    for domain in approved_values:
        results.append(
            ensure_domain(
                client,
                domain_name=domain,
                existing_domains=existing_domains,
                subtitle=subtitle_template.format(domain=domain),
                description=description_template.format(domain=domain),
                dry_run=dry_run,
            )
        )

    verb = "would create" if dry_run else "created/registered"
    tag_changed = sum(1 for r in results if r["tag_policy_status"] in ("created", "would_create"))
    discover_changed = sum(1 for r in results if r["discover_status"] in ("created", "would_create"))
    drafts = sum(1 for r in results if r.get("discover_draft"))
    logger.info(
        "Domain sync complete (dry_run=%s): %s %d tag(s), %s %d Discover card(s) (%d draft), %d total.",
        dry_run, verb, tag_changed, verb, discover_changed, drafts, len(results),
    )
    return results


# COMMAND ----------

# DBTITLE 1,Helper: read schema + table state (batched)
# Internal backing tables (materialized views, streaming tables) use a '__' prefix.
_INTERNAL_TABLE_PREFIX = "__"


def get_schema_domain_assignments(catalog_name: str, master_tag_key: str) -> list[dict]:
    """Read each schema's master domain tag value from system.information_schema.schema_tags.

    Uses SQL parameter markers (no string interpolation) for injection safety.
    """
    rows = spark.sql(
        """
        SELECT schema_name, tag_value
        FROM system.information_schema.schema_tags
        WHERE catalog_name = :catalog AND tag_name = :tag_key
        """,
        args={"catalog": catalog_name, "tag_key": master_tag_key},
    ).collect()
    return [{"schema_name": r["schema_name"], "domain_value": r["tag_value"]} for r in rows]


def list_schema_objects(catalog_name: str, schema_name: str) -> list[dict]:
    """List all user-facing objects in a schema (excluding internal '__' backing tables).

    Returns [{"table_name": ..., "table_type": ...}]. One query per schema instead of one per
    object. Type filtering is done by the caller so out-of-scope objects can be counted/reported
    rather than silently dropped.
    """
    rows = spark.sql(
        """
        SELECT table_name, table_type
        FROM system.information_schema.tables
        WHERE table_catalog = :catalog AND table_schema = :schema
        """,
        args={"catalog": catalog_name, "schema": schema_name},
    ).collect()
    return [
        {"table_name": r["table_name"], "table_type": r["table_type"]}
        for r in rows
        if not r["table_name"].startswith(_INTERNAL_TABLE_PREFIX)
    ]


def get_existing_table_tags(catalog_name: str, schema_name: str, tag_key: str) -> set[str]:
    """Return the set of table/view names in a schema that already carry tag_key.

    One query per (schema, tag_key) -- replaces the previous per-table existence check.
    """
    rows = spark.sql(
        """
        SELECT table_name
        FROM system.information_schema.table_tags
        WHERE catalog_name = :catalog AND schema_name = :schema AND tag_name = :tag_key
        """,
        args={"catalog": catalog_name, "schema": schema_name, "tag_key": tag_key},
    ).collect()
    return {r["table_name"] for r in rows}


def list_volumes(catalog_name: str, schema_name: str) -> list[dict]:
    """List volumes in a schema as securable dicts {name, kind, keyword}."""
    rows = spark.sql(
        """
        SELECT volume_name
        FROM system.information_schema.volumes
        WHERE volume_catalog = :catalog AND volume_schema = :schema
        """,
        args={"catalog": catalog_name, "schema": schema_name},
    ).collect()
    return [{"name": r["volume_name"], "kind": "VOLUME", "keyword": "VOLUME"} for r in rows]


def get_existing_volume_tags(catalog_name: str, schema_name: str, tag_key: str) -> set[str]:
    """Return the set of volume names in a schema that already carry tag_key.

    Note the column is schema_name (not volume_schema) in information_schema.volume_tags.
    """
    rows = spark.sql(
        """
        SELECT volume_name
        FROM system.information_schema.volume_tags
        WHERE catalog_name = :catalog AND schema_name = :schema AND tag_name = :tag_key
        """,
        args={"catalog": catalog_name, "schema": schema_name, "tag_key": tag_key},
    ).collect()
    return {r["volume_name"] for r in rows}



# COMMAND ----------

# DBTITLE 1,Helper: idempotent tag apply
def apply_tag_to_securable(
    catalog_name: str,
    schema_name: str,
    securable: dict,
    tag_key: str,
    already_tagged: Optional[set[str]],
    dry_run: bool,
) -> dict:
    """Idempotently apply the presence-only tag_key to one securable, honoring dry_run.

    securable is {name, kind, keyword}. already_tagged is the set of names already carrying tag_key.

    Returns a result dict with a 'status' of: 'skipped_up_to_date', 'skipped_unsupported_type',
    'dry_run', 'applied', 'error'.
    """
    name = securable["name"]
    kind = securable["kind"]
    keyword = securable["keyword"]
    base_result = {
        "catalog": catalog_name,
        "schema": schema_name,
        "object": name,
        "object_kind": kind,
        "tag_key": tag_key,
    }

    if keyword is None:
        return {**base_result, "status": "skipped_unsupported_type",
                "message": f"{kind} is not a taggable domain object"}

    fq = f"`{q(catalog_name)}`.`{q(schema_name)}`.`{q(name)}`"

    if already_tagged is not None and name in already_tagged:
        return {**base_result, "status": "skipped_up_to_date", "message": "tag already set"}

    if dry_run:
        logger.info("[dry_run] would set `%s` on %s %s", tag_key, keyword, fq)
        return {**base_result, "status": "dry_run", "message": f"would set {tag_key} on {kind}"}

    # Presence-only marker tag: applied key-only, no '= value'.
    set_tag_sql = f"SET TAG ON {keyword} {fq} `{q(tag_key)}`"
    try:
        spark.sql(set_tag_sql)
        logger.info("Applied `%s` on %s %s", tag_key, keyword, fq)
        return {**base_result, "status": "applied", "message": f"set {tag_key} on {kind}"}
    except Exception as exc:  # noqa: BLE001 -- surface as a per-object error, do not abort the run
        # information_schema tag views can lag real tag state, so a pre-check may miss a tag set
        # moments earlier. UC reports that as UC_DUPLICATE_TAG_ASSIGNMENT_CREATION -- treat it as
        # confirmation the desired state already holds, not a failure.
        if "UC_DUPLICATE_TAG_ASSIGNMENT_CREATION" in str(exc):
            logger.info("Tag `%s` already present on %s (metadata lag) -- up to date", tag_key, fq)
            return {**base_result, "status": "skipped_up_to_date", "message": "tag already set (confirmed by UC)"}
        logger.error("Failed applying tag on %s: %s", fq, exc)
        return {**base_result, "status": "error", "message": str(exc)}


# COMMAND ----------

# DBTITLE 1,Orchestration
def _collect_securables(
    catalog_name: str,
    schema_name: str,
    domain_value: str,
    table_type_kinds: list[str],
    want_volumes: bool,
) -> tuple[list[dict], list[dict]]:
    """Build the list of securables to tag in a schema for the selected object kinds.

    Returns (securables, out_of_scope_results). securables carry an 'already_tagged' set.
    out_of_scope_results reports table_type objects the executor did not select, so they are
    visible rather than silently dropped.
    """
    securables: list[dict] = []
    out_of_scope_results: list[dict] = []

    # --- Tables / views family ---
    if table_type_kinds:
        all_objects = list_schema_objects(catalog_name, schema_name)
        in_scope = [o for o in all_objects if o["table_type"] in table_type_kinds]
        out_of_scope = [o for o in all_objects if o["table_type"] not in table_type_kinds]
        if out_of_scope:
            skipped_types = sorted({o["table_type"] for o in out_of_scope})
            logger.info(
                "Schema '%s': %d table-family object(s) in scope, %d skipped (table_type not in %s: %s)",
                schema_name, len(in_scope), len(out_of_scope), table_type_kinds, skipped_types,
            )
            for o in out_of_scope:
                out_of_scope_results.append({
                    "catalog": catalog_name, "schema": schema_name, "object": o["table_name"],
                    "object_kind": o["table_type"], "tag_key": domain_value,
                    "status": "skipped_out_of_scope_kind",
                    "message": f"table_type {o['table_type']} not in requested object_kinds",
                })
        table_tagged = get_existing_table_tags(catalog_name, schema_name, domain_value)
        for o in in_scope:
            securables.append({
                "name": o["table_name"], "kind": o["table_type"],
                "keyword": _TABLE_TYPE_KEYWORD.get(o["table_type"]), "already_tagged": table_tagged,
            })

    # --- Volumes (pre-check available) ---
    if want_volumes:
        volume_tagged = get_existing_volume_tags(catalog_name, schema_name, domain_value)
        for s in list_volumes(catalog_name, schema_name):
            securables.append({**s, "already_tagged": volume_tagged})

    return securables, out_of_scope_results


def run_domain_propagation(
    catalog_name: str,
    master_tag_key: str = "domain",
    object_kinds: Optional[list[str]] = None,
    dry_run: bool = True,
) -> tuple[list[dict], list[dict]]:
    """End-to-end domain lifecycle: ensure infrastructure, then propagate tags.

    1. Ensure domain infrastructure (governed tag key + Discover card) for every approved value.
    2. For each schema carrying the master tag, propagate the per-domain tag key to every in-scope
       securable (tables/views, and optionally volumes) in that schema.
    """
    object_kinds = object_kinds or list(_TABLE_TYPE_KEYWORD.keys())
    table_type_kinds = [k for k in object_kinds if k not in _SPECIAL_KIND_KEYWORD]
    want_volumes = "VOLUME" in object_kinds

    # Read the approved roster once and reuse it.
    approved_list = get_approved_domain_values(w, master_tag_key)
    approved_set = set(approved_list)
    logger.info("Approved domain values for '%s': %s", master_tag_key, sorted(approved_set))

    # --- Step 1: Ensure all domain infrastructure exists ---
    domain_sync_results = ensure_all_domains(w, approved_list, dry_run=dry_run)
    for r in domain_sync_results:
        logger.info(
            "  %s: tag_policy=%s, discover=%s%s",
            r["domain"], r["tag_policy_status"], r["discover_status"],
            " (DRAFT)" if r.get("discover_draft") else "",
        )

    # --- Step 2: Propagate tags from schemas to securables ---
    schema_assignments = get_schema_domain_assignments(catalog_name, master_tag_key)
    logger.info(
        "Found %d schema(s) tagged with '%s' in catalog '%s'; object kinds=%s",
        len(schema_assignments), master_tag_key, catalog_name, object_kinds,
    )

    results: list[dict] = []
    for assignment in schema_assignments:
        schema_name = assignment["schema_name"]
        domain_value = assignment["domain_value"]

        if domain_value not in approved_set:
            logger.warning(
                "Schema '%s' has domain value '%s' not in approved list -- skipping",
                schema_name, domain_value,
            )
            results.append({
                "catalog": catalog_name, "schema": schema_name, "object": None, "object_kind": None,
                "tag_key": domain_value, "status": "skipped_unapproved_value",
                "message": "domain value not in master governed tag allowed values",
            })
            continue

        securables, out_of_scope = _collect_securables(
            catalog_name, schema_name, domain_value, table_type_kinds, want_volumes,
        )
        results.extend(out_of_scope)
        for s in securables:
            results.append(
                apply_tag_to_securable(
                    catalog_name, schema_name, s, domain_value, s["already_tagged"], dry_run,
                )
            )

    return domain_sync_results, results


# COMMAND ----------

# DBTITLE 1,Runtime notice
# SET TAG ON <object> requires Databricks Runtime 16.1+ (or current serverless). This notebook
# does not hard-gate on the version -- if the syntax is unsupported, each apply is reported as a
# per-object error rather than aborting the run. Check the run summary for 'error' statuses if
# nothing applies.
logger.info("Note: SET TAG requires DBR 16.1+ or current serverless.")


# COMMAND ----------

# DBTITLE 1,Execute and summarize
domain_sync_results, propagation_results = run_domain_propagation(
    catalog_name=CATALOG_NAME,
    master_tag_key=MASTER_TAG_KEY,
    object_kinds=OBJECT_KINDS,
    dry_run=DRY_RUN,
)

# Merge discover status/draft into propagation results and rename status -> tag_status.
discover_status_map = {r["domain"]: r["discover_status"] for r in domain_sync_results}
discover_draft_map = {r["domain"]: r.get("discover_draft") for r in domain_sync_results}
for r in propagation_results:
    r["discover_status"] = discover_status_map.get(r["tag_key"], "unknown")
    r["discover_draft"] = discover_draft_map.get(r["tag_key"])
    r["tag_status"] = r.pop("status")

if propagation_results:
    results_df = spark.createDataFrame(propagation_results)
    display(results_df.select(
        "catalog", "schema", "object", "object_kind", "tag_key",
        "tag_status", "discover_status", "discover_draft", "message",
    ))

    summary: dict = {}
    for r in propagation_results:
        summary[r["tag_status"]] = summary.get(r["tag_status"], 0) + 1
    logger.info("Run summary (dry_run=%s): %s", DRY_RUN, summary)
    print(f"Run summary (dry_run={DRY_RUN}): {summary}")
else:
    print(f"No schemas in catalog '{CATALOG_NAME}' are tagged with '{MASTER_TAG_KEY}'.")