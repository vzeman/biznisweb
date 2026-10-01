"""Read-only production inventory diagnostic; no login, journal or business writes."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    import boto3
    from generate_invoices import InvoiceGenerator

    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, choices=("roy", "vevo"))
    parser.add_argument("--profile", default="codex")
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name="eu-central-1")
    if session.client("sts").get_caller_identity()["Account"] != "919341186960":
        raise RuntimeError("Unexpected AWS account")
    secret = json.loads(session.client("secretsmanager").get_secret_value(
        SecretId=f"{args.project}/reporting/runtime-env")["SecretString"])
    generator = InvoiceGenerator(secret["BIZNISWEB_API_URL"], secret["BIZNISWEB_API_TOKEN"],
                                 f"https://{args.project}.flox.sk", project=args.project)
    original = generator.execute_read

    def traced(query, variables):
        try:
            result = original(query, variables)
        except Exception as exc:
            errors = getattr(exc, "errors", None) or []
            rows = ((getattr(exc, "data", None) or {}).get("getOrderList") or {}).get("data") or []
            paths = [e.get("path") for e in errors if isinstance(e, dict)]
            affected = []
            for path in paths:
                if (isinstance(path, list) and len(path) >= 3 and isinstance(path[2], int)
                        and path[2] < len(rows) and isinstance(rows[path[2]], dict)):
                    row = rows[path[2]]
                    affected.append({key: row.get(key) for key in ("id", "order_num", "status", "blocked", "invoices")})
            evidence = {"project": args.project, "variables": variables, "error_type": type(exc).__name__,
                        "paths": paths, "partial_rows": len(rows), "affected": affected}
            path = Path("data") / f"{args.project}-invoice-inventory-diagnostic.json"
            path.parent.mkdir(exist_ok=True)
            path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
            print(json.dumps({key: value for key, value in evidence.items() if key != "affected"}), flush=True)
            raise RuntimeError("Read failed; private diagnostic saved, no mutations attempted") from None
        cursor = (variables.get("params") or {}).get("cursor")
        if cursor is not None and cursor % 290 == 0:
            print(json.dumps({"project": args.project, "cursor": cursor, "read": "ok"}), flush=True)
        return result

    generator.execute_read = traced
    orders = generator.fetch_all_eligible_orders()
    print(json.dumps({"project": args.project, "complete": True, "orders": len(orders), "pages": generator.scan_pages}))


if __name__ == "__main__":
    main()
