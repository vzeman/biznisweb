from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from invoice_automation_state import S3AutomationStateStore
from manual_settlement import (
    canonical_bytes,
    confirmed_proof,
    proof_reference,
    updated_record,
    validate_proof,
)
from order_status_safety import (
    assess_settlement_evidence,
    decide_recovery,
    status_write_block_reason,
)
from scripts.record_verified_manual_settlement import (
    ACCOUNT,
    BUCKET,
    api_environment,
    check_current,
    load_reference,
    read_private,
    record_proof,
    require_private_bucket,
    require_source,
)
from tests.test_invoice_automation_state import MemoryS3
from tests.test_order_status_safety import money, order, receipt


def current_order():
    value = order()
    value.update(id="101", order_num="900001", last_change="2026-05-01 10:00:00")
    return value


def reference_for(value=None, project="vevo", target=None):
    value = value or current_order()
    proof = {
        "schema_version": 1,
        "project": project,
        "order_id": str(value["id"]),
        "order_num": str(value["order_num"]),
        "source": "explicit_user_confirmed_full_external_bank_transfer",
        "confirmed_at": "2026-01-01T00:00:00Z",
        "operation": "confirm",
        "total": "100",
        "currency": "EUR",
        "recovery_status": target or {"id": "4", "name": "Odoslaná"},
        "supporting_evidence": [
            {
                "key": f"data/{project}/order-automation/audits/2026-01-01/fixture.json",
                "sha256": "a" * 64,
            }
        ],
    }
    return bind(proof)


def bind(proof):
    digest = hashlib.sha256(canonical_bytes(proof)).hexdigest()
    return proof_reference(
        proof,
        f"data/{proof['project']}/order-automation/manual-settlements/{digest}.json",
        digest,
    )


def confirmed(value=None, **kwargs):
    return {"schema_version": 1, "state": "confirmed", **reference_for(value, **kwargs)}


def store_for(records=None):
    store = S3AutomationStateStore(
        MemoryS3(),
        BUCKET,
        "data/vevo/order-automation/state.json",
        "vevo",
        now=lambda: datetime(2026, 5, 27, tzinfo=timezone.utc),
    )
    state = store.empty_state()
    state["orders"] = deepcopy(records or {})
    store.put(state, "")
    return store


class ManualSettlementEvidenceTests(unittest.TestCase):
    def test_explicit_proof_fills_only_complete_absence_without_fabricating_flags(self):
        value = current_order()
        before = deepcopy(value)
        result = assess_settlement_evidence(
            value, project="vevo", manual_settlement=confirmed(value)
        )
        self.assertEqual(
            ("confirmed", "verified_manual_bank_settlement"),
            (result.state, result.reason),
        )
        self.assertEqual(value, before)
        self.assertFalse(value["invoices"][0]["paid"])
        self.assertEqual([], value["invoices"][0]["payments"])

    def test_native_partial_reversal_missing_and_malformed_context_cannot_be_overridden(
        self,
    ):
        for defect in (
            "partial",
            "reversal",
            "missing",
            "bad-paid",
            "bad-currency",
            "undated",
        ):
            value = current_order()
            if defect == "partial":
                value["invoices"][0]["payments"] = [receipt(amount=10)]
            elif defect == "reversal":
                value["invoices"][0]["payments"] = [receipt(amount=-100)]
            elif defect == "missing":
                value["invoices"][0].pop("payments")
            elif defect == "bad-paid":
                value["invoices"][0]["paid"] = "false"
            elif defect == "bad-currency":
                value["invoices"][0]["sum"] = money(currency="USD")
            else:
                value["invoices"][0]["payments"] = [{**receipt(), "pay_date": None}]
            with self.subTest(defect=defect):
                self.assertNotEqual(
                    "confirmed",
                    assess_settlement_evidence(
                        value, project="vevo", manual_settlement=confirmed()
                    ).state,
                )

    def test_native_confirmed_receipts_remain_primary(self):
        value = current_order()
        value["invoices"][0]["payments"] = [receipt()]
        result = assess_settlement_evidence(
            value, project="vevo", manual_settlement=confirmed()
        )
        self.assertEqual("full_receipt_settlement", result.reason)

    def test_manual_identity_amount_currency_hash_and_revocation_are_fail_closed(self):
        for defect in (
            "project",
            "id",
            "number",
            "amount",
            "currency",
            "hash",
            "revoked",
            "extra",
        ):
            value, record, project = current_order(), confirmed(), "vevo"
            if defect == "project":
                project = "roy"
            elif defect == "id":
                value["id"] = "102"
            elif defect == "number":
                value["order_num"] = "900002"
            elif defect == "amount":
                value["sum"]["value"] = 101
            elif defect == "currency":
                value["sum"]["currency"]["code"] = "USD"
            elif defect == "hash":
                record["evidence_sha256"] = "b" * 64
            elif defect == "revoked":
                record["state"] = "revoked"
            else:
                record["untrusted"] = True
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                confirmed_proof(record, project, value)

    def test_unsafe_numbers_dates_sources_and_additional_fields_rejected(self):
        for key, value in (
            ("total", "1e999999"),
            ("total", "100.00"),
            ("total", True),
            ("order_id", True),
            ("order_num", "0900001"),
            ("source", "manual_status_history"),
            ("confirmed_at", "2026-01-01"),
            ("confirmed_at", "2999-01-01T00:00:00Z"),
            ("schema_version", True),
            ("transfer_date", "invented"),
        ):
            proof = reference_for()["proof"]
            proof[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                validate_proof(proof)

    def test_no_automatic_capture_from_method_invoice_or_history_status(self):
        value = current_order()
        value["price_elements"] = [
            {"type": "payment", "title": "Bankovým prevodom", "reference_id": "6"}
        ]
        decision = decide_recovery(
            value,
            ["Stripe - expired"],
            creditnote_status="clear",
            verified_previous_status="Odoslaná",
        )
        self.assertEqual("review", decision.action)

    def test_creditnotes_returns_protected_statuses_and_blocked_order_win(self):
        for defect in (
            "creditnote",
            "unknown-creditnote",
            "return",
            "storno",
            "blocked",
            "shipment-missing",
        ):
            value, credits = current_order(), "clear"
            if defect == "creditnote":
                credits = "present"
            elif defect == "unknown-creditnote":
                credits = "unknown"
            elif defect == "return":
                value["shipments"] = [
                    {"status": "returned", "shipment_number": "fixture"}
                ]
            elif defect == "storno":
                value["status"]["name"] = "Storno"
            elif defect == "blocked":
                value["blocked"] = True
            else:
                value.pop("shipments")
            with self.subTest(defect=defect):
                decision = decide_recovery(
                    value,
                    ["Stripe - expired", "Storno"],
                    creditnote_status=credits,
                    project="vevo",
                    manual_settlement=confirmed(),
                )
                self.assertNotIn(decision.action, {"paid", "shipped"})

    def test_only_explicitly_bound_paid_target_can_restore_paid_without_shipment(self):
        value = current_order()
        record = confirmed(target={"id": "55", "name": "Platba online - zaplatené"})
        decision = decide_recovery(
            value,
            ["Stripe - expired"],
            creditnote_status="clear",
            project="vevo",
            manual_settlement=record,
        )
        self.assertEqual(
            ("paid", "Platba online - zaplatené"),
            (decision.action, decision.target_status_name),
        )
        value["shipments"] = [{"status": "delivered", "shipment_number": "fixture"}]
        decision = decide_recovery(
            value,
            ["Stripe - expired"],
            creditnote_status="clear",
            project="vevo",
            manual_settlement=record,
        )
        self.assertEqual("shipped", decision.action)

    def test_repeat_requires_same_proof_identity_target_and_strictly_new_generation(
        self,
    ):
        value, manual = current_order(), confirmed()
        value["last_change"] = "2026-05-03 10:00:00"
        previous = {
            "state": "verified",
            "target_status_name": "Odoslaná",
            "order_id": "101",
            "order_num": "900001",
            "manual_settlement_sha256": manual["evidence_sha256"],
            "source_last_change": "2026-05-01 10:00:00",
            "verified_last_change": "2026-05-02 10:00:00",
        }

        def block(record, current=value):
            return status_write_block_reason(
                record,
                project="vevo",
                current_order=current,
                next_target_status_name="Odoslaná",
                manual_settlement_sha256=manual["evidence_sha256"],
                source_statuses=["Stripe - expired"],
            )

        record = {
            "manual_settlement": manual,
            "status_mutation": previous,
            "phase": "complete",
        }
        self.assertEqual("", block(record))
        for key, changed in (
            ("state", "pending"),
            ("state", "uncertain"),
            ("order_id", "102"),
            ("manual_settlement_sha256", "b" * 64),
            ("target_status_name", "Platba online - zaplatené"),
            ("verified_last_change", value["last_change"]),
            ("source_last_change", "unavailable"),
        ):
            altered = deepcopy(record)
            altered["status_mutation"][key] = changed
            with self.subTest(key=key, changed=changed):
                self.assertTrue(block(altered))
        for field, changed in (
            ("phase", "create_ambiguous"),
            ("email_state", "sending"),
            ("email_state", "ambiguous"),
        ):
            self.assertTrue(block({**record, field: changed}))
        changed = deepcopy(value)
        changed["status"]["name"] = "Unreviewed source"
        self.assertTrue(block(record, changed))
        for status_id in (None, False, 0, "unknown"):
            changed = deepcopy(value)
            changed["status"]["id"] = status_id
            self.assertTrue(block(record, changed))
        changed = deepcopy(value)
        changed["last_change"] = None
        self.assertTrue(block(record, changed))


class ManualSettlementRecorderTests(unittest.TestCase):
    def test_source_gate_requires_clean_tracked_exact_live_remote_branch(self):
        root = Path.cwd().resolve()
        head, branch = "a" * 40, "codex/manual-fixture"
        outputs = {
            ("rev-parse", "--show-toplevel"): str(root),
            ("remote", "get-url", "origin"): "https://github.com/vzeman/biznisweb.git",
            ("status", "--porcelain", "--untracked-files=all"): "",
            ("branch", "--show-current"): branch,
            (
                "ls-files",
                "--error-unmatch",
                "scripts/record_verified_manual_settlement.py",
            ): "scripts/record_verified_manual_settlement.py",
            ("rev-parse", "HEAD"): head,
            ("rev-parse", "--abbrev-ref", "@{upstream}"): f"origin/{branch}",
            ("rev-parse", "@{upstream}"): head,
            (
                "ls-remote",
                "--exit-code",
                "origin",
                f"refs/heads/{branch}",
            ): f"{head}\trefs/heads/{branch}",
        }
        with patch(
            "scripts.record_verified_manual_settlement.subprocess.check_output",
            side_effect=lambda args, **kwargs: outputs[tuple(args[1:])],
        ):
            self.assertEqual(head, require_source(root))
            for key, changed in (
                (("status", "--porcelain", "--untracked-files=all"), " M source.py"),
                (("rev-parse", "--abbrev-ref", "@{upstream}"), "origin/other"),
                (("rev-parse", "@{upstream}"), "b" * 40),
                (
                    ("ls-remote", "--exit-code", "origin", f"refs/heads/{branch}"),
                    f"{'b' * 40}\trefs/heads/{branch}",
                ),
            ):
                before = outputs[key]
                outputs[key] = changed
                with self.subTest(key=key), self.assertRaises(ValueError):
                    require_source(root)
                outputs[key] = before

    def restored(self):
        value = current_order()
        value["status"] = {"id": 4, "name": "Odoslaná"}
        return value

    def test_confirmation_preview_and_apply_only_record_provenance_and_preserve_outbox(
        self,
    ):
        value, reference = self.restored(), reference_for()
        existing = {
            "order_num": "900001",
            "phase": "complete",
            "email_state": "sent",
            "invoice_id": "fixture",
        }
        store = store_for({"900001": existing})
        before_writes = len(store.client.writes)
        reads = []

        def read(number, **kwargs):
            reads.append(number)
            return deepcopy(value)

        record_proof(
            store=store,
            reference=reference,
            project="vevo",
            read_order=read,
            apply=False,
        )
        self.assertEqual(before_writes, len(store.client.writes))
        record_proof(
            store=store,
            reference=reference,
            project="vevo",
            read_order=read,
            apply=True,
        )
        stored = store.read()[0]["orders"]["900001"]
        for key, expected in existing.items():
            self.assertEqual(expected, stored[key])
        self.assertEqual(confirmed(), stored["manual_settlement"])
        self.assertEqual(["900001", "900001"], reads)
        self.assertTrue(
            all(row["Key"].endswith("/state.json") for row in store.client.writes)
        )
        self.assertIsNone(store.read()[0]["lease"])

    def test_recorder_refuses_unrestored_or_changed_order_and_ambiguous_operation(self):
        for defect in (
            "status",
            "id",
            "total",
            "blocked",
            "partial",
            "return",
            "pending-write",
            "ambiguous-email",
        ):
            value, record = self.restored(), {}
            if defect == "status":
                value["status"] = {"id": 69, "name": "Stripe - expired"}
            elif defect == "id":
                value["id"] = "102"
            elif defect == "total":
                value["sum"]["value"] = 101
            elif defect == "blocked":
                value["blocked"] = True
            elif defect == "partial":
                value["invoices"][0]["payments"] = [receipt(amount=10)]
            elif defect == "return":
                value["shipments"] = [
                    {"status": "returned", "shipment_number": "fixture"}
                ]
            elif defect == "pending-write":
                record["status_mutation"] = {"state": "pending"}
            else:
                record["email_state"] = "ambiguous"
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                check_current(value, record, reference_for(), "vevo")

    def test_explicit_revocation_is_bound_idempotent_and_never_reactivates_claim(self):
        original = confirmed()
        proof = {
            key: value
            for key, value in original["proof"].items()
            if key not in {"total", "currency", "recovery_status"}
        }
        proof.update(
            operation="revoke",
            source="explicit_user_withdrawal",
            revokes_sha256=original["evidence_sha256"],
        )
        reference = bind(proof)
        revoked = updated_record(
            original, reference, project="vevo", order=current_order()
        )
        self.assertEqual("revoked", revoked["state"])
        self.assertEqual(original["proof"], revoked["proof"])
        self.assertEqual(
            revoked,
            updated_record(revoked, reference, project="vevo", order=current_order()),
        )
        with self.assertRaises(ValueError):
            updated_record(
                revoked, reference_for(), project="vevo", order=current_order()
            )
        proof["revokes_sha256"] = "b" * 64
        with self.assertRaises(ValueError):
            updated_record(original, bind(proof), project="vevo", order=current_order())

    def test_lost_lease_or_cas_failure_cannot_claim_recorded(self):
        store = store_for()

        def read(*args, **kwargs):
            store.client.fail_write = True
            return self.restored()

        with self.assertRaises(RuntimeError):
            record_proof(
                store=store,
                reference=reference_for(),
                project="vevo",
                read_order=read,
                apply=True,
            )
        self.assertEqual({}, store.read()[0]["orders"])

    def test_runtime_foreign_project_and_ambient_api_override_are_rejected_or_isolated(
        self,
    ):
        secret = {
            "BIZNISWEB_API_URL": "https://vevo.flox.sk/api/graphql",
            "BIZNISWEB_API_TOKEN": "fixture",
        }
        settings = {"biznisweb_api_url": secret["BIZNISWEB_API_URL"]}
        for url in (
            "https://roy.flox.sk/api/graphql",
            "https://foreign.test/api/graphql",
            "http://vevo.flox.sk/api/graphql",
            "https://user@vevo.flox.sk/api/graphql",
            "https://vevo.flox.sk/api/graphql?x=1",
        ):
            with (
                self.subTest(url=url),
                self.assertRaises(ValueError),
                api_environment("vevo", {**secret, "BIZNISWEB_API_URL": url}, settings),
            ):
                pass
        import os

        with patch.dict(os.environ, {"VEVO_BIZNISWEB_API_TOKEN": "ambient"}):
            with api_environment("vevo", secret, settings):
                self.assertEqual("fixture", os.environ["VEVO_BIZNISWEB_API_TOKEN"])
            self.assertEqual("ambient", os.environ["VEVO_BIZNISWEB_API_TOKEN"])

    def test_private_object_requires_aes256_exact_hash_length_and_always_closes(self):
        raw = b'{"fixture":true}'
        digest = hashlib.sha256(raw).hexdigest()
        for defect in (None, "encryption", "length", "hash", "oversize"):
            body = BytesIO(raw)
            response = {
                "Body": body,
                "ContentLength": len(raw),
                "ServerSideEncryption": "AES256",
            }
            if defect == "encryption":
                response["ServerSideEncryption"] = "aws:kms"
            if defect == "length":
                response["ContentLength"] += 1
            client = SimpleNamespace(get_object=lambda **kwargs: response)
            with self.subTest(defect=defect):
                if defect:
                    with self.assertRaises(ValueError):
                        read_private(
                            client,
                            "key",
                            "b" * 64 if defect == "hash" else digest,
                            limit=2 if defect == "oversize" else 100,
                        )
                else:
                    self.assertEqual(
                        raw, read_private(client, "key", digest, limit=100)
                    )
                self.assertTrue(body.closed)

    def test_loader_reads_only_bound_canonical_private_proof_and_support_hashes(self):
        support = b'{"fixture":"explicit operator confirmation"}'
        proof = reference_for()["proof"]
        proof["supporting_evidence"][0]["sha256"] = hashlib.sha256(support).hexdigest()
        reference = bind(proof)
        objects = {
            reference["evidence_key"]: canonical_bytes(proof),
            proof["supporting_evidence"][0]["key"]: support,
        }
        calls, bodies = [], []

        def get(**kwargs):
            self.assertEqual(
                (BUCKET, ACCOUNT), (kwargs["Bucket"], kwargs["ExpectedBucketOwner"])
            )
            calls.append(kwargs["Key"])
            raw = objects[kwargs["Key"]]
            body = BytesIO(raw)
            bodies.append(body)
            return {
                "Body": body,
                "ContentLength": len(raw),
                "ServerSideEncryption": "AES256",
            }

        client = SimpleNamespace(get_object=get)
        self.assertEqual(
            reference, load_reference(client, "vevo", reference["evidence_sha256"])
        )
        self.assertEqual(list(objects), calls)
        self.assertTrue(all(body.closed for body in bodies))
        objects[proof["supporting_evidence"][0]["key"]] = b"{}"
        with self.assertRaises(ValueError):
            load_reference(client, "vevo", reference["evidence_sha256"])

    def test_recorder_source_contains_no_provider_mutation_or_web_client(self):
        import ast

        source = Path("scripts/record_verified_manual_settlement.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        calls = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertFalse(
            calls
            & {
                "post",
                "change_order_status",
                "change_status_verified",
                "send_invoice_email",
                "create_invoice",
            }
        )

    def test_bucket_owner_privacy_and_region_gates(self):
        class PrivateBucket:
            def head_bucket(self, **kwargs):
                self.kwargs = kwargs

            def get_bucket_location(self, **kwargs):
                return {"LocationConstraint": "eu-central-1"}

            def get_public_access_block(self, **kwargs):
                return {
                    "PublicAccessBlockConfiguration": dict.fromkeys(
                        (
                            "BlockPublicAcls",
                            "IgnorePublicAcls",
                            "BlockPublicPolicy",
                            "RestrictPublicBuckets",
                        ),
                        True,
                    )
                }

            def get_bucket_policy_status(self, **kwargs):
                return {"PolicyStatus": {"IsPublic": False}}

            def get_bucket_acl(self, **kwargs):
                return {
                    "Owner": {"ID": "owner"},
                    "Grants": [
                        {
                            "Grantee": {"Type": "CanonicalUser", "ID": "owner"},
                            "Permission": "FULL_CONTROL",
                        }
                    ],
                }

        client = PrivateBucket()
        require_private_bucket(client)
        self.assertEqual(ACCOUNT, client.kwargs["ExpectedBucketOwner"])
        with patch.object(
            client,
            "get_bucket_acl",
            return_value={
                "Owner": {"ID": "owner"},
                "Grants": [{"Grantee": {"Type": "Group"}, "Permission": "READ"}],
            },
        ):
            with self.assertRaises(ValueError):
                require_private_bucket(client)
        from tests.test_invoice_automation_state import S3Failure

        with patch.object(
            client,
            "get_bucket_policy_status",
            side_effect=S3Failure("NoSuchBucketPolicy"),
        ):
            require_private_bucket(client)
        with patch.object(
            client, "get_bucket_policy_status", side_effect=S3Failure("AccessDenied")
        ):
            with self.assertRaises(S3Failure):
                require_private_bucket(client)


if __name__ == "__main__":
    unittest.main()
