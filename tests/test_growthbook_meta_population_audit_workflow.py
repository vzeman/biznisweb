from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "audit-vevo-growthbook-meta-population.yml"
POPULATION_SQL = ROOT / "projects" / "vevo" / "growthbook_sql" / "population_audit.sql"
VARIATIONS_SQL = ROOT / "projects" / "vevo" / "growthbook_sql" / "population_variations.sql"
ASSIGNMENT_SQL = ROOT / "projects" / "vevo" / "growthbook_sql" / "assignment.sql"
OUTCOME_SQL = ROOT / "projects" / "vevo" / "growthbook_sql" / "device_outcomes.sql"
PRODUCTION_OUTCOME_SQL = (
    ROOT / "projects" / "vevo" / "growthbook_sql" / "device_outcomes_production.sql"
)
PRODUCTION_PERFORMANCE_SQL = (
    ROOT / "projects" / "vevo" / "growthbook_sql" / "performance_vitals_production.sql"
)


class GrowthBookMetaPopulationAuditWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_audit_is_main_only_read_only_and_host_gated(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_audit:",
            "image_digest:",
            "META_AUDIT_PREDEPLOY_OK:",
            "META_AUDIT_HARD_GATE_OK:",
            "META_AUDIT_TASK_STOPPED_READBACK:",
            "META_AUDIT_TASK_OK:",
            "VEVO_GROWTHBOOK_REPORTING_POPULATION_AUDIT_OK:",
            "VEVO_META_DIMENSION_AUDIT_OK:",
            "VEVO_META_DIMENSION_AUDIT_FAIL:",
            '"REPORT_DATA_DIR"',
            '"/tmp/vevo-growthbook-meta-audit"',
            "python -m scripts.audit_vevo_meta_dimensions",
        ):
            self.assertIn(marker, self.workflow)
        lowered = self.workflow.lower()
        for forbidden in (
            "scheduler update-schedule",
            "ads_archive",
            "ads_update",
            "s3api delete-object",
            "cloudformation delete-stack",
        ):
            self.assertNotIn(forbidden, lowered)

    def test_population_sql_uses_the_frozen_eligible_device_contract(self) -> None:
        population = POPULATION_SQL.read_text(encoding="utf-8")
        variations = VARIATIONS_SQL.read_text(encoding="utf-8")
        for sql in (population, variations):
            self.assertIn("metric_contract_version = 'vevo_cm1_v1_2026-08-20'", sql)
            self.assertIn("eligible = 1", sql)
            self.assertIn("contaminated = 0", sql)
            self.assertNotIn("email", sql.lower())
            self.assertNotIn("order_num", sql.lower())
        self.assertIn("assignments_missing_outcomes", population)
        self.assertIn("invalid_meta_dimension_rows", population)

    def test_growthbook_assignment_and_outcome_queries_keep_same_population(self) -> None:
        for sql_path in (ASSIGNMENT_SQL, OUTCOME_SQL):
            sql = sql_path.read_text(encoding="utf-8")
            for predicate in (
                "metric_contract_version = 'vevo_cm1_v1_2026-08-20'",
                "eligible = 1",
                "contaminated = 0",
            ):
                self.assertEqual(sql.count(predicate), 1)

    def test_fact_queries_expose_only_a_wildcard_schema_probe(self) -> None:
        for sql_path in (PRODUCTION_OUTCOME_SQL, PRODUCTION_PERFORMANCE_SQL):
            sql = sql_path.read_text(encoding="utf-8").lower()
            marker_counts = {
                "union all": 1,
                "__growthbook_schema_only__": 1,
                "00000000-0000-4000-8000-000000000000": (
                    1 if sql_path == PRODUCTION_OUTCOME_SQL else 2
                ),
                "cast(current_timestamp as timestamp)": 1,
                "from (values (1)) as schema_seed(x)": 1,
                "where '{{ experimentid }}' = '%'": 1,
            }
            for marker, expected_count in marker_counts.items():
                self.assertEqual(expected_count, sql.count(marker), (sql_path, marker))
            self.assertGreater(
                sql.index("where '{{ experimentid }}' = '%'"),
                sql.index("union all"),
            )
            self.assertNotIn("or '{{ experimentid }}' = '%'", sql)
            self.assertIn("cast(null as", sql)

    def test_preview_fact_query_remains_probe_free(self) -> None:
        sql = OUTCOME_SQL.read_text(encoding="utf-8").lower()
        self.assertNotIn("__growthbook_schema_only__", sql)
        self.assertNotIn("where '{{ experimentid }}' = '%'", sql)


if __name__ == "__main__":
    unittest.main()
