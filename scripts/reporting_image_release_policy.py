"""Immutable project boundaries for the finite current-target report release."""
from dataclasses import dataclass
from types import MappingProxyType

ACCOUNT = "919341186960"
REGION = "eu-central-1"
BUCKET = f"biznisweb-reporting-artifacts-{ACCOUNT}-{REGION}"


@dataclass(frozen=True)
class ReportProjectPolicy:
    project: str
    family: str
    service: str
    sink: str
    role_prefix: str
    lease_key: str
    native_hosts: frozenset[str]

    @property
    def probe_prefix(self):
        return f"data/{self.project}/reporting/runtime/probes/"

    @property
    def release_prefix(self):
        return f"data/{self.project}/reporting/image-releases/"

    @property
    def probe_marker(self):
        return self.project.upper() + "_REPORT_PROBE_HOST_OK"

    @property
    def live_marker(self):
        return self.project.upper() + "_REPORT_IMAGE_LIVE_HOST_OK"

    def probe_policy(self, release_id):
        prefix = self.probe_prefix + release_id + "/"
        return {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [
                f"arn:aws:s3:::{BUCKET}/{self.sink}/*", f"arn:aws:s3:::{BUCKET}/{prefix}*"]},
            {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [
                f"arn:aws:s3:::{BUCKET}/{prefix}markers/*", f"arn:aws:s3:::{BUCKET}/{prefix}artifacts/*"],
             "Condition": {"StringEquals": {"s3:x-amz-server-side-encryption": "AES256"}}}]}


PROJECTS = MappingProxyType({
    "vevo": ReportProjectPolicy("vevo", "vevo-reporting-daily", "vevo-daily-report-email", "daily-reports/vevo",
        "VevoReportProbe-", "data/vevo/reporting/runtime/migration.json",
        frozenset({"vevo.sk", "www.vevo.sk", "vevo.flox.sk"})),
    "roy": ReportProjectPolicy("roy", "roy-reporting-daily", "roy-daily-report-email", "daily-reports/roy-sk",
        "RoyReportProbe-", "data/roy/reporting/runtime/image-release.json",
        frozenset({"roy.sk", "www.roy.sk", "roy.flox.sk"})),
})
VEVO = PROJECTS["vevo"]


def project_policy(project="vevo"):
    try:
        return PROJECTS[project]
    except (KeyError, TypeError):
        raise RuntimeError("image-release-project-not-allowlisted") from None
