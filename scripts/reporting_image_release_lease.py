"""Project scoped CAS lease; never reads or publishes a legacy current binding."""
from contextlib import closing

from scripts import reporting_runtime_binding as binding
from scripts.reporting_image_release_policy import ACCOUNT, BUCKET, project_policy


def read_lease(s3, project, *, optional=False):
    policy = project_policy(project)
    if project == "vevo":
        found = binding.read_object(s3, policy.lease_key, optional=optional)
        if found is not None:
            binding.validate_lock(found[0])
        return found
    try:
        response = s3.get_object(Bucket=BUCKET, Key=policy.lease_key, ExpectedBucketOwner=ACCOUNT)
    except Exception as exc:
        if optional and binding.error_code(exc) == "NoSuchKey":
            return None
        raise
    with closing(response["Body"]) as body:
        size = response.get("ContentLength")
        binding.require(response.get("ServerSideEncryption") == "AES256" and type(size) is int
                        and 0 < size <= binding.LIMIT, "runtime-object-metadata")
        raw = body.read(binding.LIMIT + 1)
    value = binding.decode_json(raw)
    binding.require(len(raw) == size and raw == binding.canonical_bytes(value)
                    and isinstance(response.get("ETag"), str) and response["ETag"], "runtime-object-noncanonical")
    binding.validate_lock(value)
    return value, response["ETag"]


def require_roy_idle(s3):
    """Also used by legacy VEVO mutation gates, including paused recovery."""
    found = read_lease(s3, "roy", optional=True)
    binding.require(found is None or found[0]["state"] == "released", "runtime-peer-release-active-or-uncertain")
    return found


class ScopedImageLease(binding.MigrationLease):
    def __init__(self, s3, *, owner, project, now=None):
        super().__init__(s3, owner=owner, now=now)
        self.policy = project_policy(project)
        binding.require(self.policy.project == "roy", "runtime-scoped-lease-project")

    def _read(self, *, optional=False):
        return read_lease(self.s3, self.policy.project, optional=optional)

    def _put(self, value, previous):
        raw = binding.canonical_bytes(value)
        self.s3.put_object(Bucket=BUCKET, Key=self.policy.lease_key, Body=raw, ContentType="application/json",
            ServerSideEncryption="AES256", ExpectedBucketOwner=ACCOUNT,
            **({"IfMatch": previous} if previous else {"IfNoneMatch": "*"}))
        observed, etag = self._read()
        binding.require(observed == value, "runtime-write-readback")
        return etag

    def _check(self, *, owned=False):
        found = self._read(optional=True)
        if found is None:
            binding.require(not owned, "runtime-lease-disappeared")
            return None
        value, etag = found
        if owned:
            binding.require(value["owner"] == self.owner and etag == self.etag and value["state"] == "active"
                            and binding.stamp(value["expires_at"]) > self.now(), "runtime-lease-not-owned")
        else:
            binding.require(value["state"] == "released", "runtime-migration-active-or-uncertain")
        return etag
