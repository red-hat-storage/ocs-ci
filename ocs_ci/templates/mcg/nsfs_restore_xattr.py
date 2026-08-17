"""
Simulate tape recall on an NSFS archive target by manipulating xattrs.

Executed via python3 on a noobaa-endpoint pod by nsfs_simulate_archive_restore().
Walks $base_path to find the file with user.noobaa.restore.request, removes
that xattr, and sets user.noobaa.restore.expiry to $expiry - mimicking what
the tape backend does after a successful recall.

This is a string.Template - $base_path and $expiry are substituted at runtime.
"""

import os

base = "$base_path"
found = []
all_files = []
for root, dirs, files in os.walk(base):
    for f in files:
        path = os.path.join(root, f)
        try:
            xattrs = os.listxattr(path)
            all_files.append((path, xattrs))
            if "user.noobaa.restore.request" in xattrs:
                found.append(path)
        except OSError as ex:
            all_files.append((path, f"<listxattr failed: {ex}>"))
if not found:
    diag = "Files found under " + base + ":\n"
    for p, xa in all_files:
        diag += f"  {p} xattrs={xa}\n"
    if not all_files:
        diag += "  (none)\n"
    diag += "Expected at least one file with user.noobaa.restore.request xattr"
    raise AssertionError(diag)
assert len(found) == 1, (
    "Expected 1 file with restore.request, got " + str(len(found)) + ": " + str(found)
)
path = found[0]
os.removexattr(path, "user.noobaa.restore.request")
os.setxattr(path, "user.noobaa.restore.expiry", b"$expiry")
print(path)
