#!/usr/bin/env bash
# Security & PII check for committed files. Fails CI if a tracked file contains credential/token material,
# a real (non-placeholder) email, or a committed dotenv. Targeted, portable (ERE), low-false-positive.
# Placeholders are allowed: example.com / *.example.* domains and any noreply address.
set -uo pipefail

fail=0
report() { echo "::error::$1"; echo "$2"; fail=1; }

# tracked files, excluding this script and lockfiles
files() { git ls-files -- . ":!.github/scripts/pii-check.sh" ":!*.lock" ":!*-lock.yaml" ":!*-lock.json"; }

# credential/token material: private-key blocks + known provider token formats (OpenAI sk-, GitHub
# ghp_/gho_/…, Slack xox, AWS AKIA, Google AIza). Targeted formats keep false positives near zero.
creds=$(files | xargs -r grep -EnI \
  'BEGIN (RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY|sk-[A-Za-z0-9]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|xox[baprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{35}' \
  2>/dev/null || true)
[ -n "$creds" ] && report "credential/token material committed" "$creds"

# real emails: extract every address, drop placeholder/noreply/VCS-SSH addresses, report the rest.
# Allowed: example.* placeholders, any noreply address, and `git@<host>` SSH remote URLs (.gitmodules).
emails=$(files | xargs -r grep -EhoI '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}' 2>/dev/null \
  | sort -u \
  | grep -Eiv '@([A-Za-z0-9.-]*\.)?example\.[A-Za-z]+$|noreply|^git@' || true)
[ -n "$emails" ] && report "real (non-placeholder) email address committed" "$emails"

# committed dotenv (allow .env.example / .env.sample / .env.template)
dotenv=$(git ls-files | grep -E '(^|/)\.env($|\.[A-Za-z0-9]+$)' | grep -Ev '\.env\.(example|sample|template)$' || true)
[ -n "$dotenv" ] && report "dotenv file committed (use .env.example)" "$dotenv"

if [ "$fail" -ne 0 ]; then
  echo ""
  echo "PII check FAILED — use example.com / noreply placeholders and never commit a dotenv."
  exit 1
fi
echo "✓ security & PII check clean (credentials + email + dotenv)"
