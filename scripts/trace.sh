#!/bin/sh
#
# Requirements traceability. Every FR/NFR in docs/01-requirements.md must be named by at
# least one test, somewhere.
#
# This exists because FR-14 shipped unimplemented while every package reported 100%
# statement coverage. limits.max_message_size was defined, defaulted, validated and
# documented -- and read by no code at all. Coverage measures whether the lines that exist
# get executed. It cannot see a requirement that was never turned into lines. Nothing in
# the gate was capable of noticing, so nothing did, for eleven packages and twenty-two
# commits.
#
# A test naming a requirement is not proof the requirement is met. It is proof somebody
# thought about it, which is the part that was missing.
#
# Usage: scripts/trace.sh
set -eu

REQS=docs/01-requirements.md
missing=0

# Requirement ids as declared in the requirements document, in document order.
ids=$(grep -oE '\*\*(FR|NFR)-[0-9]+ —' "$REQS" | grep -oE '(FR|NFR)-[0-9]+' | sort -u -V)

printf '%-10s %s\n' "REQUIREMENT" "NAMED BY"
for id in $ids; do
	# Match the id as it appears in Go test identifiers and comments: FR-14 or FR14.
	alt=$(printf '%s' "$id" | tr -d '-')
	n=$(grep -rlE "(${id}|${alt})" --include='*_test.go' . 2>/dev/null | sort -u | grep -c . || true)
	if [ "${n:-0}" -gt 0 ]; then
		printf '%-10s %s\n' "$id" "${n} test file(s)"
		continue
	fi
	# A requirement that cannot be a Go test may be exempted in the requirements
	# document itself, on a line reading "*Verified:* <how>". Exemptions live beside
	# the requirement, in the diff, where a reviewer can argue with them -- the same
	# rule as "// coverage:" comments in source.
	if awk -v id="$id" '
		$0 ~ ("\\*\\*" id " —") { found = 1 }
		found && /^\*Verified:\*/  { print; exit }
		found && /^\*\*(FR|NFR)-/ && $0 !~ ("\\*\\*" id " —") { exit }
	' "$REQS" | grep -q .; then
		printf '%-10s %s\n' "$id" "exempt (*Verified:* in $REQS)"
		continue
	fi
	printf '%-10s %s\n' "$id" "NOTHING"
	missing=$((missing + 1))
done

echo
if [ "$missing" -gt 0 ]; then
	echo "FAIL: ${missing} requirement(s) named by no test." >&2
	echo "Either write the test, or delete the requirement -- a requirement nothing" >&2
	echo "checks is a wish. See the header of this script for how FR-14 shipped." >&2
	exit 1
fi
echo "OK: every requirement is named by at least one test."
