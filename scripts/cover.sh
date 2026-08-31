#!/bin/sh
#
# Coverage gate. docs/14-coding-standards.md §3 requires 100% statement coverage per
# package; this script is what checks it, and it is what CI runs.
#
# Two things it does that `go tool cover -func` alone does not:
#
#   1. It instruments every package with -coverpkg, so a package with no test files of
#      its own still appears in the profile as 0% rather than vanishing from the report.
#      A package that quietly drops out of a coverage gate is the exact failure the gate
#      exists to prevent.
#   2. It honours `// coverage: <reason>` comments. A line the standard says cannot
#      reasonably be covered is exempted by writing the reason next to it, in the diff,
#      where a reviewer can argue with it. Exemptions live in the source and never in a
#      list here, because a list here is a place to hide things.
#
# POSIX sh and awk on purpose: macOS ships bash 3.2, which has no associative arrays, and
# a build script that only runs on the maintainer's laptop is not a build script.
#
# Usage: scripts/cover.sh [profile]
set -eu

PROFILE="${1:-coverage.out}"
# A full template rather than `mktemp -t NAME`. BSD mktemp treats -t's argument as a
# prefix and appends randomness; GNU mktemp requires the template to contain XXXXXX and
# fails with "too few X's in template". The -t form therefore worked on the maintainer's
# macOS and failed on every Linux CI runner, which is how this script sat broken through
# twenty-seven commits while local runs were green.
: "${TMPDIR:=/tmp}"
PKGDIRS="$(mktemp "${TMPDIR%/}/stcover.XXXXXX")"
TESTLOG="$(mktemp "${TMPDIR%/}/sttest.XXXXXX")"
MERGED="$(mktemp "${TMPDIR%/}/stmerged.XXXXXX")"
trap 'rm -f "${PKGDIRS}" "${TESTLOG}" "${MERGED}"' EXIT

go list -f '{{.ImportPath}} {{.Dir}}' ./... > "${PKGDIRS}"

# The test output is captured rather than discarded, and replayed when the run fails.
# It was `> /dev/null` once, and combined with `set -e` that meant a failing test exited
# 1 with no output whatsoever: a red CI job with nothing in it to read. Coverage numbers
# are worthless if a test failed, so a failure prints the failures and stops before the
# gate reports a percentage that nobody should trust.
if ! go test -race -covermode=atomic -coverpkg=./... -coverprofile="${PROFILE}" ./... \
	> "${TESTLOG}" 2>&1; then
	echo "FAIL: the test run did not pass; coverage was not evaluated." >&2
	echo >&2
	grep -E '^(---|===|FAIL|ok .*FAIL|panic|[[:space:]]+[a-z_/.]+\.go:[0-9]+:)' "${TESTLOG}" >&2 \
		|| cat "${TESTLOG}" >&2
	exit 1
fi

# Merge the profile before reading it. With -coverpkg=./... every test binary reports
# every block in the module, so each block appears once per binary: covered in the run
# that exercised it, and zero in the ten that did not. `go tool cover` sums the counts for
# identical blocks; this did not, so it counted each package's statements eleven times
# over and reported every package at about a ninth of its real coverage — 14.3% for a
# package whose own tests cover all of it. Summing here is the same merge, done once,
# before the gate sees a single line.
{
	head -n 1 "${PROFILE}"
	awk 'NR > 1 { count[$1 " " $2] += $3 } END { for (block in count) print block, count[block] }' \
		"${PROFILE}" | sort
} > "${MERGED}"

awk -v module="$(go list -m)" '
	# First file: import path -> source directory.
	NR == FNR { pkgdir[$1] = $2; next }

	# Second file: the coverage profile. Line 1 is "mode: atomic".
	FNR == 1 { next }

	{
		block = $1; stmts = $2 + 0; count = $3 + 0
		c = index(block, ":")
		file = substr(block, 1, c - 1)
		span = substr(block, c + 1)

		s = file
		while (index(s, "/") > 0) { s = substr(s, index(s, "/") + 1) }
		rel = s
		pkg = substr(file, 1, length(file) - length(rel) - 1)

		if (!(pkg in total)) { total[pkg] = 0; ok[pkg] = 0 }
		total[pkg] += stmts

		if (count > 0) { ok[pkg] += stmts; next }

		# Uncovered. Exempt only if the block carries a "// coverage:" justification.
		comma = index(span, ",")
		start = substr(span, 1, index(span, ".") - 1) + 0
		tail = substr(span, comma + 1)
		end = substr(tail, 1, index(tail, ".") - 1) + 0

		path = pkgdir[pkg] "/" rel
		if (!(path in loaded)) {
			loaded[path] = 1
			n = 0
			while ((getline line < path) > 0) { n++; src[path, n] = line }
			close(path)
		}

		from = start - 3; if (from < 1) from = 1
		exempt = 0
		for (i = from; i <= end; i++) {
			if (index(src[path, i], "// coverage:") > 0) { exempt = 1; break }
		}
		if (exempt) { ok[pkg] += stmts; next }

		gaps = gaps sprintf("  %s:%d (%d stmt)\n", file, start, stmts)
	}

	END {
		prefix = module "/"
		printf "%-50s %8s %8s %8s\n", "PACKAGE", "STMTS", "COVERED", "PCT"
		fflush()
		fail = 0
		for (pkg in total) {
			short = pkg
			if (index(pkg, prefix) == 1) short = substr(pkg, length(prefix) + 1)
			if (total[pkg] == 0) {
				printf "%-50s %8d %8d %8s\n", short, 0, 0, "n/a" | "sort -k1,1"
				continue
			}
			pct = (ok[pkg] * 100) / total[pkg]
			printf "%-50s %8d %8d %7.1f%%\n", short, total[pkg], ok[pkg], pct | "sort -k1,1"
			if (ok[pkg] < total[pkg]) fail = 1
		}
		# Packages with no statements at all — pure type, interface and constant
		# declarations — contribute no profile blocks. List them anyway, so the report
		# accounts for every package go list knows about and a package cannot go missing.
		for (pkg in pkgdir) {
			if (pkg in total) continue
			short = pkg
			if (index(pkg, prefix) == 1) short = substr(pkg, length(prefix) + 1)
			printf "%-50s %8d %8d %8s\n", short, 0, 0, "n/a" | "sort -k1,1"
		}
		close("sort -k1,1")

		if (fail) {
			printf "\nUncovered statements with no // coverage: justification:\n"
			printf "%s", gaps
			printf "\nFAIL: coverage is below 100%% of statements.\n"
			printf "Write the test, or justify the line in place with a\n"
			printf "\"// coverage: <reason>\" comment. See docs/14-coding-standards.md §3.\n"
			exit 1
		}
		printf "\nOK: every package at 100%% (exemptions justified in source).\n"
	}
' "${PKGDIRS}" "${MERGED}"
