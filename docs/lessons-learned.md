# Lessons learned

Decisions that changed because of evidence, while building this project. Each entry: what we assumed, what the data showed, what we changed, and the measured result.

## 1. A fixed reject-rate threshold produced mostly false alarms
- **Assumption:** alert Quality when a station rejects more than 3% of parts in an hour (with at least 20 parts handled).
- **What the data showed:** replaying the live session through the alert rules locally, before deploying, raised **22 alerts in 3 hours, and 14 of them were false** (64%). Stations handle very different volumes (~60 vs ~200 parts per hour), so on low-volume stations a single reject already crossed 3%. Over the 7-day history the rule fired 863 times.
- **Why it matters:** alert fatigue. A team that gets 22 emails a morning stops reading them, including the real ones.
- **Change:** statistical process control. The alert fires when a station is above its **p-chart upper control limit** (`p̄ + 3·√(p̄(1−p̄)/n)`), which adapts to volume (5.4% at 60 parts, 3.5% at 200), plus a 3% floor and at least 4 rejects. See ADR-010 in `docs/decisions.md`.
- **Result:** **7 alerts in the live session, all 7 on the stations using the defective lot.** Zero false alarms. Over the 7-day history: 66 alerts instead of 863.
- **Takeaway:** test alert rules against realistic data before deploying them. A threshold is a business decision, and its false-alarm rate is part of that decision.

## 2. Unit tests on pure logic missed a bug that only the real API shape revealed
- **Assumption:** the DynamoDB update `ADD consumed :quantity` was correct; the pure-logic tests passed.
- **What the data showed:** running the same code against **moto** (a local simulation of the AWS APIs) failed immediately: `consumed` is a **DynamoDB reserved word**. In AWS, every install event would have failed on the first deploy.
- **Change:** attribute names go through placeholders (`ADD #counter :quantity` with `ExpressionAttributeNames`). The Lambda's state layer is now always tested against moto: transactions, conditions and reserved words behave like the real service.
- **Result:** the bug was fixed before any deploy, at no cost. The moto tests also prove end to end that replaying the whole live session (a full Lambda retry) changes no stock and sends no repeated alerts.
- **Takeaway:** test pure logic with plain unit tests, and the AWS integration against a faithful simulation of the API, before paying for a deploy.

## 3. The toolchain's CPU architecture matters on Apple Silicon
- **Assumption:** `brew install terraform` gives a working Terraform.
- **What happened:** `terraform validate` failed with *"timeout while waiting for plugin to start"*, both in the sandbox and in a normal terminal. Homebrew was an Intel install (`/usr/local`, running under Rosetta), so it installed the Intel Terraform, which downloaded the Intel AWS provider (~700 MB). That provider couldn't initialize within Terraform's start-up timeout under emulation.
- **Change:** installed the official **native arm64** Terraform from releases.hashicorp.com, **verified its SHA256 checksum**, and re-initialized, which downloaded arm64 providers. Documented in `docs/configuration.md`.
- **Result:** `terraform validate` ran and immediately caught two real type errors in the configuration (an inconsistent conditional, and subscriber entries with different shapes). Both are fixed.
- **Takeaway:** when a tool "times out" for no clear reason on Apple Silicon, check `file $(which <tool>)` for the binary's architecture before debugging the code.
