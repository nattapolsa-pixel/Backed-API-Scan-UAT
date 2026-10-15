# 1.9.10-free — Require complete dates in UAT Delivery Report

## Report safety

- New report rows now require a fresh `Wave_Monitoring` entry for the same Wave and branch.
- The planned pick date must be present so the existing business-date rules can produce all three report dates before a row is appended.
- Background reconciliation no longer rebuilds old report entries from the full `Member Data` history or uses `Booking & Wave` as a fallback source.
- Existing report rows with all three dates retain those dates when historical quantities are edited after `Wave_Monitoring` has been replaced.
- Existing rows with missing dates are left untouched by the writer so the authorized UAT cleanup can remove them; incomplete rows are not silently repaired or reinserted.

## Scope

- Backend UAT only; version `1.9.10-free`.
- The cleanup targets only the isolated UAT copy `1Am1cC8ORHgRfbyA_kfBEWQpDQZlKm1Ii8-wsx39o4xQ`, tab `Delivery report`.
- Production reports and Booking data are not written by this change.
