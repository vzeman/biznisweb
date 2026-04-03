# __CLIENT_DISPLAY_NAME__ Reporting Client Template

This folder is the starting point for a new BizniWeb reporting client.

## Files

- `settings.json`: project-level runtime and business rules
- `.env.example`: per-client secrets contract
- `product_expenses.json`: SKU/EAN-level COGS mapping

## First setup steps

1. Copy `.env.example` to `.env` in `projects/__CLIENT_SLUG__/`.
2. Fill `BIZNISWEB_API_URL` and `BIZNISWEB_API_TOKEN`.
3. Update `settings.json` with:
   - `report_from_date`
   - packaging / shipping / fixed cost rules
   - weather toggle and locations
4. Replace `product_expenses.json` with real purchase prices.
5. Run a smoke export:

```powershell
python export_orders.py --project __CLIENT_SLUG__ --from-date 2026-01-01 --to-date 2026-01-07
```

## Notes

- Do not commit the real `.env`.
- Keep client-specific business logic in `projects/__CLIENT_SLUG__/settings.json`, not in the core code.
