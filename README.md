ARMS Performance Entity Viewer

Streamlit app for browsing E10 entities with an end-user friendly UI: preset client picker, progress feedback, fast column selection (DuckDB), “Player Name” column, contact type defaults, exact + LIKE name search, and CSV/JSON downloads.

✨ Features

Client picker with “Other” option
Preloaded hosts + free entry for new clients.

Progress & status while fetching (no blank screens).

Player Name column auto-created (firstName + " " + lastName) and shown first.

Default filter: contactType = 1 (editable/clearable).

Player search

Exact multi-select (type to filter)

LIKE search (case-insensitive, e.g., Rol → Roland, Rolando…) using DuckDB ILIKE

Optional paste list; flags names that don’t exist

Fast column chooser (projection via DuckDB).

Downloads

Visible table (CSV)

Filtered full table (CSV)

Raw JSON

Quick ID export (IDs/ContactIDs)