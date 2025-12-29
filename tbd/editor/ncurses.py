import curses
import yaml
from pathlib import Path
from collections import OrderedDict
from tbd.models import *

# ---------- YAML ↔ MODELS ----------


def load_database_from_dbt(path: Path) -> Database:
    with open(path) as f:
        raw = yaml.safe_load(f)

    source = raw["sources"][0]  # single-source assumption
    db = Database(source["name"])

    for t in source.get("tables", []):
        table = Table(
            name=t["name"],
            description=t.get("description"),
            columns=t.get("columns", []),
        )
        db.add_table(table)

    return db, raw


def write_database_to_dbt(db: Database, raw: dict):
    source = raw["sources"][0]
    source["tables"] = [t.to_dict() for t in db.tables]


# ---------- CURSES EDITOR ----------

def edit_dbt_sources_curses(path: str | Path):
    path = Path(path)
    db, raw = load_database_from_dbt(path)

    def curses_app(stdscr):
        curses.curs_set(0)
        stdscr.keypad(True)

        table_idx = 0
        field_idx = 0

        def safe_addstr(y, x, text, attr=0):
            try:
                h, w = stdscr.getmaxyx()
                if y < 0 or y >= h:
                    return
                if x < 0 or x >= w:
                    return
                max_len = w - x - 1
                if max_len <= 0:
                    return
                stdscr.addstr(y, x, text[:max_len], attr)
            except curses.error:
                pass

        def text_input(y, x, prompt, initial=""):
            curses.echo()
            safe_addstr(y, x, prompt)
            stdscr.clrtoeol()
            value = stdscr.getstr(y, x + len(prompt)).decode()
            curses.noecho()
            return value if value else initial

        while True:
            stdscr.clear()
            h, w = stdscr.getmaxyx()
            split = max(24, w // 4)

            # ---------- LEFT PANE ----------
            safe_addstr(0, 2, "Tables", curses.A_BOLD)
            for i, table in enumerate(db.tables):
                attr = curses.A_REVERSE if i == table_idx else 0
                safe_addstr(2 + i, 2, table.name[: split - 4], attr)

            # ---------- RIGHT PANE ----------
            table = db.tables[table_idx]
            x0 = split + 2
            y = 1

            safe_addstr(y, x0, f"Table: {table.name}", curses.A_BOLD)
            y += 2

            fields = [("Table description", table.description)]

            for col in table.columns:
                fields.extend([
                    (f"Column: {col.name}", None),
                    ("  name", col.name),
                    ("  type", col.dtype),
                    ("  nullable", col.nullable),
                    ("  primary_key", col.primary_key),
                    ("  unique", col.unique),
                    ("  description", col.metadata.get("description")),
                ])

            # Render fields
            cursor_y_positions = []
            for i, (label, value) in enumerate(fields):
                attr = curses.A_REVERSE if i == field_idx else 0
                line = f"{label:<20} : {value}"
                safe_addstr(y, x0, line[: w - x0 - 2], attr)
                cursor_y_positions.append(y)
                y += 1

            # Footer
            safe_addstr(
                h - 1,
                2,
                "↑↓ Navigate  Enter Edit  Space Toggle  a Add Col  r Rename  d Delete  s Save  q Quit",
                curses.A_DIM,
            )

            stdscr.refresh()

            key = stdscr.getch()

            # ---------- GLOBAL ----------
            if key == ord("q"):
                break

            if key == ord("s"):
                write_database_to_dbt(db, raw)
                with open(path, "w") as f:
                    yaml.safe_dump(raw, f, sort_keys=False)

            # ---------- TABLE NAV ----------
            if key == curses.KEY_LEFT:
                table_idx = max(0, table_idx - 1)
                field_idx = 0
            elif key == curses.KEY_RIGHT:
                table_idx = min(len(db.tables) - 1, table_idx + 1)
                field_idx = 0

            # ---------- FIELD NAV ----------
            elif key == curses.KEY_UP:
                field_idx = max(0, field_idx - 1)
            elif key == curses.KEY_DOWN:
                field_idx = min(len(fields) - 1, field_idx + 1)

            # ---------- EDIT ----------
            elif key in (10, curses.KEY_ENTER):
                label, value = fields[field_idx]

                if label == "Table description":
                    table.description = text_input(
                        cursor_y_positions[field_idx],
                        x0,
                        "Table description: ",
                        table.description or "",
                    )

                elif label.strip() == "name":
                    col = table.columns[(field_idx - 1) // 7]
                    new = text_input(
                        cursor_y_positions[field_idx],
                        x0,
                        "Column name: ",
                        col.name,
                    )
                    table.rename_column(col.name, new)

                elif label.strip() == "type":
                    col = table.columns[(field_idx - 2) // 7]
                    col.dtype = text_input(
                        cursor_y_positions[field_idx],
                        x0,
                        "Type: ",
                        col.dtype,
                    )

                elif label.strip() == "description":
                    col = table.columns[(field_idx - 6) // 7]
                    col.metadata["description"] = text_input(
                        cursor_y_positions[field_idx],
                        x0,
                        "Description: ",
                        col.metadata.get("description", ""),
                    )

            # ---------- TOGGLES ----------
            elif key == ord(" "):
                label, _ = fields[field_idx]
                if label.strip() in {"nullable", "primary_key", "unique"}:
                    col = table.columns[(field_idx - 3) // 7]
                    setattr(col, label.strip(), not bool(getattr(col, label.strip())))

            # ---------- COLUMN OPS ----------
            elif key == ord("a"):
                name = text_input(h - 3, 2, "New column name: ")
                dtype = text_input(h - 2, 2, "Type: ")
                table.add_column(Column(name=name, dtype=dtype))

            elif key == ord("d"):
                label, _ = fields[field_idx]
                if label.startswith("Column:"):
                    col_name = label.split(":", 1)[1].strip()
                    table._columns.pop(col_name)
                    field_idx = max(0, field_idx - 1)

        curses.endwin()

    curses.wrapper(curses_app)
