import curses
import yaml
from pathlib import Path
from collections import OrderedDict
from tbd.models import Database, Table, Column


# ---------- YAML → MODELS ----------

def load_database_from_dbt(path: Path) -> Database:
    with open(path) as f:
        raw = yaml.safe_load(f)

    source = raw["sources"][0]  # single-source editing for now
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
        col_idx = 0
        view = "tables"  # tables | columns | edit

        def draw():
            stdscr.clear()
            h, w = stdscr.getmaxyx()

            stdscr.addstr(0, 2, f"dbt Source Editor — {path}", curses.A_BOLD)

            if view == "tables":
                stdscr.addstr(2, 2, "Tables", curses.A_UNDERLINE)
                for i, t in enumerate(db.tables):
                    attr = curses.A_REVERSE if i == table_idx else 0
                    stdscr.addstr(4 + i, 4, t.name, attr)

            elif view == "columns":
                table = db.tables[table_idx]
                stdscr.addstr(2, 2, f"Columns — {table.name}", curses.A_UNDERLINE)
                for i, c in enumerate(table.columns):
                    label = f"{c.name} : {c.dtype}"
                    if c.primary_key:
                        label += " [PK]"
                    attr = curses.A_REVERSE if i == col_idx else 0
                    stdscr.addstr(4 + i, 4, label, attr)

            elif view == "edit":
                table = db.tables[table_idx]
                col = table.columns[col_idx]
                stdscr.addstr(2, 2, f"Edit Column — {col.name}", curses.A_UNDERLINE)
                stdscr.addstr(4, 4, f"n Name        : {col.name}")
                stdscr.addstr(5, 4, f"t Type        : {col.dtype}")
                stdscr.addstr(6, 4, f"u Nullable    : {col.nullable}")
                stdscr.addstr(7, 4, f"p Primary Key : {col.primary_key}")
                stdscr.addstr(8, 4, f"q Unique      : {col.unique}")
                stdscr.addstr(9, 4, f"d Description : {col.metadata.get('description')}")

            stdscr.addstr(
                h - 1,
                2,
                "↑↓ Navigate  Enter Select  e Edit  a Add  r Rename  d Delete  s Save  q Quit",
                curses.A_DIM,
            )

            stdscr.refresh()

        def text_input(prompt, initial=""):
            curses.echo()
            stdscr.addstr(prompt)
            stdscr.clrtoeol()
            val = stdscr.getstr().decode()
            curses.noecho()
            return val if val else initial

        while True:
            draw()
            key = stdscr.getch()

            if key == ord("q"):
                break

            if view == "tables":
                if key == curses.KEY_UP:
                    table_idx = max(0, table_idx - 1)
                elif key == curses.KEY_DOWN:
                    table_idx = min(len(db.tables) - 1, table_idx + 1)
                elif key in (10, curses.KEY_ENTER):
                    view = "columns"
                    col_idx = 0

            elif view == "columns":
                table = db.tables[table_idx]

                if key == curses.KEY_UP:
                    col_idx = max(0, col_idx - 1)
                elif key == curses.KEY_DOWN:
                    col_idx = min(len(table.columns) - 1, col_idx + 1)
                elif key == ord("a"):
                    name = text_input("New column name: ")
                    dtype = text_input("Type: ")
                    table.add_column(Column(name=name, dtype=dtype))
                elif key == ord("r"):
                    old = table.columns[col_idx].name
                    new = text_input(f"Rename {old} → ")
                    table.rename_column(old, new)
                elif key == ord("d"):
                    name = table.columns[col_idx].name
                    table._columns.pop(name)
                    col_idx = max(0, col_idx - 1)
                elif key == ord("e"):
                    view = "edit"
                elif key == 27:
                    view = "tables"

            elif view == "edit":
                table = db.tables[table_idx]
                col = table.columns[col_idx]

                if key == ord("n"):
                    col.name = text_input("Name: ", col.name)
                elif key == ord("t"):
                    col.dtype = text_input("Type: ", col.dtype)
                elif key == ord("u"):
                    col.nullable = not bool(col.nullable)
                elif key == ord("p"):
                    col.primary_key = not bool(col.primary_key)
                elif key == ord("q"):
                    col.unique = not bool(col.unique)
                elif key == ord("d"):
                    col.metadata["description"] = text_input(
                        "Description: ",
                        col.metadata.get("description", ""),
                    )
                elif key == 27:
                    view = "columns"

            if key == ord("s"):
                write_database_to_dbt(db, raw)
                with open(path, "w") as f:
                    yaml.safe_dump(raw, f, sort_keys=False)

    curses.wrapper(curses_app)
