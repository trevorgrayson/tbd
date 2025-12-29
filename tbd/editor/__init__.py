import subprocess
from os import environ
from .ncurses import edit_dbt_sources_curses

EDITOR = environ.get("EDITOR")

def editor(filename):
    if EDITOR == 'ncurses':
        edit_dbt_sources_curses(filename)
    # present ncurses form editor
    # user should be able to fill out standard DBT Source YAML fields,
    # ownership, and contact information including a team, email,
    # and a slack channel
    if EDITOR is None:
        raise NotImplementedError("EDITOR environment variable not set")
    subprocess.run([EDITOR, filename])