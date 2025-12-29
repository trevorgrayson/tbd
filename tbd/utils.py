import shutil

def ls(names, verbose=False):
    if not names:
        return

    if verbose:
        for name in names:
            print(name)
        return

    term_width = shutil.get_terminal_size(fallback=(80, 20)).columns
    max_len = max(len(n) for n in names) + 2  # padding
    cols = max(1, term_width // max_len)
    rows = (len(names) + cols - 1) // cols

    for r in range(rows):
        row = []
        for c in range(cols):
            i = c * rows + r
            if i < len(names):
                row.append(names[i].ljust(max_len))
        print("".join(row).rstrip())
