from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from lstore.page import Page

# Page identifier:
# (table_name, range_id, "base"/"tail", page_idx, col_idx)
PageKey = Tuple[str, int, str, int, int]


@dataclass
class Slot:
    """Represents one page sitting in the buffer."""
    key: PageKey
    page: Page
    pins: int = 0       # how many things are currently using this page
    dirty: bool = False # true if we changed the page and need to save it
    stamp: int = 0      # used for LRU (lower = older)
    

class BufferPool:
    """
    My bufferpool implementation.
    Basically: keeps pages in memory, evicts the least recently used page
    when we're full, and handles pin/unpin + dirty tracking.

    We store pages on disk under a root folder.

    What you actually use:
      - get_page(...) → returns the page + pins it
      - release_page(..., modified=True/False)
      - mark_dirty(...)
      - persist_all()  # flush everything to disk (I use this in DB.close)
    """

    def __init__(self, capacity: int, root_dir: Path | str):
        if capacity <= 0:
            raise ValueError("BufferPool capacity must be positive")

        self._limit = int(capacity)
        self._root = Path(root_dir)
        self._root.mkdir(parents=True, exist_ok=True)

        self._slots: Dict[PageKey, Slot] = {}
        self._clock: int = 0  # just a counter to keep track of LRU order

    # ---------------------------------------------------------------
    # Public functions
    # ---------------------------------------------------------------

    def get_page(self, table: str, range_id: int, segment: str, page_idx: int, col_idx: int) -> Page:
        """
        Grab a page. If it's already loaded, cool — just pin it and return it.
        If not, load it from disk (or make a new empty page) and cache it.
        If the buffer is full, evict the LRU unpinned page.
        """
        key: PageKey = (table, range_id, segment, page_idx, col_idx)

        slot = self._slots.get(key)
        if slot is not None:
            # already have it, update LRU + pin
            self._touch(slot)
            slot.pins += 1
            return slot.page

        # need to load it
        if len(self._slots) >= self._limit:
            self._evict_one()

        page = self._load_from_disk(key)
        self._clock += 1
        slot = Slot(key=key, page=page, pins=1, dirty=False, stamp=self._clock)
        self._slots[key] = slot
        return page

    def release_page(self, table: str, range_id: int, segment: str, page_idx: int, col_idx: int, modified: bool = False) -> None:
        """
        Unpin the page once you're done with it.
        If modified=True, mark it dirty so we know to write it back later.
        """
        key: PageKey = (table, range_id, segment, page_idx, col_idx)
        slot = self._slots.get(key)
        if slot is None:
            return

        if modified:
            slot.dirty = True

        if slot.pins > 0:
            slot.pins -= 1
            self._touch(slot)

    def mark_dirty(self, table: str, range_id: int, segment: str, page_idx: int, col_idx: int) -> None:
        """Just marks the page as dirty without touching pins."""
        key: PageKey = (table, range_id, segment, page_idx, col_idx)
        slot = self._slots.get(key)
        if slot is not None:
            slot.dirty = True

    def persist_all(self) -> None:
        """Write all dirty pages to disk. I use this when closing DB."""
        for slot in self._slots.values():
            if slot.dirty:
                self._write_to_disk(slot)
                slot.dirty = False

    def reset(self) -> None:
        """Writes everything then clears the buffer. Good for tests."""
        self.persist_all()
        self._slots.clear()

    # ---------------------------------------------------------------
    # Internal stuff
    # ---------------------------------------------------------------

    def _touch(self, slot: Slot) -> None:
        """Update LRU timestamp since we just used this page."""
        self._clock += 1
        slot.stamp = self._clock

    def _evict_one(self) -> None:
        """
        Kick out the least recently used page that is NOT pinned.
        If everything is pinned, that's bad — means the caller is holding too many pages.
        """
        victim: Optional[Slot] = None

        for s in self._slots.values():
            if s.pins > 0:
                continue
            if victim is None or s.stamp < victim.stamp:
                victim = s

        if victim is None:
            raise RuntimeError(
                f"BufferPool is full ({self._limit}) and all pages are pinned."
            )

        if victim.dirty:
            self._write_to_disk(victim)
        del self._slots[victim.key]

    # ------------------- disk I/O -------------------------

    def _file_path(self, key: PageKey) -> Path:
        """
        Turn a page key into a real file path.
        I’m just storing things as:
          root/table/range_X/segment/page_i_col_j.bin
        """
        table, range_id, segment, page_idx, col_idx = key
        directory = self._root / table / f"range_{range_id}" / segment
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"page_{page_idx}_col_{col_idx}.bin"

    def _load_from_disk(self, key: PageKey) -> Page:
        """Loads a page if it exists; otherwise returns an empty Page()."""
        path = self._file_path(key)
        page = Page()

        if not path.exists():
            return page

        try:
            raw = path.read_bytes()
            page.data[: len(raw)] = raw[: len(page.data)]
            return page
        except Exception:
            # if something goes wrong, just give a blank page instead
            return Page()

    def _write_to_disk(self, slot: Slot) -> None:
        """Writes this page's data out to disk."""
        path = self._file_path(slot.key)
        try:
            path.write_bytes(bytes(slot.page.data))
        except Exception:
            # ignoring disk errors for this project
            pass