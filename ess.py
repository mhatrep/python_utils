#!/usr/bin/env python
"""
es_gui.py

Professional-looking PyQt6 GUI wrapper using fd + rg (no admin, no Everything).

Backend:
- fd.exe  → fast filename/path search
- rg.exe  → fast content search + match previews

Key Features:
- Filename/path search via fd (regex or literal)
- Exact match option (glob-based, whole-name)
- Case sensitive option
- Type filter: All / Files only / Folders only
- Folders to search:
    - Configure a list of folders
    - Option "Use only these folders for search"
    - Folder list persisted in search_config.json next to this script/EXE
    - Panel is hidden by default; toggle via "Folders ▸/▾" button in Search options
- Category filter: All, Text, Images, Audio, Video, Documents, Archives, Code
- Content search (case-insensitive) for ALL categories:
    - "Content contains" field: press Enter / Apply to filter files whose contents contain the keyword
    - Uses rg -l once over current results for performance
    - Hover over file name: show matching lines with line numbers & highlighted matches (lazy-loaded with rg)
- Client-side "Filter" box: filters current results (name or folder) as you type
- Result columns:
    - Name
    - Folder
    - File Size (human-readable for files)
    - Last Modified (YYYY-MM-DD HH:MM)
- Result table:
    - All columns same default width (user can resize)
    - Sorting enabled on all columns
    - Size & Last Modified sort numerically / by timestamp
    - Selected row highlighted with lighter color
- Copy selected/all results
- Open file or containing folder
- Double-click:
    - Name  → open file
    - Folder → open folder

Requirements:
- Windows (tested)
- fd.exe and rg.exe in the SAME DIRECTORY as this script / EXE
- PyQt6: pip install PyQt6
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from html import escape as html_escape
from datetime import datetime

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QCursor
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QLabel,
    QMessageBox,
    QCheckBox,
    QComboBox,
    QGroupBox,
    QToolTip,
    QListWidget,
    QFileDialog,
)
from PyQt6.QtWidgets import QHeaderView


# Category -> set of extensions (lowercase, with dot)
CATEGORY_EXTS = {
    "all": None,
    "text": {
        ".txt", ".md", ".log", ".ini", ".cfg", ".csv", ".tsv",
        ".json", ".xml", ".yaml", ".yml",
    },
    "images": {
        ".png", ".jpg", ".jpeg", ".gif", ".bmp",
        ".tif", ".tiff", ".webp", ".ico", ".svg",
    },
    "audio": {
        ".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a", ".wma",
    },
    "video": {
        ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
    },
    "documents": {
        ".pdf", ".doc", ".docx", ".rtf",
        ".xls", ".xlsx", ".ods",
        ".ppt", ".pptx", ".odp",
    },
    "archives": {
        ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2",
    },
    "code": {
        ".py", ".js", ".ts", ".jsx", ".tsx",
        ".java", ".cs", ".cpp", ".c", ".h", ".hpp",
        ".sql", ".html", ".htm", ".css", ".json",
        ".ipynb", ".ps1", ".sh", ".bat",
    },
}


# ---------------------------
# Helpers
# ---------------------------

def _norm_path(p: str) -> str:
    """Normalize path for comparison across tools / OS."""
    return os.path.normcase(os.path.abspath(p))


# ---------------------------
# Custom items for proper sorting
# ---------------------------

class SizeItem(QTableWidgetItem):
    """QTableWidgetItem that sorts by underlying byte size."""
    def __init__(self, text: str, size_bytes: int):
        super().__init__(text)
        self.size_bytes = size_bytes

    def __lt__(self, other):
        if isinstance(other, SizeItem):
            return self.size_bytes < other.size_bytes
        return super().__lt__(other)


class DateItem(QTableWidgetItem):
    """QTableWidgetItem that sorts by underlying timestamp."""
    def __init__(self, text: str, timestamp: float):
        super().__init__(text)
        self.timestamp = timestamp

    def __lt__(self, other):
        if isinstance(other, DateItem):
            return self.timestamp < other.timestamp
        return super().__lt__(other)


# ---------------------------
# Worker thread to run fd.exe
# ---------------------------

class SearchWorker(QThread):
    results_ready = pyqtSignal(list)   # list[str] of full paths
    error = pyqtSignal(str)

    def __init__(
        self,
        fd_path: Path,
        query: str,
        max_results: int,
        exact_match: bool,
        case_sensitive: bool,
        type_filter: str,       # "all" | "files" | "folders"
        roots: list[Path],
        parent=None,
    ):
        super().__init__(parent)
        self.fd_path = fd_path
        self.query = query
        self.max_results = max_results
        self.exact_match = exact_match
        self.case_sensitive = case_sensitive
        self.type_filter = type_filter
        self.roots = roots

    def run(self):
        if not self.fd_path.exists():
            self.error.emit(f"fd.exe not found at: {self.fd_path}")
            return

        if not self.roots:
            self.error.emit("No search roots configured.")
            return

        # Build fd command
        cmd = [
            str(self.fd_path),
            "--hidden",
            "--no-ignore",         # don't respect .gitignore, like Everything
            "--color", "never",
            "--max-results", str(self.max_results),
        ]

        # Type filter
        if self.type_filter == "files":
            cmd.extend(["--type", "f"])
        elif self.type_filter == "folders":
            cmd.extend(["--type", "d"])

        # Case-sensitivity
        if self.case_sensitive:
            cmd.append("--case-sensitive")

        pattern = self.query or ""

        # Exact match vs substring
        if pattern:
            if self.exact_match:
                # Treat pattern as glob, match name exactly
                cmd.extend(["--glob", pattern])
            else:
                # Literal substring search
                cmd.extend(["--fixed-strings", pattern])
        else:
            # No pattern → match everything under roots
            pattern = "."
            cmd.append(pattern)

        # Add roots
        for root in self.roots:
            cmd.append(str(root))

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as e:
            self.error.emit(f"Failed to run fd.exe: {e}")
            return

        # fd returns:
        #  0: matches found
        #  1: no matches
        #  2: error
        if proc.returncode not in (0, 1):
            msg = proc.stderr.strip() or f"fd.exe exited with code {proc.returncode}"
            self.error.emit(msg)
            return

        lines = proc.stdout.splitlines()
        paths = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            p = Path(line)
            if not p.is_absolute():
                p = Path(self.roots[0]) / p
            paths.append(str(p.resolve()))

        self.results_ready.emit(paths)


# ---------------------------
# Main Window
# ---------------------------

class EsGui(QMainWindow):
    def __init__(self):
        super().__init__()

        self.script_dir = Path(__file__).resolve().parent
        self.fd_path = self.script_dir / "fd.exe"
        self.rg_path = self.script_dir / "rg.exe"
        self.config_path = self.script_dir / "search_config.json"

        # Default search roots: drive root of home directory (e.g. C:\)
        home = Path.home()
        if home.drive:
            self.default_roots = [Path(home.drive + os.sep)]
        else:
            self.default_roots = [home]

        self.setWindowTitle("Everything-style Search – fd + rg")
        self.resize(1200, 720)

        self.worker: SearchWorker | None = None
        self.all_results: list[str] = []

        # Custom folders (persisted)
        self.custom_roots: list[str] = []
        self.use_custom_roots: bool = False

        # Active content keyword (applied when user hits Enter/Apply)
        self._active_content_kw: str | None = None

        # Normalized set of paths that match active content keyword (via rg -l)
        self._content_match_paths_norm: set[str] = set()

        # Cache for content preview: {(norm_path, keyword_lower): html_tooltip or None}
        self._content_preview_cache: dict[tuple[str, str], str | None] = {}

        self._build_ui()
        self._load_config()

    # ---------------------------
    # Config load/save
    # ---------------------------

    def _load_config(self):
        """Load folder list and settings from search_config.json."""
        if not self.config_path.exists():
            self._refresh_roots_list_widget()
            return

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}

        folders = data.get("folders", [])
        if isinstance(folders, list):
            self.custom_roots = [str(Path(p)) for p in folders]
        else:
            self.custom_roots = []

        self.use_custom_roots = bool(data.get("use_custom", False))
        self.chk_use_custom_roots.setChecked(self.use_custom_roots)

        self._refresh_roots_list_widget()

    def _save_config(self):
        """Save folder list and settings to search_config.json."""
        data = {
            "folders": self.custom_roots,
            "use_custom": self.chk_use_custom_roots.isChecked(),
        }
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            # Silent failure is ok for config
            pass

    # ---------------------------
    # Helpers for folder list UI
    # ---------------------------

    def _refresh_roots_list_widget(self):
        self.roots_list.clear()
        for p in self.custom_roots:
            self.roots_list.addItem(p)

    def _add_folder(self):
        directory = QFileDialog.getExistingDirectory(
            self,
            "Select folder to add to search list",
            str(self.default_roots[0]) if self.default_roots else str(Path.home()),
        )
        if directory:
            directory = str(Path(directory).resolve())
            if directory not in self.custom_roots:
                self.custom_roots.append(directory)
                self._refresh_roots_list_widget()
                self._save_config()

    def _remove_selected_folder(self):
        selected_items = self.roots_list.selectedItems()
        if not selected_items:
            return
        for item in selected_items:
            path = item.text()
            if path in self.custom_roots:
                self.custom_roots.remove(path)
        self._refresh_roots_list_widget()
        self._save_config()

    def _get_active_roots(self) -> list[Path]:
        """Return list of Path objects representing active search roots."""
        if self.chk_use_custom_roots.isChecked() and self.custom_roots:
            return [Path(p) for p in self.custom_roots]
        return self.default_roots

    # ---------------------------
    # UI setup
    # ---------------------------

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)

        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)
        central.setLayout(main_layout)

        # --- Search group ---
        search_group = QGroupBox("Search")
        sg_layout = QHBoxLayout()
        sg_layout.setContentsMargins(8, 8, 8, 8)
        sg_layout.setSpacing(8)

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText(
            "Filename pattern (fd syntax – literal or regex). Press Enter to search…"
        )
        self.search_edit.returnPressed.connect(self.start_search)

        self.btn_search = QPushButton("Search")
        self.btn_search.setFixedWidth(100)
        self.btn_search.clicked.connect(self.start_search)

        sg_layout.addWidget(self.search_edit, stretch=1)
        sg_layout.addWidget(self.btn_search)
        search_group.setLayout(sg_layout)
        main_layout.addWidget(search_group)

        # --- Search options group ---
        opts_group = QGroupBox("Search options (fd)")
        opts_layout = QHBoxLayout()
        opts_layout.setContentsMargins(8, 8, 8, 8)
        opts_layout.setSpacing(12)

        self.chk_exact = QCheckBox("Exact name match")
        self.chk_exact.setToolTip(
            "Use fd's glob mode and match the file name exactly.\n"
            "Pattern is treated as a glob, not a regex."
        )

        self.chk_case = QCheckBox("Case sensitive")
        self.chk_case.setToolTip("Force fd to perform a case-sensitive search.")

        self.combo_type = QComboBox()
        self.combo_type.addItem("All items", userData="all")
        self.combo_type.addItem("Files only", userData="files")
        self.combo_type.addItem("Folders only", userData="folders")
        self.combo_type.setToolTip("Filter results by type using fd (files/folders).")

        opts_layout.addWidget(self.chk_exact)
        opts_layout.addWidget(self.chk_case)
        opts_layout.addWidget(QLabel("Item type:"))
        opts_layout.addWidget(self.combo_type)
        opts_layout.addStretch(1)

        # Toggle button for folders panel
        self.btn_toggle_folders = QPushButton("Folders ▸")
        self.btn_toggle_folders.setToolTip("Show/hide the 'Folders to search' panel.")
        self.btn_toggle_folders.setCheckable(True)
        self.btn_toggle_folders.setChecked(False)
        self.btn_toggle_folders.clicked.connect(self.toggle_folders_panel)
        opts_layout.addWidget(self.btn_toggle_folders)

        opts_group.setLayout(opts_layout)
        main_layout.addWidget(opts_group)

        # --- Folders to search group (hidden by default, toggled) ---
        self.folders_group = QGroupBox("Folders to search")
        fg_layout = QHBoxLayout()
        fg_layout.setContentsMargins(8, 8, 8, 8)
        fg_layout.setSpacing(8)

        self.chk_use_custom_roots = QCheckBox("Use only these folders for search")
        self.chk_use_custom_roots.setToolTip(
            "If checked, searches are restricted to the folders in the list below.\n"
            "If unchecked, searches use the default root (e.g., drive of your home directory)."
        )
        self.chk_use_custom_roots.stateChanged.connect(lambda _: self._save_config())

        self.roots_list = QListWidget()
        self.roots_list.setSelectionMode(self.roots_list.SelectionMode.ExtendedSelection)
        self.roots_list.setMinimumHeight(80)

        btns_layout = QVBoxLayout()
        self.btn_add_folder = QPushButton("Add folder…")
        self.btn_add_folder.clicked.connect(self._add_folder)
        self.btn_remove_folder = QPushButton("Remove selected")
        self.btn_remove_folder.clicked.connect(self._remove_selected_folder)

        btns_layout.addWidget(self.btn_add_folder)
        btns_layout.addWidget(self.btn_remove_folder)
        btns_layout.addStretch(1)

        fg_left = QVBoxLayout()
        fg_left.addWidget(self.chk_use_custom_roots)
        fg_left.addWidget(self.roots_list)

        fg_layout.addLayout(fg_left, stretch=3)
        fg_layout.addLayout(btns_layout, stretch=1)

        self.folders_group.setLayout(fg_layout)
        self.folders_group.setVisible(False)   # default hidden
        main_layout.addWidget(self.folders_group)

        # --- Filters group ---
        filters_group = QGroupBox("Result filters (client-side)")
        filters_layout = QHBoxLayout()
        filters_layout.setContentsMargins(8, 8, 8, 8)
        filters_layout.setSpacing(12)

        self.combo_category = QComboBox()
        self.combo_category.addItem("All categories", userData="all")
        self.combo_category.addItem("Text files", userData="text")
        self.combo_category.addItem("Images", userData="images")
        self.combo_category.addItem("Audio", userData="audio")
        self.combo_category.addItem("Video", userData="video")
        self.combo_category.addItem("Documents", userData="documents")
        self.combo_category.addItem("Archives", userData="archives")
        self.combo_category.addItem("Code", userData="code")
        self.combo_category.setToolTip("Limit results to a category based on file extension.")
        self.combo_category.currentIndexChanged.connect(self.refresh_table)

        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("Filter by file name or folder path…")
        self.filter_edit.textChanged.connect(self.refresh_table)
        self.filter_edit.setClearButtonEnabled(True)

        self.content_edit = QLineEdit()
        self.content_edit.setPlaceholderText("Content contains (case-insensitive)…")
        self.content_edit.setToolTip(
            "Type content keyword and press Enter or click Apply.\n"
            "Uses ripgrep (rg.exe) once over current results.\n"
            "Hover a file name to see matching lines."
        )
        self.content_edit.setClearButtonEnabled(True)
        self.content_edit.returnPressed.connect(self.apply_content_filter)

        self.btn_apply_content = QPushButton("Apply")
        self.btn_apply_content.setToolTip("Apply content filter to current results.")
        self.btn_apply_content.clicked.connect(self.apply_content_filter)

        filters_layout.addWidget(QLabel("Category:"))
        filters_layout.addWidget(self.combo_category, stretch=1)
        filters_layout.addSpacing(10)
        filters_layout.addWidget(QLabel("Name / folder filter:"))
        filters_layout.addWidget(self.filter_edit, stretch=2)
        filters_layout.addSpacing(10)
        filters_layout.addWidget(QLabel("Content contains:"))
        filters_layout.addWidget(self.content_edit, stretch=2)
        filters_layout.addWidget(self.btn_apply_content)

        filters_group.setLayout(filters_layout)
        main_layout.addWidget(filters_group)

        # --- Actions row ---
        actions_layout = QHBoxLayout()
        actions_layout.setSpacing(8)

        self.btn_copy_selected = QPushButton("Copy selected")
        self.btn_copy_selected.clicked.connect(self.copy_selected)

        self.btn_copy_all = QPushButton("Copy all")
        self.btn_copy_all.clicked.connect(self.copy_all)

        self.btn_open_file = QPushButton("Open file")
        self.btn_open_file.clicked.connect(self.open_selected_file)

        self.btn_open_folder = QPushButton("Open folder")
        self.btn_open_folder.clicked.connect(self.open_selected_folder)

        actions_layout.addWidget(self.btn_copy_selected)
        actions_layout.addWidget(self.btn_copy_all)
        actions_layout.addSpacing(16)
        actions_layout.addWidget(self.btn_open_file)
        actions_layout.addWidget(self.btn_open_folder)
        actions_layout.addStretch(1)

        main_layout.addLayout(actions_layout)

        # --- Results table ---
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Name", "Folder", "Size", "Last Modified"])
        self.table.setSelectionBehavior(self.table.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(self.table.EditTrigger.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.setToolTip(
            "Results from fd.\n"
            "Click column headers to sort.\n"
            "Double-click Name to open file.\n"
            "Double-click Folder to open folder.\n"
            "Hover Name for content preview (when content filter is active)."
        )
        # Enable hover tracking so itemEntered fires
        self.table.setMouseTracking(True)
        self.table.viewport().setMouseTracking(True)

        # Light highlight for selected row
        self.table.setStyleSheet(
            """
            QTableWidget::item:selected {
                background-color: #cce7ff;
                color: black;
            }
            """
        )

        # Enable sorting on columns
        self.table.setSortingEnabled(True)

        header = self.table.horizontalHeader()
        # All columns same default size; user can adjust
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setDefaultSectionSize(250)  # same width for all columns initially

        # Use cellDoubleClicked for reliable double-click handling
        self.table.cellDoubleClicked.connect(self.on_cell_double_click)

        # Lazy tooltip loading on hover
        self.table.itemEntered.connect(self.on_item_entered)

        main_layout.addWidget(self.table, stretch=1)

        # --- Status label ---
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet("color: gray; font-size: 11px;")
        main_layout.addWidget(self.status_label)

    # ---------------------------
    # Toggle folders panel
    # ---------------------------

    def toggle_folders_panel(self):
        visible = self.folders_group.isVisible()
        new_visible = not visible
        self.folders_group.setVisible(new_visible)
        self.btn_toggle_folders.setChecked(new_visible)
        self.btn_toggle_folders.setText("Folders ▾" if new_visible else "Folders ▸")

    # ---------------------------
    # Size formatting helper
    # ---------------------------

    @staticmethod
    def _format_size(num_bytes: int) -> str:
        """Return human-readable file size."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if num_bytes < 1024 or unit == "TB":
                return f"{num_bytes:.0f} {unit}" if unit == "B" else f"{num_bytes:.1f} {unit}"
            num_bytes /= 1024.0
        return f"{num_bytes:.1f} TB"

    # ---------------------------
    # Search logic (fd)
    # ---------------------------

    def start_search(self):
        query = self.search_edit.text().strip()

        if not self.fd_path.exists():
            QMessageBox.critical(
                self,
                "Missing fd.exe",
                "fd.exe was not found.\n\n"
                "Place fd.exe in the same folder as this application."
            )
            return

        # If a previous search is still running, ignore
        if self.worker is not None and self.worker.isRunning():
            return

        exact_match = self.chk_exact.isChecked()
        case_sensitive = self.chk_case.isChecked()
        type_filter = self.combo_type.currentData()

        roots = self._get_active_roots()
        if not roots:
            QMessageBox.information(
                self,
                "No folders to search",
                "No search roots configured.\n\n"
                "Either add folders in 'Folders to search' or disable the option."
            )
            return

        self.status_label.setText("Searching with fd…")
        self.btn_search.setEnabled(False)
        self.table.setRowCount(0)
        self.all_results = []

        # Clear any previous content filter since result set changed
        self._active_content_kw = None
        self._content_match_paths_norm.clear()
        self._content_preview_cache.clear()

        self.worker = SearchWorker(
            fd_path=self.fd_path,
            query=query,
            max_results=1000,
            exact_match=exact_match,
            case_sensitive=case_sensitive,
            type_filter=type_filter,
            roots=roots,
        )
        self.worker.results_ready.connect(self.on_search_results)
        self.worker.error.connect(self.on_search_error)
        self.worker.finished.connect(self.on_search_finished)
        self.worker.start()

    def on_search_results(self, paths: list[str]):
        self.all_results = paths
        self.refresh_table()

    def on_search_error(self, message: str):
        QMessageBox.critical(self, "Search error", message)

    def on_search_finished(self):
        self.btn_search.setEnabled(True)

    # ---------------------------
    # Content filter logic (rg -l once, batched)
    # ---------------------------

    def apply_content_filter(self):
        """Apply or clear the content filter using rg -l over current results (batched to avoid WinError 206)."""
        raw_kw = self.content_edit.text().strip()
        kw_lower = raw_kw.lower()

        # Clear active filter if empty
        if not raw_kw:
            self._active_content_kw = None
            self._content_match_paths_norm.clear()
            self._content_preview_cache.clear()
            self.refresh_table()
            self.status_label.setText("Content filter cleared.")
            return

        if not self.rg_path.exists():
            QMessageBox.critical(
                self,
                "Missing rg.exe",
                "rg.exe (ripgrep) was not found.\n\n"
                "Place rg.exe in the same folder as this application.\n"
                "Content filtering cannot be applied."
            )
            return

        # Only consider files from all_results
        file_candidates = [p for p in self.all_results if os.path.isfile(p)]
        if not file_candidates:
            self._active_content_kw = None
            self._content_match_paths_norm.clear()
            self._content_preview_cache.clear()
            self.refresh_table()
            self.status_label.setText("No files to apply content filter on.")
            return

        self.status_label.setText(f"Applying content filter '{raw_kw}' with rg…")
        QApplication.processEvents()

        # Base command (literal, case-insensitive)
        base_cmd = [
            str(self.rg_path),
            "-l",
            "-i",
            "--fixed-strings",     # treat pattern as a literal string, not regex
            "--color", "never",
            raw_kw,
            "--",                  # end of options; following args are file paths
        ]

        # Approximate Windows command line max length
        MAX_CMD_LENGTH = 30000

        def run_batch(files: list[str]):
            """Run rg on a batch of files and return (returncode, stdout, stderr)."""
            if not files:
                return 0, "", ""
            cmd = base_cmd + files
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            return proc.returncode, proc.stdout, proc.stderr

        match_paths_norm: set[str] = set()
        any_stdout = False
        last_error_msg = ""

        # Precompute base length
        base_len = sum(len(part) + 1 for part in base_cmd)

        batch: list[str] = []
        batch_len = base_len

        for path in file_candidates:
            path_len = len(path) + 1
            if batch and (batch_len + path_len > MAX_CMD_LENGTH):
                # Run current batch
                rc, out, err = run_batch(batch)
                if out.strip():
                    any_stdout = True
                    for line in out.splitlines():
                        line = line.strip()
                        if line:
                            match_paths_norm.add(_norm_path(line))
                elif rc not in (0, 1) and not any_stdout:
                    last_error_msg = f"rg.exe exited with code {rc}.\n\n{err}"

                # Reset batch
                batch = []
                batch_len = base_len

            batch.append(path)
            batch_len += path_len

        # Run last batch
        if batch:
            rc, out, err = run_batch(batch)
            if out.strip():
                any_stdout = True
                for line in out.splitlines():
                    line = line.strip()
                    if line:
                        match_paths_norm.add(_norm_path(line))
            elif rc not in (0, 1) and not any_stdout:
                last_error_msg = f"rg.exe exited with code {rc}.\n\n{err}"

        # If nothing succeeded and we had an error, surface it
        if not any_stdout and last_error_msg:
            QMessageBox.critical(
                self,
                "Content filter error",
                last_error_msg
            )
            # Do not change existing content filter in this case
            return

        # Apply filter (it's okay if there are 0 matches; just show 0)
        self._active_content_kw = kw_lower
        self._content_match_paths_norm = match_paths_norm
        self._content_preview_cache.clear()
        self.refresh_table()

        msg = f"Content filter applied: '{raw_kw}', {len(match_paths_norm)} matching file(s)."
        self.status_label.setText(msg)

    # ---------------------------
    # Lazy content preview helper (rg -n per file on hover)
    # ---------------------------

    def _get_content_preview(
        self,
        norm_path_key: str,
        keyword_lower: str,
        max_matches: int = 5,
    ) -> str | None:
        """
        Use ripgrep (rg.exe) to get matching lines with line numbers.
        Return HTML tooltip with highlighted keyword, or None if no match / error.

        Cache key: (norm_path_key, keyword_lower)
        """
        key = (norm_path_key, keyword_lower)
        if key in self._content_preview_cache:
            return self._content_preview_cache[key]

        if not self.rg_path.exists():
            self._content_preview_cache[key] = None
            return None

        real_path = norm_path_key  # already absolute; but we can re-normalize just in case
        if not os.path.exists(real_path):
            self._content_preview_cache[key] = None
            return None

        cmd = [
            str(self.rg_path),
            "-n",
            "-i",
            "--fixed-strings",     # literal search again
            "--no-heading",
            "--no-filename",
            "--color", "never",
            keyword_lower,
            real_path,
        ]

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception:
            self._content_preview_cache[key] = None
            return None

        if proc.returncode == 1 or not proc.stdout.strip():
            self._content_preview_cache[key] = None
            return None
        if proc.returncode not in (0, 1):
            self._content_preview_cache[key] = None
            return None

        raw_lines = proc.stdout.splitlines()
        matches: list[str] = []

        for raw in raw_lines:
            parts = raw.split(":", 2)
            if len(parts) == 3:
                line_no_str, _col_str, text = parts
            elif len(parts) == 2:
                line_no_str, text = parts
            else:
                continue

            try:
                line_no = int(line_no_str)
            except ValueError:
                continue

            line = text
            lower_line = line.lower()

            if keyword_lower not in lower_line:
                continue

            # Highlight all occurrences
            marked = []
            i = 0
            while True:
                j = lower_line.find(keyword_lower, i)
                if j == -1:
                    marked.append(line[i:])
                    break
                marked.append(line[i:j])
                marked.append("<<HIGHLIGHT>>")
                marked.append(line[j:j + len(keyword_lower)])
                marked.append("<<END>>")
                i = j + len(keyword_lower)

            joined = "".join(marked)
            safe = html_escape(joined)
            safe = safe.replace(
                "&lt;&lt;HIGHLIGHT&gt;&gt;",
                "<span style='background-color: yellow; font-weight: bold;'>"
            ).replace(
                "&lt;&lt;END&gt;&gt;",
                "</span>"
            )
            line_html = f"<span style='color:#888;'>[{line_no}]</span> {safe}"
            matches.append(line_html)

            if len(matches) >= max_matches:
                break

        if not matches:
            self._content_preview_cache[key] = None
            return None

        more = ""
        if len(raw_lines) > len(matches):
            more = "<br><span style='color:#888;'>… more matches not shown …</span>"

        body = "<br>".join(matches) + more
        tooltip = (
            "<html><body style='font-family: Consolas, monospace; white-space: pre;'>"
            f"{body}"
            "</body></html>"
        )
        self._content_preview_cache[key] = tooltip
        return tooltip

    # ---------------------------
    # Table population + live filtering
    # ---------------------------

    def refresh_table(self):
        """Rebuild the table from self.all_results using category + name filter + active content keyword."""
        # Keep current sort state
        sorting_enabled = self.table.isSortingEnabled()
        self.table.setSortingEnabled(False)

        self.table.setRowCount(0)

        if not self.all_results:
            self.status_label.setText("No results")
            self.table.setSortingEnabled(sorting_enabled)
            return

        filter_text = self.filter_edit.text().strip().lower()
        category_key = self.combo_category.currentData() or "all"
        ext_set = CATEGORY_EXTS.get(category_key)

        kw = self._active_content_kw
        require_content = bool(kw)

        shown = 0
        total = len(self.all_results)

        for path in self.all_results:
            name = os.path.basename(path)
            folder = os.path.dirname(path)
            ext = Path(path).suffix.lower()
            norm = _norm_path(path)

            # Category filter (by extension)
            if ext_set is not None and ext not in ext_set:
                continue

            # Live name/folder filter
            if filter_text:
                if filter_text not in name.lower() and filter_text not in folder.lower():
                    continue

            # Content filter via precomputed match set
            if require_content:
                if not os.path.isfile(path):
                    continue
                if norm not in self._content_match_paths_norm:
                    continue

            # Compute size & mtime
            size_str = ""
            size_bytes = 0
            mtime_str = ""
            mtime_ts = 0.0
            try:
                st = os.stat(path)
                if os.path.isfile(path):
                    size_bytes = st.st_size
                    size_str = self._format_size(st.st_size)
                mtime_ts = st.st_mtime
                dt = datetime.fromtimestamp(st.st_mtime)
                mtime_str = dt.strftime("%Y-%m-%d %H:%M")
            except OSError:
                pass

            row = self.table.rowCount()
            self.table.insertRow(row)

            item_name = QTableWidgetItem(name)
            item_folder = QTableWidgetItem(folder)
            item_size = SizeItem(size_str, size_bytes)
            item_mtime = DateItem(mtime_str, mtime_ts)

            # Store both raw path and normalized path
            item_name.setData(Qt.ItemDataRole.UserRole, path)
            item_name.setData(Qt.ItemDataRole.UserRole + 1, norm)
            # Default tooltip = full path; preview added lazily on hover
            item_name.setToolTip(path)

            # Right-align size
            item_size.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

            self.table.setItem(row, 0, item_name)
            self.table.setItem(row, 1, item_folder)
            self.table.setItem(row, 2, item_size)
            self.table.setItem(row, 3, item_mtime)

            shown += 1

        # Re-enable sorting
        self.table.setSortingEnabled(sorting_enabled)

        # Status message
        if require_content and kw:
            self.status_label.setText(
                f"{shown} result(s) shown (content contains '{kw}') from {total} total"
            )
        else:
            self.status_label.setText(f"{shown} result(s) shown (from {total} total)")

    # ---------------------------
    # Double-click + hover behavior
    # ---------------------------

    def on_cell_double_click(self, row: int, column: int):
        """Open file/folder based on double-clicked cell."""
        item_name = self.table.item(row, 0)
        item_folder = self.table.item(row, 1)

        if not item_name or not item_folder:
            return

        full_path = item_name.data(Qt.ItemDataRole.UserRole)
        folder_path = item_folder.text()

        if column == 0 and full_path:
            self._open_path(full_path)
        elif column == 1 and folder_path:
            self._open_path(folder_path)

    def on_item_entered(self, item: QTableWidgetItem):
        """Lazy-load tooltip preview when hovering over the Name column."""
        if item.column() != 0:
            return

        path = item.data(Qt.ItemDataRole.UserRole)
        if not path:
            return

        norm = item.data(Qt.ItemDataRole.UserRole + 1)
        if not norm:
            norm = _norm_path(path)

        kw = self._active_content_kw
        if not kw:
            # No content filter → keep default tooltip (path)
            if not item.toolTip():
                item.setToolTip(path)
            return

        # If tooltip already looks like HTML preview, just show it
        tt = item.toolTip() or ""
        if tt.startswith("<html>"):
            QToolTip.showText(QCursor.pos(), tt, self.table)
            return

        preview = self._get_content_preview(norm, kw)
        if preview:
            item.setToolTip(preview)
            QToolTip.showText(QCursor.pos(), preview, self.table)
        else:
            item.setToolTip(path)
            QToolTip.showText(QCursor.pos(), path, self.table)

    # ---------------------------
    # Helpers to get selections
    # ---------------------------

    def _get_selected_paths(self) -> list[str]:
        paths: list[str] = []
        if not self.table.selectionModel():
            return paths

        for idx in self.table.selectionModel().selectedRows():
            row = idx.row()
            item_name = self.table.item(row, 0)
            if item_name is None:
                continue
            full_path = item_name.data(Qt.ItemDataRole.UserRole)
            if full_path:
                paths.append(full_path)
        return paths

    def _get_all_paths(self) -> list[str]:
        paths: list[str] = []
        for row in range(self.table.rowCount()):
            item_name = self.table.item(row, 0)
            if item_name is None:
                continue
            full_path = item_name.data(Qt.ItemDataRole.UserRole)
            if full_path:
                paths.append(full_path)
        return paths

    # ---------------------------
    # Copy / Open actions
    # ---------------------------

    def copy_selected(self):
        paths = self._get_selected_paths()
        if not paths:
            QMessageBox.information(self, "Everything-style Search", "No rows selected.")
            return
        text = "\n".join(paths)
        QApplication.clipboard().setText(text)
        self.status_label.setText(f"Copied {len(paths)} path(s) to clipboard.")

    def copy_all(self):
        paths = self._get_all_paths()
        if not paths:
            QMessageBox.information(self, "Everything-style Search", "No results to copy.")
            return
        text = "\n".join(paths)
        QApplication.clipboard().setText(text)
        self.status_label.setText(f"Copied {len(paths)} path(s) to clipboard.")

    def _open_path(self, path: str):
        if not os.path.exists(path):
            QMessageBox.warning(self, "Everything-style Search", f"Path no longer exists:\n{path}")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            QMessageBox.critical(self, "Everything-style Search", f"Failed to open:\n{path}\n\n{e}")

    def open_selected_file(self):
        paths = self._get_selected_paths()
        if not paths:
            return
        self._open_path(paths[0])

    def open_selected_folder(self):
        paths = self._get_selected_paths()
        if not paths:
            QMessageBox.information(self, "Everything-style Search", "No rows selected.")
            return
        folder = os.path.dirname(paths[0])
        if not folder:
            return
        self._open_path(folder)


# ---------------------------
# Main entry
# ---------------------------

def main():
    app = QApplication(sys.argv)
    win = EsGui()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
